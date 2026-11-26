package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;

@Service
@RequiredArgsConstructor
public class JavaUpgradeJobHandler implements JobHandler {
    private final JobService jobs;
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final ClusterServiceAssignmentRepository assignments;
    private final TaskRepository tasks;
    private final HostParcelRepository parcels;
    private final AddNodeArtifactCatalog catalog;
    private final HostStatusService hostStatus;
    private final ObjectMapper json;
    private final TransactionTemplate transaction;
    private final AgentTaskAwaiter taskAwaiter;
    private final ExternalJavaUpgradeService externalJava;

    private record StepRequest(UUID clusterId, UUID artifactId, String hostId, String version, String kafkaVersion) {}

    @Transactional
    public UUID schedule(UUID clusterId, UUID artifactId, List<String> hostIds, String version) {
        if (artifactId == null || version == null || !version.matches("[0-9]+[.][0-9]+[.][0-9]+")
                || hostIds == null || hostIds.isEmpty() || hostIds.stream().anyMatch(id -> id == null || id.isBlank())) {
            throw new IllegalArgumentException("Select a staged JDK and at least one Kafka host.");
        }
        jobs.assertClusterMutable(clusterId);
        Cluster cluster = cluster(clusterId);
        if (externalJava.external(clusterId)) externalJava.validateTargets(clusterId, hostIds);
        var steps = new ArrayList<JobStep>();
        for (String hostId : new LinkedHashSet<>(hostIds)) {
            var input = new StepRequest(clusterId, artifactId, hostId, version, cluster.getKafkaVersion());
            prepareTask(input);
            JobStep step = new JobStep();
            step.setName("Upgrade Java to " + version + " on " + hostId);
            step.setTargetId(hostId);
            step.setStepOrder(steps.size() + 1);
            step.setPayload(write(input));
            steps.add(step);
        }
        Job job = new Job();
        job.setType(JobType.JAVA_UPGRADE);
        job.setResourceKey("cluster:" + clusterId);
        job.setPayload(write(Map.of("clusterId", clusterId, "version", version, "artifactId", artifactId)));
        return jobs.createJob(job, steps).getId();
    }

    @Override
    public boolean supports(JobType type) {
        return type == JobType.JAVA_UPGRADE;
    }

    @Override
    public void execute(Job job) {
        for (JobStep step : jobs.getSteps(job.getId())) {
            if (step.getStatus() == JobStepStatus.SUCCESS) continue;
            try {
                UUID taskId = transaction.execute(status -> {
                    if (step.getExternalKafkaTaskId() != null) return step.getExternalKafkaTaskId();
                    if (step.getAgentTaskId() != null) return step.getAgentTaskId();
                    Task task = prepareTask(read(step.getPayload(), StepRequest.class));
                    jobs.startStep(step.getId());
                    StepRequest input = read(step.getPayload(), StepRequest.class);
                    UUID id = externalJava.external(input.clusterId())
                            ? externalJava.enqueue(input.clusterId(), input.hostId(), input.artifactId(), input.version())
                            : tasks.saveAndFlush(task).getId();
                    jobs.attachAgentTask(step.getId(), id);
                    return id;
                });
                Task result = taskAwaiter.await(Objects.requireNonNull(taskId));
                jobs.completeStep(step.getId(), result.getLogOutput());
            } catch (RuntimeException ex) {
                jobs.failStep(step.getId(), ex.getMessage());
                throw ex;
            }
        }
    }

    private Task prepareTask(StepRequest input) {
        Cluster cluster = cluster(input.clusterId());
        if (!Objects.equals(input.kafkaVersion(), cluster.getKafkaVersion())) {
            throw new IllegalArgumentException("Kafka version changed. Review Java compatibility and submit a new job.");
        }
        if (externalJava.external(input.clusterId())) {
            var params = externalJava.prepare(input.clusterId(), input.hostId(), input.artifactId(), input.version());
            Task task = new Task(); task.setCommand("JAVA_UPGRADE"); task.setClusterId(input.clusterId());task.setHostId(input.hostId());task.setParameters(write(params));
            return task;
        }
        Host host = hosts.findById(input.hostId()).orElseThrow(() -> new IllegalArgumentException("Host not found."));
        if (!cluster.getId().equals(host.getClusterId()) || !"ONLINE".equalsIgnoreCase(hostStatus.agentConnectivityStatus(host))
                || !Objects.toString(host.getAgentVersion(), "").contains(".java-upgrade.1")) {
            throw new IllegalArgumentException("Host must belong to this cluster and report an online Java Upgrade agent.");
        }
        var matching = assignments.findLiveAssignmentsForHost(input.hostId());
        if (matching.isEmpty() || matching.size() > 2 || matching.stream().anyMatch(a -> a.getCluster() == null || !cluster.getId().equals(a.getCluster().getId()))) throw new IllegalArgumentException("Java upgrade requires Kafka services from this cluster only.");
        boolean separate = matching.size() == 2;
        if (separate && (!matching.stream().map(a -> Objects.toString(a.getRole(), "").toLowerCase(Locale.ROOT)).collect(java.util.stream.Collectors.toSet()).equals(Set.of("broker", "controller")))) throw new IllegalArgumentException("Separate Java upgrade requires one broker and one controller on this host.");
        if (separate && !host.getAgentVersion().contains(".java-separate.1")) throw new IllegalArgumentException("Install the separate-service Java upgrade agent first.");
        var service = matching.getFirst();
        String role = Objects.toString(service.getRole(), "").toLowerCase(Locale.ROOT);
        String defaultUnit = switch (role) {
            case "broker" -> "broker";
            case "controller" -> "controller";
            case "broker_controller" -> "kafka";
            default -> throw new IllegalArgumentException("Unsupported Kafka service role.");
        };
        HostParcel parcel = parcels.findFirstByHostIdAndArtifactIdOrderByCreatedAtDescIdDesc(input.hostId(), input.artifactId())
                .filter(item -> "JAVA".equalsIgnoreCase(item.getServiceType()) && "DISTRIBUTED".equals(item.getStatus())
                        && input.version().equals(item.getVersion()))
                .orElseThrow(() -> new IllegalArgumentException("Distribute the selected JDK to " + input.hostId() + " first."));
        var artifact = catalog.resolveType("JAVA", input.version(), input.artifactId().toString());
        if (!artifact.checksum().equalsIgnoreCase(parcel.getChecksum())) {
            throw new IllegalArgumentException("The staged JDK checksum differs from the approved artifact.");
        }
        JsonNode config = configuration(service.getConfigJson());
        JsonNode clusterConfig = configuration(cluster.getConfigJson());
        String unit = config.path("service_name").asText(config.path("systemd_service").asText(defaultUnit));
        String endpoint = role.equals("controller")
                ? "127.0.0.1:" + config.path("controller_port").asInt(clusterConfig.path("controller_port").asInt(9093))
                : Objects.toString(cluster.getBootstrapServers(), "").split(",", -1)[0].trim();
        String protocol = config.path("security_protocol").asText(clusterConfig.path("security_protocol").asText("PLAINTEXT"));
        if (!"PLAINTEXT".equalsIgnoreCase(protocol) || endpoint.isBlank() || endpoint.contains("://")) {
            throw new IllegalArgumentException("A PLAINTEXT Kafka endpoint is required.");
        }
        if (cluster.getConfigPath() == null || !cluster.getConfigPath().startsWith("/")) {
            throw new IllegalArgumentException("The Kafka installation path is missing.");
        }
        String path = cluster.getConfigPath();
        int configAt = path.lastIndexOf("/config/");
        if (configAt < 1) throw new IllegalArgumentException("The Kafka installation path cannot be resolved.");
        Task task = new Task();
        task.setClusterId(cluster.getId());
        task.setHostId(input.hostId());
        task.setCommand("JAVA_UPGRADE");
        task.setStatus("PENDING");
        task.setChecksum(artifact.checksum());
        var parameters = new LinkedHashMap<String, String>(Map.of(
                "artifact_id", input.artifactId().toString(), "target_version", input.version(),
                "parcel_dir", parcel.getParcelDir(), "service_name", unit, "service_role", role,
                "kafka_home", path.substring(0, configAt), "health_endpoint", endpoint));
        if (separate) {
            var serviceTargets = new ArrayList<Map<String,String>>();
            var units = new HashSet<String>();
            for (var assignment : matching) {
                String targetRole = assignment.getRole().toLowerCase(Locale.ROOT);
                JsonNode targetConfig = configuration(assignment.getConfigJson());
                String targetUnit = targetConfig.path("service_name").asText(targetConfig.path("systemd_service").asText(targetRole));
                if (targetUnit.endsWith(".service")) targetUnit = targetUnit.substring(0, targetUnit.length()-8);
                if (!targetUnit.matches("[A-Za-z0-9_][A-Za-z0-9_.@:-]{0,191}") || !units.add(targetUnit)) throw new IllegalArgumentException("Separate Kafka services must have distinct valid systemd units.");
                if (!"PLAINTEXT".equalsIgnoreCase(targetConfig.path("security_protocol").asText(clusterConfig.path("security_protocol").asText("PLAINTEXT")))) throw new IllegalArgumentException("A PLAINTEXT Kafka endpoint is required for both services.");
                String targetEndpoint = targetRole.equals("controller") ? "127.0.0.1:" + targetConfig.path("controller_port").asInt(clusterConfig.path("controller_port").asInt(9093)) : Objects.toString(cluster.getBootstrapServers(), "").split(",", -1)[0].trim();
                serviceTargets.add(Map.of("name",targetUnit+".service","role",targetRole,"kafka_home",path.substring(0,configAt),"health_endpoint",targetEndpoint));
            }
            parameters.put("service_name", unit.endsWith(".service") ? unit.substring(0,unit.length()-8) : unit);
            parameters.put("separate_services",write(serviceTargets));
        }
        task.setParameters(write(parameters));
        return task;
    }

    private Cluster cluster(UUID id) {
        if (externalJava.external(id)) return externalJava.cluster(id);
        return clusters.findById(id).filter(c -> "INTERNAL".equalsIgnoreCase(c.getOriginType())
                        && "kraft".equalsIgnoreCase(c.getMode()) && "SUCCESS".equalsIgnoreCase(c.getStatus())
                        && c.getServices() != null && !c.getServices().isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Java upgrade requires a ready, managed KRaft cluster."));
    }

    private JsonNode configuration(String value) {
        return value == null || value.isBlank() ? json.createObjectNode() : read(value, JsonNode.class);
    }

    private <T> T read(String value, Class<T> type) {
        try { return json.readValue(value, type); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("Cannot read Java upgrade configuration.", e); }
    }

    private String write(Object value) {
        try { return json.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Cannot save Java upgrade configuration.", e); }
    }
}
