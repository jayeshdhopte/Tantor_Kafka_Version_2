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

    private record StepRequest(UUID clusterId, UUID artifactId, String hostId, String version, String kafkaVersion) {}

    @Transactional
    public UUID schedule(UUID clusterId, UUID artifactId, List<String> hostIds, String version) {
        if (artifactId == null || version == null || !version.matches("[0-9]+[.][0-9]+[.][0-9]+")
                || hostIds == null || hostIds.isEmpty() || hostIds.stream().anyMatch(id -> id == null || id.isBlank())) {
            throw new IllegalArgumentException("Select a staged JDK and at least one Kafka host.");
        }
        jobs.assertClusterMutable(clusterId);
        Cluster cluster = cluster(clusterId);
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
                    if (step.getAgentTaskId() != null) return step.getAgentTaskId();
                    Task task = prepareTask(read(step.getPayload(), StepRequest.class));
                    jobs.startStep(step.getId());
                    UUID id = tasks.saveAndFlush(task).getId();
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
        Host host = hosts.findById(input.hostId()).orElseThrow(() -> new IllegalArgumentException("Host not found."));
        if (!cluster.getId().equals(host.getClusterId()) || !"ONLINE".equalsIgnoreCase(hostStatus.agentConnectivityStatus(host))
                || !Objects.toString(host.getAgentVersion(), "").contains(".java-upgrade.1")) {
            throw new IllegalArgumentException("Host must belong to this cluster and report an online Java Upgrade agent.");
        }
        var matching = assignments.findLiveAssignmentsForHost(input.hostId());
        if (matching.size() != 1 || !cluster.getId().equals(matching.getFirst().getCluster().getId())) throw new IllegalArgumentException("Java upgrade requires one Kafka service per dedicated host.");
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
        task.setParameters(write(Map.of(
                "artifact_id", input.artifactId().toString(), "target_version", input.version(),
                "parcel_dir", parcel.getParcelDir(), "service_name", unit, "service_role", role,
                "kafka_home", path.substring(0, configAt), "health_endpoint", endpoint)));
        return task;
    }

    private Cluster cluster(UUID id) {
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
