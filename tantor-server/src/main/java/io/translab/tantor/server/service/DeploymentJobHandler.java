package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.dto.SaveConnectionRequest;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

@Component
@RequiredArgsConstructor
public class DeploymentJobHandler implements JobHandler {

    private final JobService jobService;
    private final DeploymentService deploymentService;
    private final AgentTaskAwaiter taskAwaiter;
    private final ClusterRepository clusterRepository;
    private final HostRepository hostRepository;
    private final ObjectMapper objectMapper;
    private final PrometheusMonitoringService prometheusMonitoringService;
    private final KafkaAdminService kafkaAdminService;
    private final DataServiceConnectionService dataServiceConnectionService;
    private final ClusterServiceAssignmentRepository clusterServiceAssignmentRepository;

    @Override
    public boolean supports(JobType type) {
        return type == JobType.DEPLOYMENT || type == JobType.ADD_HOST;
    }

    @Override
    public void execute(Job job) {
        Map<String, Object> jobPayload = readMap(job.getPayload());
        UUID clusterId = UUID.fromString(String.valueOf(jobPayload.get("clusterId")));
        Cluster cluster = clusterRepository.findById(clusterId)
                .orElseThrow(() -> new RuntimeException("Cluster not found: " + clusterId));
        saveClusterStatus(cluster, "RUNNING", job);

        List<JobStep> steps = jobService.getSteps(job.getId());
        if (steps.isEmpty()) throw new RuntimeException("Deployment job has no persisted steps.");

        for (int i = 0; i < steps.size();) {
            JobStep step = steps.get(i);
            if (step.getStatus() == JobStepStatus.SUCCESS) {
                i++;
                continue;
            }
            Map<String, Object> payload = readMap(step.getPayload());
            if (isDeployOperation(payload)) {
                List<JobStep> batchSteps = new ArrayList<>();
                List<Map<String, Object>> batchPayloads = new ArrayList<>();
                while (i < steps.size()) {
                    JobStep candidate = steps.get(i);
                    if (candidate.getStatus() == JobStepStatus.SUCCESS) {
                        i++;
                        continue;
                    }
                    Map<String, Object> candidatePayload = readMap(candidate.getPayload());
                    if (!isDeployOperation(candidatePayload)) break;
                    batchSteps.add(candidate);
                    batchPayloads.add(candidatePayload);
                    i++;
                }
                executeDeployBatch(job, cluster, clusterId, jobPayload, batchSteps, batchPayloads);
                continue;
            }
            try {
                if (isSchemaOperation(payload)) markSchemaService(clusterId, "DEPLOYING", null);
                if (isConnectOperation(payload)) markConnectService(clusterId, "DEPLOYING", null);
                if (isBackendSchemaOperation(payload)) {
                    jobService.startStep(step.getId());
                    String output = executeBackendSchemaOperation(clusterId, jobPayload, payload);
                    jobService.completeStep(step.getId(), output);
                    if ("save_schema_connection".equals(operation(payload))) {
                        markSchemaService(clusterId, "ONLINE", null);
                    }
                    jobService.appendLog(job.getId(), step.getName() + " completed.");
                    i++;
                    continue;
                }
                if (isBackendConnectOperation(payload)) {
                    jobService.startStep(step.getId());
                    String output = executeBackendConnectOperation(clusterId, jobPayload, payload);
                    jobService.completeStep(step.getId(), output);
                    if ("save_connect_connection".equals(operation(payload))) {
                        markConnectService(clusterId, "ONLINE", null);
                    }
                    jobService.appendLog(job.getId(), step.getName() + " completed.");
                    i++;
                    continue;
                }
                if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                    Task completed = taskAwaiter.await(step.getAgentTaskId());
                    jobService.completeStep(step.getId(), taskOutput(completed));
                    i++;
                    continue;
                }
                jobService.startStep(step.getId());
                UUID taskId = startOperation(clusterId, jobPayload, payload);
                jobService.attachAgentTask(step.getId(), taskId);
                Task completed = taskAwaiter.await(taskId);
                jobService.completeStep(step.getId(), taskOutput(completed));
                jobService.appendLog(job.getId(), step.getName() + " completed on " + step.getTargetId() + ".");
            } catch (Exception e) {
                jobService.failStep(step.getId(), e.getMessage());
                if (isSchemaOperation(payload)) {
                    markSchemaService(clusterId, "FAILED", e.getMessage());
                    saveClusterStatus(cluster, "DEGRADED", job);
                } else if (isConnectOperation(payload)) {
                    markConnectService(clusterId, "FAILED", e.getMessage());
                    saveClusterStatus(cluster, "DEGRADED", job);
                } else {
                    saveClusterStatus(cluster, "FAILED", job);
                }
                throw e;
            }
            i++;
        }

        if (!"DEGRADED".equalsIgnoreCase(cluster.getStatus())) {
            saveClusterStatus(cluster, "SUCCESS", job);
        }
    }

    private void executeDeployBatch(
            Job job,
            Cluster cluster,
            UUID clusterId,
            Map<String, Object> jobPayload,
            List<JobStep> steps,
            List<Map<String, Object>> payloads
    ) {
        List<RunningStep> running = new ArrayList<>();
        JobStep activeStep = null;
        try {
            for (int i = 0; i < steps.size(); i++) {
                JobStep step = steps.get(i);
                activeStep = step;
                UUID taskId = step.getAgentTaskId();
                if (step.getStatus() != JobStepStatus.IN_PROGRESS || taskId == null) {
                    jobService.startStep(step.getId());
                    taskId = startOperation(clusterId, jobPayload, payloads.get(i));
                    jobService.attachAgentTask(step.getId(), taskId);
                    jobService.appendLog(job.getId(), step.getName() + " dispatched to " + step.getTargetId() + ".");
                }
                running.add(new RunningStep(step, taskId));
            }

            for (RunningStep runningStep : running) {
                activeStep = runningStep.step();
                Task completed = taskAwaiter.await(runningStep.taskId());
                jobService.completeStep(activeStep.getId(), taskOutput(completed));
                jobService.appendLog(job.getId(), activeStep.getName() + " completed on " + activeStep.getTargetId() + ".");
            }
        } catch (Exception e) {
            if (activeStep != null) {
                jobService.failStep(activeStep.getId(), e.getMessage());
            }
            saveClusterStatus(cluster, "FAILED", job);
            throw e;
        }
    }

    @Override
    public void rollback(Job job) {
        Map<String, Object> jobPayload = readMap(job.getPayload());
        UUID clusterId = UUID.fromString(String.valueOf(jobPayload.get("clusterId")));
        List<JobStep> steps = new ArrayList<>(jobService.getSteps(job.getId()));
        Collections.reverse(steps);

        for (JobStep step : steps) {
            if (step.getStatus() != JobStepStatus.SUCCESS
                    && step.getStatus() != JobStepStatus.ROLLBACK_FAILED
                    && step.getStatus() != JobStepStatus.IN_PROGRESS) continue;
            Map<String, Object> payload = readMap(step.getPayload());
            try {
				String operation = operation(payload);
				if (!"deploy".equals(operation)) {
					if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
						taskAwaiter.await(step.getAgentTaskId());
					}
					jobService.rolledBackStep(step.getId(), "Validation step has no deployed resource to remove.");
					continue;
				}
                if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                    Task completed = taskAwaiter.await(step.getAgentTaskId());
                    jobService.rolledBackStep(step.getId(), taskOutput(completed));
                    continue;
                }
                jobService.startStep(step.getId());
                UUID taskId = deploymentService.deleteClusterFromHost(
                        clusterId,
                        required(payload, "host_id"),
                        required(jobPayload, "kafkaVersion"),
                        stringValue(payload, "serviceConfigJson")
                );
                jobService.attachAgentTask(step.getId(), taskId);
                Task completed = taskAwaiter.await(taskId);
                jobService.rolledBackStep(step.getId(), taskOutput(completed));
            } catch (Exception e) {
                jobService.rollbackFailedStep(step.getId(), e.getMessage());
                throw e;
            }
        }
        finalizeRollback(job, clusterId);
    }

    protected void finalizeRollback(Job job, UUID clusterId) {
        Cluster cluster = clusterRepository.findWithServicesById(clusterId).orElse(null);
        if (cluster == null) return;

        if (job.getType() == JobType.ADD_HOST) {
            List<Map<String, Object>> rolledBackTargets = jobService.getSteps(job.getId()).stream()
                    .filter(step -> step.getStatus() == JobStepStatus.ROLLED_BACK)
                    .map(step -> readMap(step.getPayload()))
                    .toList();
            if (cluster.getServices() != null) {
                cluster.getServices().removeIf(service -> rolledBackTargets.stream().anyMatch(target ->
                        required(target, "host_id").equals(service.getHostId())
                                && required(target, "node_id").equals(String.valueOf(service.getNodeId()))));
            }
            for (Map<String, Object> target : rolledBackTargets) {
                hostRepository.findById(required(target, "host_id")).ifPresent(host -> {
                    if (clusterId.equals(host.getClusterId())) {
                        host.setClusterId(null);
                        hostRepository.save(host);
                    }
                });
            }
            cluster.setStatus("SUCCESS");
        } else {
            if (cluster.getServices() != null) {
                cluster.getServices().forEach(service -> hostRepository.findById(service.getHostId()).ifPresent(host -> {
                    if (clusterId.equals(host.getClusterId())) {
                        host.setClusterId(null);
                        hostRepository.save(host);
                    }
                }));
            }
            cluster.setStatus("ROLLED_BACK");
        }
        applyJobActor(cluster, job);
        clusterRepository.save(cluster);
    }

    private void saveClusterStatus(Cluster cluster, String status, Job job) {
        // Adding a data service does not change the existing Kafka deployment's status.
        // Service status and errors are tracked on its assignment and deployment job.
        if (Boolean.TRUE.equals(readMap(job.getPayload()).get("servicesOnly"))) return;
        applyJobActor(cluster, job);
        cluster.setStatus(status);
        clusterRepository.save(cluster);
    }

    private void applyJobActor(Cluster cluster, Job job) {
        if (job.getRequestedBy() != null && !job.getRequestedBy().isBlank()) {
            cluster.setUpdatedBy(job.getRequestedBy());
        }
    }

    private UUID startOperation(UUID clusterId, Map<String, Object> jobPayload, Map<String, Object> payload) {
        return switch (operation(payload)) {
            case "connectivity" -> deploymentService.checkKRaftConnectivity(
                    clusterId,
                    required(payload, "host_id"),
                    required(payload, "controller_endpoints")
            );
            case "verify_quorum" -> deploymentService.verifyKRaftQuorum(
                    clusterId,
                    required(payload, "host_id"),
                    required(payload, "controller_endpoints"),
                    required(payload, "cluster_uuid"),
                    required(payload, "expected_controller_count"),
                    stringValue(payload, "serviceConfigJson")
            );
            case "verify_zk_quorum" -> deploymentService.verifyZooKeeperQuorum(
                    clusterId,
                    required(payload, "host_id"),
                    required(payload, "zookeeper_connect"),
                    stringValue(payload, "serviceConfigJson")
            );
            case "precheck_schema" -> dispatchSchemaAgentTask(clusterId, jobPayload, "PRECHECK_SCHEMA");
            case "install_schema" -> dispatchSchemaAgentTask(clusterId, jobPayload, "INSTALL_SCHEMA");
            case "verify_schema" -> dispatchSchemaAgentTask(clusterId, jobPayload, "VERIFY_SCHEMA_REGISTRY");
            case "precheck_connect" -> dispatchConnectAgentTask(clusterId, jobPayload, "PRECHECK_CONNECT");
            case "install_connect" -> dispatchConnectAgentTask(clusterId, jobPayload, "INSTALL_CONNECT");
            case "verify_connect" -> dispatchConnectAgentTask(clusterId, jobPayload, "VERIFY_CONNECT");
            case "deploy" -> deploymentService.deployKafkaToHost(
                    clusterId,
                    required(payload, "host_id"),
                    required(jobPayload, "kafkaVersion"),
                    stringValue(jobPayload, "finalArtifactUrl"),
                    "",
                    required(payload, "node_id"),
                    stringValue(jobPayload, "quorumVoters"),
                    required(payload, "role"),
                    stringValue(payload, "serviceConfigJson")
            );
            default -> throw new IllegalArgumentException("Unknown deployment job operation: " + operation(payload));
        };
    }

    @SuppressWarnings("unchecked")
    private UUID dispatchSchemaAgentTask(UUID clusterId, Map<String, Object> jobPayload, String command) {
        Object configuration = jobPayload.get("schemaRegistry");
        if (!(configuration instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Schema Registry job configuration is missing.");
        }
        Map<String, Object> schema = new java.util.LinkedHashMap<>((Map<String, Object>) configuration);
        schema.put("kafka_version", stringValue(jobPayload, "kafkaVersion"));
        schema.put("schema_registry_url", "http://" + schema.get("host_name") + ":" + schema.get("rest_port"));
        if ("PRECHECK_SCHEMA".equals(command)) {
            try {
                kafkaAdminService.getAdminClient(clusterId).describeCluster().nodes().get();
                schema.put("admin_api_verified", "true");
            } catch (Exception e) {
                throw new RuntimeException("Kafka Admin API is not responding: " + e.getMessage(), e);
            }
        }
        return deploymentService.dispatchSchemaTask(
                clusterId,
                String.valueOf(schema.get("host_id")),
                command,
                String.valueOf(schema.getOrDefault("artifact_url", "")),
                String.valueOf(schema.getOrDefault("checksum", "")),
                schema
        );
    }

    @SuppressWarnings("unchecked")
    private UUID dispatchConnectAgentTask(UUID clusterId, Map<String, Object> jobPayload, String command) {
        Object configuration = jobPayload.get("kafkaConnect");
        if (!(configuration instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Kafka Connect job configuration is missing.");
        }
        Map<String, Object> connect = new java.util.LinkedHashMap<>((Map<String, Object>) configuration);
        connect.put("kafka_version", stringValue(jobPayload, "kafkaVersion"));
        connect.put("connect_url", "http://" + connect.get("host_name") + ":" + connect.get("rest_port"));
        if ("PRECHECK_CONNECT".equals(command)) {
            try {
                kafkaAdminService.getAdminClient(clusterId).describeCluster().nodes().get();
                connect.put("admin_api_verified", "true");
            } catch (Exception e) {
                throw new RuntimeException("Kafka Admin API is not responding: " + e.getMessage(), e);
            }
        }
        return deploymentService.dispatchSchemaTask(clusterId, String.valueOf(connect.get("host_id")), command,
                String.valueOf(connect.getOrDefault("artifact_url", "")),
                String.valueOf(connect.getOrDefault("checksum", "")), connect);
    }

    private boolean isBackendSchemaOperation(Map<String, Object> payload) {
        return "create_schema_topic".equals(operation(payload))
                || "save_schema_connection".equals(operation(payload));
    }

    private boolean isBackendConnectOperation(Map<String, Object> payload) {
        return "create_connect_topics".equals(operation(payload))
                || "save_connect_connection".equals(operation(payload));
    }

    private void markSchemaService(UUID clusterId, String status, String error) {
        clusterServiceAssignmentRepository.findByClusterIdAndRole(clusterId, "schema_registry").ifPresent(service -> {
            service.setStatus(status);
            service.setLastError(error);
            clusterServiceAssignmentRepository.save(service);
        });
    }

    private void markConnectService(UUID clusterId, String status, String error) {
        clusterServiceAssignmentRepository.findByClusterIdAndRole(clusterId, "kafka_connect").ifPresent(service -> {
            service.setStatus(status);
            service.setLastError(error);
            clusterServiceAssignmentRepository.save(service);
        });
    }

    private boolean isSchemaOperation(Map<String, Object> payload) {
        return switch (operation(payload)) {
            case "precheck_schema", "create_schema_topic", "install_schema", "verify_schema", "save_schema_connection" -> true;
            default -> false;
        };
    }

    private boolean isConnectOperation(Map<String, Object> payload) {
        return switch (operation(payload)) {
            case "precheck_connect", "create_connect_topics", "install_connect", "verify_connect", "save_connect_connection" -> true;
            default -> false;
        };
    }

    @SuppressWarnings("unchecked")
    private String executeBackendSchemaOperation(UUID clusterId, Map<String, Object> jobPayload,
                                                 Map<String, Object> payload) {
        Object configuration = jobPayload.get("schemaRegistry");
        if (!(configuration instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Schema Registry job configuration is missing.");
        }
        Map<String, Object> schema = (Map<String, Object>) configuration;
        if ("create_schema_topic".equals(operation(payload))) {
            int replicationFactor = Integer.parseInt(String.valueOf(schema.getOrDefault("replication_factor", 1)));
            AdminClient admin = kafkaAdminService.getAdminClient(clusterId);
            try {
                boolean exists = admin.listTopics(new ListTopicsOptions().listInternal(true))
                        .names().get().contains("_schemas");
                if (!exists) {
                    admin.createTopics(List.of(new NewTopic("_schemas", 1, (short) replicationFactor)
                            .configs(Map.of("cleanup.policy", "compact")))).all().get();
                    return "Created compacted _schemas topic with replication factor " + replicationFactor + ".";
                }
                var description = admin.describeTopics(List.of("_schemas"))
                        .allTopicNames().get().get("_schemas");
                if (description.partitions().size() != 1) {
                    throw new IllegalStateException("Existing _schemas topic must have exactly one partition.");
                }
                int actualReplication = description.partitions().get(0).replicas().size();
                if (actualReplication != replicationFactor) {
                    throw new IllegalStateException("Existing _schemas replication factor is "
                            + actualReplication + ", expected " + replicationFactor + ".");
                }
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "_schemas");
                ConfigEntry cleanup = admin.describeConfigs(List.of(resource)).all().get()
                        .get(resource).get("cleanup.policy");
                if (cleanup == null || !java.util.Arrays.asList(cleanup.value().split(",")).contains("compact")) {
                    throw new IllegalStateException("Existing _schemas topic must use cleanup.policy=compact.");
                }
                return "Verified existing _schemas topic.";
            } catch (Exception e) {
                throw new RuntimeException("Unable to create or verify _schemas topic: " + e.getMessage(), e);
            }
        }

        SaveConnectionRequest request = new SaveConnectionRequest();
        request.setConnectionName("Deployed Schema Registry");
        request.setProtocol("http");
        request.setHost(String.valueOf(schema.get("host_name")));
        request.setPort(Integer.parseInt(String.valueOf(schema.get("rest_port"))));
        request.setIsDefault(true);
        dataServiceConnectionService.saveConnection(clusterId, "SCHEMA_REGISTRY", request,
                stringValue(jobPayload, "requestedBy"));
        return "Saved the default Schema Registry connection.";
    }

    @SuppressWarnings("unchecked")
    private String executeBackendConnectOperation(UUID clusterId, Map<String, Object> jobPayload,
                                                  Map<String, Object> payload) {
        Object configuration = jobPayload.get("kafkaConnect");
        if (!(configuration instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Kafka Connect job configuration is missing.");
        }
        Map<String, Object> connect = (Map<String, Object>) configuration;
        if ("create_connect_topics".equals(operation(payload))) {
            int replicationFactor = Integer.parseInt(String.valueOf(connect.getOrDefault("replication_factor", 1)));
            AdminClient admin = kafkaAdminService.getAdminClient(clusterId);
            try {
                List<String> existing = admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get().stream().toList();
                ensureCompactedTopic(admin, existing, String.valueOf(connect.get("offset_topic")), 25, replicationFactor);
                ensureCompactedTopic(admin, existing, String.valueOf(connect.get("config_topic")), 1, replicationFactor);
                ensureCompactedTopic(admin, existing, String.valueOf(connect.get("status_topic")), 5, replicationFactor);
                return "Created or verified Kafka Connect internal topics with replication factor " + replicationFactor + ".";
            } catch (Exception e) {
                throw new RuntimeException("Unable to create or verify Kafka Connect internal topics: " + e.getMessage(), e);
            }
        }

        SaveConnectionRequest request = new SaveConnectionRequest();
        request.setConnectionName("Deployed Kafka Connect");
        request.setProtocol("http");
        request.setHost(String.valueOf(connect.get("host_name")));
        request.setPort(Integer.parseInt(String.valueOf(connect.get("rest_port"))));
        request.setIsDefault(true);
        dataServiceConnectionService.saveConnection(clusterId, "KAFKA_CONNECT", request,
                stringValue(jobPayload, "requestedBy"));
        return "Saved the default Kafka Connect connection.";
    }

    private void ensureCompactedTopic(AdminClient admin, List<String> existing, String topic,
                                      int partitions, int replicationFactor) throws Exception {
        if (topic == null || topic.isBlank() || "null".equals(topic)) {
            throw new IllegalArgumentException("Kafka Connect internal topic name is missing.");
        }
        if (!existing.contains(topic)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) replicationFactor)
                    .configs(Map.of("cleanup.policy", "compact")))).all().get();
            return;
        }
        var description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
        if (description.partitions().size() != partitions) {
            throw new IllegalStateException("Existing topic " + topic + " has " + description.partitions().size()
                    + " partitions; expected " + partitions + ".");
        }
        if (description.partitions().stream().anyMatch(partition -> partition.replicas().size() != replicationFactor)) {
            throw new IllegalStateException("Existing topic " + topic + " does not use replication factor " + replicationFactor + ".");
        }
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry cleanup = admin.describeConfigs(List.of(resource)).all().get().get(resource).get("cleanup.policy");
        if (cleanup == null || !java.util.Arrays.asList(cleanup.value().split(",")).contains("compact")) {
            throw new IllegalStateException("Existing topic " + topic + " must use cleanup.policy=compact.");
        }
    }

    private String operation(Map<String, Object> payload) {
        String value = stringValue(payload, "operation");
        return value.isBlank() ? "deploy" : value;
    }

    private boolean isDeployOperation(Map<String, Object> payload) {
        return "deploy".equals(operation(payload));
    }

    private record RunningStep(JobStep step, UUID taskId) {}

    private Map<String, Object> readMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid job payload", e);
        }
    }

    private String required(Map<String, Object> payload, String key) {
        Object value = payload.get(key);
        if (value == null || String.valueOf(value).isBlank()) {
            throw new IllegalArgumentException("Missing job payload field: " + key);
        }
        return String.valueOf(value);
    }

    private String stringValue(Map<String, Object> payload, String key) {
        Object value = payload.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private String taskOutput(Task task) {
        String logs = task.getLogOutput() == null ? "" : task.getLogOutput();
        return logs.isBlank() ? "Agent task " + task.getId() + " completed successfully." : logs;
    }
}
