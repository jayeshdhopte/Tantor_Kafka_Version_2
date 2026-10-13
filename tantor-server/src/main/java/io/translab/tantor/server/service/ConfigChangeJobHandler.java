package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class ConfigChangeJobHandler implements JobHandler {

    private final JobService jobService;
    private final ConfigVersionService configVersionService;
    private final DeploymentService deploymentService;
    private final AgentTaskAwaiter taskAwaiter;
    private final KafkaAdminService kafkaAdminService;
    private final ClusterRepository clusterRepository;
    private final ExternalClusterService externalClusterService;
    private final ObjectMapper objectMapper;
    private final RollingRestartHealthService health;

    @Override
    public boolean supports(JobType type) {
        return type == JobType.CONFIG_CHANGE;
    }

    @Override
    public void execute(Job job) {
        Map<String, Object> jobPayload = readMap(job.getPayload());
        UUID clusterId = UUID.fromString(String.valueOf(jobPayload.get("clusterId")));
        Object versionValue = jobPayload.get("configVersionId");
        UUID versionId = versionValue == null ? null : UUID.fromString(String.valueOf(versionValue));
        for (JobStep step : jobService.getSteps(job.getId())) {
            if (step.getStatus() == JobStepStatus.SUCCESS) continue;
            Map<String, Object> payload = readMap(step.getPayload());
            try {
                if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                    Task completed = taskAwaiter.await(step.getAgentTaskId());
                    if ("patch_service".equals(payload.get("operation"))) {
                        waitForHealth(clusterId, required(payload, "hostId"));
                        assertRoleSafety(clusterId, required(payload, "role").toLowerCase(),
                                Integer.parseInt(required(payload, "nodeId")));
                    }
                    jobService.completeStep(step.getId(), completed.getLogOutput());
                    continue;
                }
                jobService.startStep(step.getId());
                executeStep(clusterId, step, payload, false);
                jobService.completeStep(step.getId(), "Configuration change applied successfully.");
            } catch (Exception e) {
                if (versionId != null) configVersionService.markFailed(versionId);
                jobService.failStep(step.getId(), e.getMessage());
                throw e;
            }
        }
        if (versionId != null) {
            configVersionService.markApplied(versionId,
                    String.valueOf(jobPayload.getOrDefault("activeServiceConfigJson", "{}")));
        }
        clusterRepository.findById(clusterId).ifPresent(cluster -> {
            cluster.setStatus("SUCCESS");
            clusterRepository.save(cluster);
        });
    }

    @Override
    public void rollback(Job job) {
        Map<String, Object> jobPayload = readMap(job.getPayload());
        UUID clusterId = UUID.fromString(String.valueOf(jobPayload.get("clusterId")));
        Object versionValue = jobPayload.get("configVersionId");
        UUID versionId = versionValue == null ? null : UUID.fromString(String.valueOf(versionValue));
        List<JobStep> steps = new java.util.ArrayList<>(jobService.getSteps(job.getId()));
        java.util.Collections.reverse(steps);
        for (JobStep step : steps) {
            if (step.getStatus() != JobStepStatus.SUCCESS
                    && step.getStatus() != JobStepStatus.ROLLBACK_FAILED
                    && step.getStatus() != JobStepStatus.IN_PROGRESS) continue;
            try {
                if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                    Task completed = taskAwaiter.await(step.getAgentTaskId());
                    jobService.rolledBackStep(step.getId(), completed.getLogOutput());
                    continue;
                }
                jobService.startStep(step.getId());
                executeStep(clusterId, step, readMap(step.getPayload()), true);
                jobService.rolledBackStep(step.getId(), "Previous configuration restored.");
            } catch (Exception e) {
                jobService.rollbackFailedStep(step.getId(), e.getMessage());
                clusterRepository.findById(clusterId).ifPresent(cluster -> {
                    cluster.setStatus("FAILED");
                    clusterRepository.save(cluster);
                });
                throw e;
            }
        }
        if (versionId != null) configVersionService.markJobRolledBack(versionId);
        clusterRepository.findById(clusterId).ifPresent(cluster -> {
            cluster.setStatus("SUCCESS");
            clusterRepository.save(cluster);
        });
    }

    @SuppressWarnings("unchecked")
    private void executeStep(UUID clusterId, JobStep step, Map<String, Object> payload, boolean rollback) {
        String operation = String.valueOf(payload.getOrDefault("operation", "service"));
        if ("patch_service".equals(operation)) {
            if (!Boolean.parseBoolean(String.valueOf(payload.getOrDefault("restart", true)))) {
                throw new IllegalStateException("A versioned file change must restart its Kafka service.");
            }
            String role = required(payload, "role").toLowerCase();
            if (role.contains("broker_controller") || role.contains("broker+controller")) {
                throw new IllegalStateException("Combined broker/controller processes are not eligible for role-only rolling restart.");
            }
            assertRoleSafety(clusterId, role, Integer.parseInt(required(payload, "nodeId")));
            waitForHealth(clusterId, required(payload, "hostId"));
            @SuppressWarnings("unchecked")
            Map<String, Object> changes = (Map<String, Object>) payload.get("changes");
            @SuppressWarnings("unchecked")
            Map<String, Object> expected = (Map<String, Object>) payload.get("expected");
            if (changes == null || changes.isEmpty() || expected == null) {
                throw new IllegalStateException("Version has no verified property diff.");
            }
            UUID taskId = deploymentService.patchKafkaConfig(
                    clusterId, required(payload, "hostId"), role, required(payload, "nodeId"),
                    String.valueOf(payload.getOrDefault("configJson", "{}")), required(payload, "configPath"),
                    rollback ? expected : changes, rollback ? changes : expected,
                    true, String.valueOf(payload.getOrDefault("configVersion", "unversioned")));
            jobService.attachAgentTask(step.getId(), taskId);
            Task task = taskAwaiter.await(taskId);
            jobService.appendStepLog(step.getId(), task.getLogOutput());
            waitForHealth(clusterId, required(payload, "hostId"));
            assertRoleSafety(clusterId, role, Integer.parseInt(required(payload, "nodeId")));
            return;
        }
        if ("dynamic".equals(operation)) {
            String key = required(payload, "configKey");
            if (rollback) {
                Map<String, Object> previous = (Map<String, Object>) payload.getOrDefault("previousByBroker", Map.of());
                for (Map.Entry<String, Object> entry : previous.entrySet()) {
                    alterDynamicWithBackup(clusterId, step, Integer.parseInt(entry.getKey()), key, String.valueOf(entry.getValue()));
                }
            } else {
                String value = required(payload, "configValue");
                for (Integer brokerId : kafkaAdminService.getBrokerConfigs(clusterId).keySet()) {
                    alterDynamicWithBackup(clusterId, step, brokerId, key, value);
                }
            }
            return;
        }
        if ("external".equals(operation)) {
            String configValue = rollback
                    ? String.valueOf(payload.getOrDefault("previousValue", ""))
                    : required(payload, "configValue");
            Map<String, Object> queued = externalClusterService.queueConfigUpdate(
                    clusterId,
                    required(payload, "configKey"),
                    configValue,
                    Boolean.parseBoolean(String.valueOf(payload.getOrDefault("restart", false)))
            );
            String taskId = String.valueOf(queued.get("taskId"));
            for (int attempt = 0; attempt < 120; attempt++) {
                Map<String, Object> statusMap = externalClusterService.getExternalTaskStatus(taskId);
                String status = String.valueOf(statusMap.get("status"));
                if ("SUCCESS".equals(status)) return;
                if ("FAILED".equals(status)) throw new RuntimeException(String.valueOf(statusMap.get("message")));
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for external configuration task", e);
                }
            }
            throw new RuntimeException("Timed out waiting for external configuration task.");
        }

        String configJson = rollback
                ? String.valueOf(payload.getOrDefault("previousConfigJson", "{}"))
                : String.valueOf(payload.getOrDefault("configJson", "{}"));
        String properties = rollback
                ? String.valueOf(payload.getOrDefault("previousPropertiesTemplate", ""))
                : String.valueOf(payload.getOrDefault("propertiesTemplate", ""));
        UUID taskId = deploymentService.updateKafkaConfig(
                clusterId,
                required(payload, "hostId"),
                required(payload, "role"),
                required(payload, "nodeId"),
                configJson,
                properties,
                Boolean.parseBoolean(String.valueOf(payload.getOrDefault("restart", true))),
                rollback ? "rollback" : String.valueOf(payload.getOrDefault("configVersion", "unversioned")),
                String.valueOf(payload.getOrDefault("configPath", ""))
        );
        jobService.attachAgentTask(step.getId(), taskId);
        Task task = taskAwaiter.await(taskId);
        jobService.appendStepLog(step.getId(), task.getLogOutput());

        if (!rollback && payload.containsKey("clusterConfigJson")) {
            clusterRepository.findById(clusterId).ifPresent(cluster -> {
                cluster.setConfigJson(String.valueOf(payload.get("clusterConfigJson")));
                clusterRepository.save(cluster);
            });
        }
    }

    private Map<String, Object> readBackupConfig(String json) {
        return json == null || json.isBlank() ? Map.of() : readMap(json);
    }

    private String backupConfigJson(Map<String, Object> config) {
        try { return objectMapper.writeValueAsString(config); }
        catch (Exception e) { throw new IllegalStateException("Cannot serialize backup configuration", e); }
    }

    private void alterDynamicWithBackup(UUID clusterId, JobStep step, int brokerId, String key, String value) {
        Cluster cluster = clusterRepository.findWithServicesById(clusterId).orElseThrow(() -> new IllegalStateException("Managed node agent is required for mandatory config backup."));
        ClusterServiceAssignment node = cluster.getServices().stream()
                .filter(service -> service.getNodeId() != null && service.getNodeId() == brokerId
                        && Set.of("broker", "broker_controller").contains(service.getRole().toLowerCase(java.util.Locale.ROOT)))
                .findFirst().orElseThrow(() -> new IllegalStateException("No managed broker agent is available for config backup."));
        Map<String, Object> config = new LinkedHashMap<>(readBackupConfig(cluster.getConfigJson()));
        config.putAll(readBackupConfig(node.getConfigJson())); config.put("mode", cluster.getMode()); config.put("version", cluster.getKafkaVersion());
        String base = String.valueOf(config.getOrDefault("kafka_install_base_dir", config.getOrDefault("kafka_install_dir", "/opt"))).replaceAll("/+$", "");
        if (base.isBlank()) base = "/opt";
        if (!base.endsWith("/kafka")) {
            if (base.substring(base.lastIndexOf('/') + 1).startsWith("kafka_")) base = base.substring(0, base.lastIndexOf('/'));
            base += "/kafka";
        }
        boolean flat = cluster.getKafkaVersion() != null && cluster.getKafkaVersion().matches("^[vV]?[4-9].*");
        String path = base + "/config" + (flat ? "" : "/kraft") + ("broker_controller".equalsIgnoreCase(node.getRole()) ? "/server.properties" : "/broker.properties");
        Map<String, Object> before = kafkaAdminService.getBrokerConfigs(clusterId).get(brokerId);
        if (before == null || !(before.get(key) instanceof Map<?, ?> previous)) throw new IllegalStateException("Live property is unavailable for backup.");
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("mode", "DYNAMIC"); snapshot.put("key", key); snapshot.put("previous", previous);
        snapshot.put("requestedValue", value); snapshot.put("brokerId", brokerId); snapshot.put("clusterId", clusterId.toString());
        snapshot.put("capturedAt", java.time.Instant.now().toString());
        UUID taskId = deploymentService.backupKafkaConfig(clusterId, node.getHostId(), node.getRole(), String.valueOf(brokerId),
                backupConfigJson(config), path, key, snapshot, "dynamic-" + step.getId());
        Task backup = taskAwaiter.await(taskId);
        jobService.appendStepLog(step.getId(), backup.getLogOutput());
        if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Interrupted before configuration change");
        Map<String, Object> latest = kafkaAdminService.getBrokerConfigs(clusterId).get(brokerId);
        if (latest == null || !java.util.Objects.equals(previous, latest.get(key))) throw new IllegalStateException("Broker value changed while backing up; refresh the plan.");
        kafkaAdminService.alterBrokerConfig(clusterId, brokerId, key, value);
    }

    private Map<String, Object> readMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid configuration job payload", e);
        }
    }

    private String required(Map<String, Object> payload, String key) {
        Object value = payload.get(key);
        if (value == null || String.valueOf(value).isBlank()) {
            throw new IllegalArgumentException("Missing configuration payload field: " + key);
        }
        return String.valueOf(value);
    }

    private void waitForHealth(UUID clusterId, String hostId) {
        int expectedBrokers = clusterRepository.findWithServicesById(clusterId)
                .map(cluster -> (int) cluster.getServices().stream()
                        .filter(service -> service.getRole() != null && service.getRole().contains("broker"))
                        .count()).orElse(1);
        String lastReason = "Kafka health did not recover";
        for (int attempt = 0; attempt < 45; attempt++) {
            var snapshot = health.inspect(clusterId, Math.max(1, expectedBrokers), Set.of(hostId));
            if (snapshot.healthy()) return;
            lastReason = snapshot.failureReason();
            try { Thread.sleep(2000); }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while verifying Kafka health", e);
            }
        }
        throw new IllegalStateException("Kafka health verification failed: " + lastReason);
    }

    private void assertRoleSafety(UUID clusterId, String role, int nodeId) {
        var cluster = clusterRepository.findWithServicesById(clusterId)
                .orElseThrow(() -> new IllegalStateException("Managed cluster was not found"));
        long dedicatedCount = cluster.getServices().stream()
                .filter(service -> role.equalsIgnoreCase(service.getRole())).count();
        if ("broker".equals(role) && dedicatedCount < 2) {
            throw new IllegalStateException("Broker rolling change requires at least two dedicated brokers.");
        }
        if ("controller".equals(role)) {
            if (dedicatedCount < 3) throw new IllegalStateException("Controller rolling change requires at least three dedicated controllers.");
            try {
                var quorum = kafkaAdminService.getAdminClient(clusterId).describeMetadataQuorum().quorumInfo().get();
                if (quorum.leaderId() < 0 || quorum.voters().size() < 3
                        || quorum.voters().stream().noneMatch(voter -> voter.replicaId() == nodeId)) {
                    throw new IllegalStateException("KRaft quorum or selected controller voter is unavailable.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted during KRaft quorum check", e);
            } catch (java.util.concurrent.ExecutionException e) {
                throw new IllegalStateException("KRaft quorum check failed", e);
            }
        }
    }
}
