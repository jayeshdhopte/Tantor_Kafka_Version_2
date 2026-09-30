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
                    jobService.completeStep(step.getId(), completed.getLogOutput());
                    continue;
                }
                jobService.startStep(step.getId());
                executeStep(clusterId, step, payload, false);
                jobService.completeStep(step.getId(), "Configuration change applied successfully.");
            } catch (Exception e) {
                if (versionId != null) configVersionService.markFailed(versionId);
                jobService.failStep(step.getId(), e.getMessage());
                clusterRepository.findById(clusterId).ifPresent(cluster -> {
                    cluster.setStatus("FAILED");
                    clusterRepository.save(cluster);
                });
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
        if ("dynamic".equals(operation)) {
            String key = required(payload, "configKey");
            if (rollback) {
                Map<String, Object> previous = (Map<String, Object>) payload.getOrDefault("previousByBroker", Map.of());
                for (Map.Entry<String, Object> entry : previous.entrySet()) {
                    kafkaAdminService.alterBrokerConfig(clusterId, Integer.parseInt(entry.getKey()), key, String.valueOf(entry.getValue()));
                }
            } else {
                String value = required(payload, "configValue");
                for (Integer brokerId : kafkaAdminService.getBrokerConfigs(clusterId).keySet()) {
                    kafkaAdminService.alterBrokerConfig(clusterId, brokerId, key, value);
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
}
