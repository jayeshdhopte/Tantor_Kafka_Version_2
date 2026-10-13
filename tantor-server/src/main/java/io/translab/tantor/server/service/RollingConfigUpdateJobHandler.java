package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.domain.JobStatus;
import io.translab.tantor.server.domain.JobStepStatus;
import io.translab.tantor.server.domain.JobType;
import org.springframework.stereotype.Service;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.UUID;

@Service
public class RollingConfigUpdateJobHandler implements JobHandler {

    private final JobService jobService;
    private final ExternalClusterService externalClusterService;
    private final ClusterRepository clusterRepository;
    private final KafkaAdminService kafkaAdminService;
    private final ExternalClusterRepository externalClusterRepository;
    private final RollingRestartHealthService healthService;
    private final ObjectMapper objectMapper;

    public RollingConfigUpdateJobHandler(
            JobService jobService,
            ExternalClusterService externalClusterService,
            ClusterRepository clusterRepository,
            KafkaAdminService kafkaAdminService,
            ExternalClusterRepository externalClusterRepository,
            RollingRestartHealthService healthService,
            ObjectMapper objectMapper) {
        this.jobService = jobService;
        this.externalClusterService = externalClusterService;
        this.clusterRepository = clusterRepository;
        this.kafkaAdminService = kafkaAdminService;
        this.externalClusterRepository = externalClusterRepository;
        this.healthService = healthService;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean supports(JobType jobType) {
        return jobType.name().equals("ROLLING_CONFIG_UPDATE");
    }

    @Override
    public void execute(Job job) {
        try {
            Map<String, Object> jobPayload = objectMapper.readValue(job.getPayload(), new TypeReference<Map<String, Object>>() {});
            UUID clusterId = UUID.fromString(jobPayload.get("clusterId").toString());
            boolean rollingRestart = (boolean) jobPayload.get("rollingRestart");
            List<Map<String, Object>> changes = (List<Map<String, Object>>) jobPayload.get("changes");
            int expectedBrokerCount = ((Number) jobPayload.getOrDefault("expectedBrokerCount", 1)).intValue();
            boolean controllerChange = "CONTROLLER".equals(jobPayload.get("role"));

            if (!externalClusterRepository.existsById(clusterId)) {
                failJob(job, "Cluster not found");
                return;
            }

            jobService.updateJobStatus(job.getId(), JobStatus.IN_PROGRESS);
            List<JobStep> steps = jobService.getSteps(job.getId());

            // We iterate over the steps that were pre-created by ConfigController

            for (JobStep step : steps) {
                if (step.getStatus() == JobStepStatus.SUCCESS) continue;

                jobService.startStep(step.getId());

                if (step.getName().equals("PREFLIGHT")) {
                    try {
                        var snapshot = healthService.inspect(clusterId, expectedBrokerCount, java.util.Set.of());
                        if (!snapshot.healthy()) throw new IllegalStateException(snapshot.failureReason());
                        if (controllerChange) verifyQuorum(clusterId);
                        jobService.completeStep(step.getId(), "Preflight Validation Passed");
                    } catch (Exception e) {
                        jobService.failStep(step.getId(), "Cluster health check failed during preflight");
                        failJob(job, "Cluster health not OK");
                        return;
                    }
                } else if (step.getName().startsWith("BACKUP_ALL")) {
                    boolean success = true;
                    for (Map<String, Object> change : changes) {
                        String host = (String) change.get("host");
                        String configFilePath = (String) change.get("configFilePath");
                        
                        Map<String, Object> backupResult = executeAgentTask(clusterId, host, "backup_file", Map.of(
                                "configFilePath", configFilePath
                        ), null);
                        if (backupResult == null || !"SUCCESS".equals(backupResult.get("status"))) {
                            jobService.failStep(step.getId(), "Backup failed on " + host);
                            failJob(job, "Backup failed on " + host);
                            success = false;
                            break;
                        }
                        jobService.appendStepLog(step.getId(), "Checkpoint on " + host + ": " + backupResult.get("backupFilePath"));
                    }
                    if (success) jobService.completeStep(step.getId(), "All configurations backed up");
                    else return;
                } else if (step.getName().startsWith("WRITE_CONFIG")) {
                    String host = step.getTargetId();
                    Map<String, Object> change = findChangeForHost(changes, host);
                    if (change != null) {
                        if (!healthService.inspect(clusterId, expectedBrokerCount, java.util.Set.of()).healthy()) {
                            failJob(job, "Cluster health precheck failed before writing " + host);
                        }
                        if (controllerChange) verifyQuorum(clusterId);
                        String configFilePath = (String) change.get("configFilePath");
                        List<Map<String, Object>> properties = (List<Map<String, Object>>) change.get("properties");
                        Map<String, String> configChanges = new HashMap<>();
                        Map<String, String> expectedValues = new HashMap<>();
                        if (properties != null) {
                            for (Map<String, Object> prop : properties) {
                                configChanges.put((String) prop.get("key"), (String) prop.get("newValue"));
                                String oldValue = String.valueOf(prop.getOrDefault("oldValue", ""));
                                expectedValues.put((String) prop.get("key"), oldValue.isEmpty() ? null : oldValue);
                            }
                        }
                        Map<String, Object> writePayload = new HashMap<>();
                        writePayload.put("configFilePath", configFilePath);
                        writePayload.put("configChanges", configChanges);
                        writePayload.put("expectedValues", expectedValues);
                        Map<String, Object> writeResult = executeAgentTask(clusterId, host, "write_config", writePayload, step);
                        if (writeResult == null || !"SUCCESS".equals(writeResult.get("status"))) {
                            failJob(job, "Configuration write failed on " + host + ". Check the saved backup before operator recovery.");
                        }
                    } else {
                        jobService.completeStep(step.getId(), "No changes");
                    }
                } else if (step.getName().startsWith("RESTART_SERVICE")) {
                    String host = step.getTargetId();
                    Map<String, Object> change = findChangeForHost(changes, host);
                    if (change != null) {
                        if (!healthService.inspect(clusterId, expectedBrokerCount, java.util.Set.of()).healthy()) {
                            failJob(job, "Cluster health precheck failed before restarting " + host);
                        }
                        if (controllerChange) verifyQuorum(clusterId);
                        String serviceName = (String) change.get("serviceName");
                        Map<String, Object> restartResult = executeAgentTask(clusterId, host, "restart_service", Map.of(
                                "serviceName", serviceName
                        ), step);
                        if (restartResult == null || !"SUCCESS".equals(restartResult.get("status"))) {
                            failJob(job, "Restart failed on " + host + ". Further nodes were not changed; inspect the saved backup before recovery.");
                        }
                    } else {
                        jobService.completeStep(step.getId(), "No restart");
                    }
                } else if (step.getName().startsWith("HEALTH_CHECK")) {
                    String host = step.getTargetId();
                    Map<String, Object> change = findChangeForHost(changes, host);
                    if (change != null) {
                        boolean healthy = verifyHealthWithTimeout(clusterId, expectedBrokerCount, 90);
                        if (healthy && controllerChange) {
                            try { verifyQuorum(clusterId); }
                            catch (Exception e) { healthy = false; }
                        }
                        if (!healthy) {
                            jobService.failStep(step.getId(), "Health check failed after restart");
                            failJob(job, "Health check failed after restarting " + host + ". Further nodes were not changed.");
                        } else {
                            jobService.completeStep(step.getId(), "Node healthy");
                        }
                    } else {
                        jobService.completeStep(step.getId(), "OK");
                    }
                } else if (step.getName().startsWith("FINAL_VERIFY")) {
                    for (Map<String, Object> change : changes) {
                        String host = (String) change.get("host");
                        Map<String, Object> readResult = executeAgentTask(clusterId, host, "read_config", Map.of(
                                "configFilePath", (String) change.get("configFilePath")), null);
                        if (!"SUCCESS".equals(readResult.get("status"))) {
                            failJob(job, "Final config read failed on " + host);
                        }
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> properties = (List<Map<String, Object>>) change.get("properties");
                        for (Map<String, Object> property : properties) {
                            String desired = String.valueOf(property.get("newValue"));
                            String actual = String.valueOf(readResult.getOrDefault(property.get("key"), ""));
                            if (!actual.equals(desired) || (desired.isEmpty() && readResult.containsKey(property.get("key")))) {
                                failJob(job, "Final value verification failed for " + property.get("key") + " on " + host);
                            }
                        }
                    }
                    if (!verifyHealthWithTimeout(clusterId, expectedBrokerCount, 10)) {
                        failJob(job, "Cluster health did not pass final verification.");
                    }
                    jobService.completeStep(step.getId(), "All node values and cluster health verified");
                }
            }
            
            jobService.updateJobStatus(job.getId(), JobStatus.SUCCESS);
            
        } catch (Exception e) {
            if (jobService.getJob(job.getId()).getStatus() == JobStatus.FAILED) {
                throw e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e);
            }
            failJob(job, "Rolling config update failed: " + e.getMessage());
        }
    }

    private void failJob(Job job, String message) {
        jobService.updateJobStatus(job.getId(), JobStatus.FAILED);
        jobService.appendLog(job.getId(), message);
        throw new IllegalStateException(message);
    }
    
    private Map<String, Object> findChangeForHost(List<Map<String, Object>> changes, String host) {
        for (Map<String, Object> change : changes) {
            if (host.equals(change.get("host"))) return change;
        }
        return null;
    }
    
    private boolean verifyHealthWithTimeout(UUID clusterId, int expectedBrokerCount, int secondsToWait) {
        long end = System.currentTimeMillis() + (secondsToWait * 1000L);
        while (System.currentTimeMillis() < end) {
            try {
                if (healthService.inspect(clusterId, expectedBrokerCount, java.util.Set.of()).healthy()) return true;
            } catch (Exception e) {
                try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            }
        }
        return false;
    }

    private void verifyQuorum(UUID clusterId) {
        try {
            var quorum = kafkaAdminService.getAdminClient(clusterId).describeMetadataQuorum().quorumInfo().get();
            if (quorum.leaderId() < 0 || quorum.voters().size() < 3) {
                throw new IllegalStateException("KRaft controller quorum is unavailable");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during KRaft quorum check", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("KRaft quorum check failed", e);
        }
    }

    private Map<String, Object> executeAgentTask(UUID clusterId, String host, String taskName, Map<String, Object> payload, JobStep step) {
        try {
            Map<String, Object> queued = externalClusterService.queueTask(clusterId, host, taskName, payload);
            String taskId = String.valueOf(queued.get("taskId"));
            if (step != null) jobService.attachAgentTask(step.getId(), UUID.fromString(taskId));
            for (int attempt = 0; attempt < 60; attempt++) {
                Map<String, Object> statusMap = externalClusterService.getExternalTaskStatus(taskId);
                String status = String.valueOf(statusMap.get("status"));
                if ("SUCCESS".equals(status) || "FAILED".equals(status)) {
                    if ("FAILED".equals(status)) {
                        String errMsg = String.valueOf(statusMap.get("message"));
                        if (step != null) jobService.failStep(step.getId(), "Task failed: " + errMsg);
                        return Map.of("status", "FAILED", "error", errMsg);
                    }
                    if (step != null) jobService.completeStep(step.getId(), "Success");
                    
                    Map<String, Object> result = new HashMap<>();
                    result.put("status", "SUCCESS");
                    if (statusMap.get("data") != null) result.putAll((Map<String, Object>) statusMap.get("data"));
                    return result;
                }
                Thread.sleep(1000);
            }
            if (step != null) jobService.failStep(step.getId(), "Task timeout: " + taskName);
            return Map.of("status", "FAILED", "error", "Timeout");
        } catch (Exception e) {
            if (step != null) jobService.failStep(step.getId(), "Task error: " + e.getMessage());
            return Map.of("status", "FAILED", "error", e.getMessage());
        }
    }
}
