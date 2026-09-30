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
    private final ObjectMapper objectMapper;

    public RollingConfigUpdateJobHandler(
            JobService jobService,
            ExternalClusterService externalClusterService,
            ClusterRepository clusterRepository,
            KafkaAdminService kafkaAdminService,
            ObjectMapper objectMapper) {
        this.jobService = jobService;
        this.externalClusterService = externalClusterService;
        this.clusterRepository = clusterRepository;
        this.kafkaAdminService = kafkaAdminService;
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

            Cluster cluster = clusterRepository.findById(clusterId).orElse(null);
            if (cluster == null) {
                failJob(job, "Cluster not found");
                return;
            }

            jobService.updateJobStatus(job.getId(), JobStatus.IN_PROGRESS);
            List<JobStep> steps = jobService.getSteps(job.getId());

            // We iterate over the steps that were pre-created by ConfigController
            Map<String, String> hostToBackupFile = new HashMap<>();

            for (JobStep step : steps) {
                if (step.getStatus() == JobStepStatus.SUCCESS) continue;

                jobService.startStep(step.getId());

                if (step.getName().equals("PREFLIGHT")) {
                    try {
                        kafkaAdminService.describeClusterNodes(clusterId);
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
                        hostToBackupFile.put(host, backupResult.get("backupFilePath") != null ? backupResult.get("backupFilePath").toString() : null);
                    }
                    if (success) jobService.completeStep(step.getId(), "All configurations backed up");
                    else return;
                } else if (step.getName().startsWith("WRITE_CONFIG")) {
                    String host = step.getTargetId();
                    Map<String, Object> change = findChangeForHost(changes, host);
                    if (change != null) {
                        String configFilePath = (String) change.get("configFilePath");
                        List<Map<String, Object>> properties = (List<Map<String, Object>>) change.get("properties");
                        Map<String, String> configChanges = new HashMap<>();
                        if (properties != null) {
                            for (Map<String, Object> prop : properties) {
                                configChanges.put((String) prop.get("key"), (String) prop.get("newValue"));
                            }
                        }
                        Map<String, Object> writeResult = executeAgentTask(clusterId, host, "write_config", Map.of(
                                "configFilePath", configFilePath,
                                "configChanges", configChanges
                        ), step);
                        if (writeResult == null || !"SUCCESS".equals(writeResult.get("status"))) {
                            executeRollback(job, clusterId, host, configFilePath, hostToBackupFile.get(host), (String) change.get("serviceName"), rollingRestart);
                            return;
                        }
                    } else {
                        jobService.completeStep(step.getId(), "No changes");
                    }
                } else if (step.getName().startsWith("RESTART_SERVICE")) {
                    String host = step.getTargetId();
                    Map<String, Object> change = findChangeForHost(changes, host);
                    if (change != null) {
                        String serviceName = (String) change.get("serviceName");
                        Map<String, Object> restartResult = executeAgentTask(clusterId, host, "restart_service", Map.of(
                                "serviceName", serviceName
                        ), step);
                        if (restartResult == null || !"SUCCESS".equals(restartResult.get("status"))) {
                            executeRollback(job, clusterId, host, (String) change.get("configFilePath"), hostToBackupFile.get(host), serviceName, true);
                            return;
                        }
                    } else {
                        jobService.completeStep(step.getId(), "No restart");
                    }
                } else if (step.getName().startsWith("HEALTH_CHECK")) {
                    String host = step.getTargetId();
                    Map<String, Object> change = findChangeForHost(changes, host);
                    if (change != null) {
                        boolean healthy = verifyHealthWithTimeout(clusterId, 30);
                        if (!healthy) {
                            jobService.failStep(step.getId(), "Health check failed after restart");
                            executeRollback(job, clusterId, host, (String) change.get("configFilePath"), hostToBackupFile.get(host), (String) change.get("serviceName"), true);
                            return;
                        } else {
                            jobService.completeStep(step.getId(), "Node healthy");
                        }
                    } else {
                        jobService.completeStep(step.getId(), "OK");
                    }
                } else if (step.getName().startsWith("FINAL_VERIFY")) {
                    jobService.completeStep(step.getId(), "All changes successfully applied");
                }
            }
            
            jobService.updateJobStatus(job.getId(), JobStatus.SUCCESS);
            
        } catch (Exception e) {
            failJob(job, "Rolling config update failed: " + e.getMessage());
        }
    }

    private void failJob(Job job, String message) {
        jobService.updateJobStatus(job.getId(), JobStatus.FAILED);
        jobService.appendLog(job.getId(), message);
    }
    
    private void executeRollback(Job job, UUID clusterId, String host, String configFilePath, String backupFilePath, String serviceName, boolean doRestart) {
        try {
            executeAgentTask(clusterId, host, "restore_backup", Map.of(
                    "configFilePath", configFilePath,
                    "backupFilePath", backupFilePath
            ), null);
            
            if (doRestart) {
                executeAgentTask(clusterId, host, "restart_service", Map.of(
                        "serviceName", serviceName
                ), null);
                
                verifyHealthWithTimeout(clusterId, 30);
            }
            failJob(job, "Failed on " + host + ". Node was rolled back.");
        } catch (Exception e) {
            failJob(job, "Failed on " + host + ". Rollback also failed! " + e.getMessage());
        }
    }
    
    private Map<String, Object> findChangeForHost(List<Map<String, Object>> changes, String host) {
        for (Map<String, Object> change : changes) {
            if (host.equals(change.get("host"))) return change;
        }
        return null;
    }
    
    private boolean verifyHealthWithTimeout(UUID clusterId, int secondsToWait) {
        long end = System.currentTimeMillis() + (secondsToWait * 1000L);
        while (System.currentTimeMillis() < end) {
            try {
                kafkaAdminService.describeClusterNodes(clusterId);
                kafkaAdminService.listTopics(clusterId);
                return true; 
            } catch (Exception e) {
                try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            }
        }
        return false;
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
