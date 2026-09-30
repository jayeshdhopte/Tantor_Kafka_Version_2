package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.dto.HostHeartbeatDto;
import io.translab.tantor.server.dto.HostRegistrationDto;
import io.translab.tantor.server.dto.TaskDto;
import io.translab.tantor.server.dto.TaskResultDto;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class AgentService {
    private final HostRepository hostRepository;
    private final TaskRepository taskRepository;
    private final io.translab.tantor.server.repository.ClusterRepository clusterRepository;
    private final ObjectMapper objectMapper;
    private final ParcelService parcelService;
    private final ActivityAlertService activityAlertService;
    private final AuditService auditService;

    @Value("${tantor.hosts.heartbeat-timeout-seconds:90}")
    private long heartbeatTimeoutSeconds;

    @Value("${tantor.tasks.claim-lease-seconds:300}")
    private long taskClaimLeaseSeconds;

    @Value("${tantor.tasks.max-claim-attempts:3}")
    private int maxTaskClaimAttempts;

    public enum HeartbeatResult {
        ACCEPTED,
        NOT_FOUND,
        SOURCE_MISMATCH
    }

    @Transactional
    public boolean registerHost(HostRegistrationDto dto, String sourceIp) {
        Host existing = hostRepository.findById(dto.getHostId()).orElse(null);
        if (existing != null && !Boolean.TRUE.equals(existing.getRemoved()) && !sourceMatches(existing, sourceIp) && !isHeartbeatStale(existing)) {
            log.warn("Rejected duplicate registration for host {} from {}. Registered host IP is {}.",
                    dto.getHostId(), sourceIp, existing.getHostIp());
            return false;
        }
        Map<String, Object> oldValue = existing == null ? Map.of() : Map.of(
                "hostname", String.valueOf(existing.getHostname()),
                "status", String.valueOf(existing.getStatus()),
                "agentVersion", String.valueOf(existing.getAgentVersion()));
        Host host = existing == null ? new Host() : existing;
        host.setId(dto.getHostId());
        host.setHostname(dto.getHostname());
        List<String> selectedIps = selectHostIp(dto.getIpAddresses());
        if (!selectedIps.isEmpty()) {
            host.setHostIp(selectedIps.get(0));
        }

        String selectedHostIp = selectedIps.isEmpty() ? sourceIp : selectedIps.get(0);
        String agentName = firstNonBlank(dto.getAgentName(), host.getAgentName(), dto.getHostname(), dto.getHostId());
        String agentPath = firstNonBlank(dto.getAgentPath(), host.getAgentPath(), "/srv/tantor-agent/tantor-agent-linux");
        String auditActor = selectedHostIp == null || selectedHostIp.isBlank()
                ? agentName
                : agentName + " (" + selectedHostIp + ")";
        host.setResourceType("HOST");
        host.setUser(auditActor);
        host.setRemoved(false);
        host.setAction(existing == null ? "HOST_REGISTERED" : "HOST_UPDATED");
        host.setOsDetails(dto.getOsDetails());
        host.setAgentVersion(dto.getAgentVersion());
        host.setAgentName(agentName);
        host.setAgentPath(agentPath);

        if (host.getStatus() == null) {
            host.setStatus("PENDING");
        } else if (!"PENDING".equals(host.getStatus()) && !"OCCUPIED".equalsIgnoreCase(host.getStatus())) {
            host.setStatus("ONLINE");
        }
        host.setLastHeartbeat(OffsetDateTime.now());
        
        hostRepository.save(host);
        log.info("Registered host: {}", dto.getHostId());
        activityAlertService.logAudit("INFO", "AGENT", existing != null ? "RECONNECT" : "REGISTER",
                existing != null ? "Agent reconnected" : "Agent registered", "HOST", dto.getHostId(), host.getClusterId(),
                null, host.getStatus(), "SUCCESS", existing != null ? null : "PENDING", "agentVersion=" + dto.getAgentVersion());
        return true;
    }

    @Transactional
    public void registerHost(HostRegistrationDto dto) {
        registerHost(dto, null);
    }

    @Transactional
    public HeartbeatResult processHeartbeat(HostHeartbeatDto dto, String sourceIp) {
        return hostRepository.findById(dto.getHostId()).map(host -> {
            if (!sourceMatches(host, sourceIp)) {
                if (isHeartbeatStale(host)) {
                    log.warn("Host {} heartbeat from {} does not match registered IP {}, but the registered heartbeat is stale. Requesting re-registration.",
                            dto.getHostId(), sourceIp, host.getHostIp());
                    return HeartbeatResult.NOT_FOUND;
                }
                log.warn("Rejected heartbeat for host {} from {}. Registered host IP is {}.",
                        dto.getHostId(), sourceIp, host.getHostIp());
                return HeartbeatResult.SOURCE_MISMATCH;
            }
            host.setCpuUsagePct(dto.getCpuUsagePct());
            host.setMemTotalMb(dto.getMemTotalMb());
            host.setMemUsedMb(dto.getMemUsedMb());
            host.setDiskTotalGb(dto.getDiskTotalGb());
            host.setDiskUsedGb(dto.getDiskUsedGb());
            host.setJavaVersion(dto.getJavaVersion());
            host.setLastHeartbeat(OffsetDateTime.now());

            host.setRemoved(false);
            if (!"PENDING".equals(host.getStatus()) && !"OCCUPIED".equalsIgnoreCase(host.getStatus())) {
                host.setStatus("ONLINE");
            }
            hostRepository.save(host);
            log.debug("Processed heartbeat for host: {}", dto.getHostId());
            return HeartbeatResult.ACCEPTED;
        }).orElse(HeartbeatResult.NOT_FOUND);
    }

    @Transactional
    public boolean processHeartbeat(HostHeartbeatDto dto) {
        return processHeartbeat(dto, null) == HeartbeatResult.ACCEPTED;
    }

    private List<String> selectHostIp(List<String> addresses) {
        if (addresses == null) return List.of();
        String selected = addresses.stream()
                .filter(java.util.Objects::nonNull)
                .map(String::trim)
                .filter(ip -> ip.matches("^(?:\\d{1,3}\\.){3}\\d{1,3}$"))
                .filter(ip -> !ip.startsWith("127.") && !ip.startsWith("169.254."))
                .sorted(java.util.Comparator.comparingInt(this::ipPriority))
                .findFirst().orElse(null);
        return selected == null ? List.of() : List.of(selected);
    }

    private int ipPriority(String ip) {
        if (ip.startsWith("192.168.")) return 0;
        if (ip.startsWith("10.")) return 1;
        if (ip.matches("^172\\.(1[6-9]|2\\d|3[01])\\..*")) return 2;
        return 3;
    }

    private boolean sourceMatches(Host host, String sourceIp) {
        String normalized = normalizeIp(sourceIp);
        if (normalized == null || isLoopback(normalized)) {
            return true;
        }
        if (normalized.equals(normalizeIp(host.getHostIp()))) {
            return true;
        }
        if (host.getIpAddresses() == null || host.getIpAddresses().isBlank()) {
            return host.getHostIp() == null || host.getHostIp().isBlank();
        }
        try {
            List<String> knownIps = objectMapper.readValue(host.getIpAddresses(), new TypeReference<List<String>>() {});
            return knownIps.stream().map(this::normalizeIp).anyMatch(normalized::equals);
        } catch (Exception e) {
            log.warn("Failed to parse known IPs for host {}", host.getId(), e);
            return false;
        }
    }

    private String normalizeIp(String value) {
        if (value == null) return null;
        String ip = value.trim();
        if (ip.isBlank()) return null;
        if (ip.startsWith("::ffff:")) {
            ip = ip.substring("::ffff:".length());
        }
        int portIndex = ip.lastIndexOf(':');
        if (portIndex > -1 && ip.indexOf(':') == portIndex && ip.substring(portIndex + 1).matches("\\d+")) {
            ip = ip.substring(0, portIndex);
        }
        return ip;
    }

    private boolean isLoopback(String ip) {
        return "localhost".equalsIgnoreCase(ip) || "0:0:0:0:0:0:0:1".equals(ip) || "::1".equals(ip) || ip.startsWith("127.");
    }

    private boolean isHeartbeatStale(Host host) {
        if (host.getLastHeartbeat() == null) {
            return true;
        }
        long timeoutSeconds = Math.max(heartbeatTimeoutSeconds, 1);
        return host.getLastHeartbeat().isBefore(OffsetDateTime.now().minusSeconds(timeoutSeconds));
    }

    @Transactional
    public List<TaskDto> getPendingTasks(String hostId) {
        OffsetDateTime now = OffsetDateTime.now();
        int maxAttempts = Math.max(maxTaskClaimAttempts, 1);
        taskRepository.failExpiredClaims(hostId, now, maxAttempts);
        taskRepository.releaseExpiredClaims(hostId, now, maxAttempts);
        List<Task> pendingTasks = taskRepository.findByHostIdAndStatusOrderByCreatedAtAsc(hostId, "PENDING");
        OffsetDateTime leaseExpiresAt = now.plusSeconds(Math.max(taskClaimLeaseSeconds, 1));

        return pendingTasks.stream().map(t -> {
            String claimToken = UUID.randomUUID().toString();
            if (taskRepository.claimPendingTask(t.getId(), hostId, claimToken, now, leaseExpiresAt) != 1) {
                return null;
            }
            Task claimed = taskRepository.findById(t.getId()).orElse(null);
            if (claimed == null) return null;
            
            TaskDto dto = new TaskDto();
            dto.setTaskId(claimed.getId().toString());
            dto.setClaimToken(claimToken);
            if (claimed.getClusterId() != null) {
                dto.setClusterId(claimed.getClusterId().toString());
            }
            dto.setCommand(claimed.getCommand());
            dto.setArtifactUrl(claimed.getArtifactUrl());
            dto.setChecksum(claimed.getChecksum());
            try {
                if (claimed.getParameters() != null) {
                    dto.setParameters(objectMapper.readValue(claimed.getParameters(), new TypeReference<Map<String, Object>>() {}));
                }
            } catch (JsonProcessingException e) {
                log.warn("Failed to deserialize parameters for task {}", claimed.getId(), e);
            }
            return dto;
        }).filter(java.util.Objects::nonNull).collect(Collectors.toList());
    }

    @Transactional
    public void processTaskResult(TaskResultDto dto) {
        try {
            UUID taskId = UUID.fromString(dto.getTaskId());
            taskRepository.findById(taskId).ifPresent(task -> {
                if (isTerminalTaskStatus(task.getStatus())) {
                    log.info("Ignoring duplicate result for terminal task {}", taskId);
                    return;
                }
                if (dto.getClaimToken() == null || !dto.getClaimToken().equals(task.getClaimToken())) {
                    log.warn("Ignoring result for task {} with a missing or stale claim token", taskId);
                    return;
                }
                if (!isTerminalTaskStatus(dto.getStatus())) {
                    task.setCurrentStep(dto.getCurrentStep());
                    try {
                        Map<String, String> stepLogsMap = new java.util.LinkedHashMap<>();
                        if (task.getStepLogs() != null && !task.getStepLogs().isBlank()) {
                            stepLogsMap = objectMapper.readValue(task.getStepLogs(), new TypeReference<Map<String, String>>() {});
                        }
                        String existingLog = stepLogsMap.getOrDefault(dto.getCurrentStep(), "");
                        String newLog = dto.getLogOutput() != null ? dto.getLogOutput() : "";
                        if (!newLog.isEmpty()) {
                            stepLogsMap.put(dto.getCurrentStep(), existingLog + newLog);
                            task.setStepLogs(objectMapper.writeValueAsString(stepLogsMap));
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse step logs", e);
                    }
                    taskRepository.save(task);
                } else {
                    task.setStatus(dto.getStatus());
                    String resultOutput = resultOutput(dto);
                    if (resultOutput != null) {
                        task.setLogOutput(resultOutput);
                    }
                    task.setErrorMsg(dto.getErrorMsg());
                    if ((task.getErrorMsg() == null || task.getErrorMsg().isBlank())
                            && dto.getFailedReason() != null && !dto.getFailedReason().isBlank()) {
                        task.setErrorMsg(dto.getFailedReason());
                    }
                    task.setCurrentStep(dto.getCurrentStep());
                    try {
                        if (dto.getCurrentStep() != null && resultOutput != null && !resultOutput.isEmpty()) {
                            Map<String, String> stepLogsMap = new java.util.LinkedHashMap<>();
                            if (task.getStepLogs() != null && !task.getStepLogs().isBlank()) {
                                stepLogsMap = objectMapper.readValue(task.getStepLogs(), new TypeReference<Map<String, String>>() {});
                            }
                            String existingLog = stepLogsMap.getOrDefault(dto.getCurrentStep(), "");
                            stepLogsMap.put(dto.getCurrentStep(), existingLog + resultOutput);
                            task.setStepLogs(objectMapper.writeValueAsString(stepLogsMap));
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse or write step logs for task {}", taskId, e);
                    }
                    
                    taskRepository.save(task);
                    log.info("Task {} completed with status: {}", taskId, dto.getStatus());
                    activityAlertService.logAudit("FAILED".equalsIgnoreCase(dto.getStatus()) ? "ERROR" : "INFO",
                            "TASK", task.getCommand(), "Task completed with status " + dto.getStatus(), "TASK", taskId.toString(),
                            task.getClusterId(), "IN_PROGRESS", dto.getStatus(), dto.getStatus(), null,
                            dto.getErrorMsg());
                    if ("CHECK_PREREQUISITES".equals(task.getCommand())
                            || "CHECK_PORTS".equals(task.getCommand())
                            || "APPLY_PREREQUISITES".equals(task.getCommand())) {
                        Map<String, Object> prerequisiteDetails = new java.util.LinkedHashMap<>();
                        prerequisiteDetails.put("taskId", taskId.toString());
                        prerequisiteDetails.put("hostId", task.getHostId());
                        prerequisiteDetails.put("result", dto.getStatus());
                        if (dto.getErrorMsg() != null && !dto.getErrorMsg().isBlank()) {
                            prerequisiteDetails.put("error", dto.getErrorMsg());
                        }
                        if (dto.getFailedReason() != null && !dto.getFailedReason().isBlank()) {
                            prerequisiteDetails.put("failedReason", dto.getFailedReason());
                        }
                        String action = "APPLY_PREREQUISITES".equals(task.getCommand())
                                ? "PREREQUISITE_FIX_COMPLETED"
                                : "CHECK_PORTS".equals(task.getCommand())
                                ? "PORT_CHECK_COMPLETED"
                                : "PREREQUISITE_CHECK_COMPLETED";
                        String auditActor = taskParameter(task, "requested_by");
                        if (auditActor == null || auditActor.isBlank()) {
                            auditActor = "agent:" + task.getHostId();
                        }
                        auditService.recordAs(auditActor, "AGENT", null,
                                "PREREQUISITE", action, "HOST", task.getHostId(),
                                task.getClusterId(), dto.getStatus(), null, null, null, prerequisiteDetails);
                    }
                    if ("REBOOT_HOST".equals(task.getCommand())) {
                        String auditActor = taskParameter(task, "requested_by");
                        if (auditActor == null || auditActor.isBlank()) {
                            auditActor = "agent:" + task.getHostId();
                        }
                        auditService.recordAs(auditActor, "AGENT", null,
                                "RESTART", "HOST_REBOOT_SCHEDULED", "HOST", task.getHostId(), task.getClusterId(),
                                dto.getStatus(), null, null, null,
                                Map.of("taskId", taskId.toString(), "result", dto.getStatus()));
                    }
                    if ("INSTALL_KAFKA".equals(task.getCommand())) {
                        if ("SUCCESS".equalsIgnoreCase(dto.getStatus())) {
                            hostRepository.findById(task.getHostId()).ifPresent(host -> {
                                host.setStatus("OCCUPIED");
                                hostRepository.save(host);
                            });
                        }
                    }
                    parcelService.processTaskResult(task);
                    cancelPendingClusterDeploymentTasks(task);

                    if ("SUCCESS".equals(dto.getStatus())) {
                        String originalTaskId = taskParameter(task, "original_task_id");
                        if (originalTaskId != null && !originalTaskId.isBlank()) {
                            try {
                                taskRepository.findById(UUID.fromString(originalTaskId)).ifPresent(originalTask -> {
                                    if ("ROLLBACK_DEPLOYMENT".equals(task.getCommand())) {
                                        originalTask.setStatus("ROLLBACK_DONE");
                                    } else if ("DELETE_CLUSTER".equals(task.getCommand())) {
                                        originalTask.setStatus("CLEANUP_DONE");
                                    }
                                    taskRepository.save(originalTask);
                                });
                            } catch (Exception e) {
                                log.warn("Failed to update original task status", e);
                            }
                        }
                    }

                    if (task.getClusterId() != null) {
                        clusterRepository.findById(task.getClusterId()).ifPresent(cluster -> updateClusterStatus(cluster, task));
                    } else {
                        // Legacy tasks created before cluster_id was added can only be mapped through host assignment.
                        hostRepository.findById(task.getHostId()).ifPresent(host -> {
                            if (host.getClusterId() != null) {
                                clusterRepository.findById(host.getClusterId()).ifPresent(cluster -> updateClusterStatus(cluster, task));
                            }
                        });
                    }
                }
            });
        } catch (IllegalArgumentException e) {
            log.error("Invalid task ID format: {}", dto.getTaskId(), e);
        }
    }

    private String resultOutput(TaskResultDto dto) {
        StringBuilder output = new StringBuilder(dto.getLogOutput() == null ? "" : dto.getLogOutput());
        if (dto.getPlanHash() != null && !dto.getPlanHash().isBlank()) {
            if (!output.isEmpty()) output.append("\n");
            output.append("plan_hash=").append(dto.getPlanHash());
        }
        if (dto.getChecks() != null && !dto.getChecks().isEmpty()) {
            try {
                if (!output.isEmpty()) output.append("\n");
                output.append("checks=").append(objectMapper.writeValueAsString(dto.getChecks()));
            } catch (JsonProcessingException e) {
                log.warn("Failed to serialize task checks for {}", dto.getTaskId(), e);
            }
        }
        return output.isEmpty() ? null : output.toString();
    }

    private void cancelPendingClusterDeploymentTasks(Task failedTask) {
        if (failedTask.getClusterId() == null
                || !"INSTALL_KAFKA".equals(failedTask.getCommand())
                || !"FAILED".equals(failedTask.getStatus())) {
            return;
        }

        taskRepository.findByClusterIdOrderByCreatedAtDesc(failedTask.getClusterId()).stream()
                .filter(task -> !task.getId().equals(failedTask.getId()))
                .filter(task -> "INSTALL_KAFKA".equals(task.getCommand()))
                .filter(task -> "PENDING".equals(task.getStatus()))
                .forEach(task -> {
                    task.setStatus("CANCELLED");
                    task.setErrorMsg("Cancelled because another node failed during cluster deployment.");
                    taskRepository.save(task);
                    log.warn("Cancelled pending deployment task {} after failure of {}", task.getId(), failedTask.getId());
                });
    }

    private void updateClusterStatus(io.translab.tantor.server.domain.Cluster cluster, Task currentTask) {
        String command = currentTask.getCommand();
        String status = currentTask.getStatus();
        
        if ("FAILED".equals(status)) {
            if ("UPGRADE_KAFKA".equals(command) && upgradeRollbackCompleted(currentTask)) {
                cluster.setStatus("SUCCESS");
            } else {
                cluster.setStatus("FAILED");
            }
        } else if ("VALIDATING".equals(status)) {
            cluster.setStatus("VALIDATING");
        } else if ("RUNNING".equals(status) || "IN_PROGRESS".equals(status)) {
            cluster.setStatus("DELETE_CLUSTER".equals(command) ? "DELETING" : "RUNNING");
        } else if ("SUCCESS".equals(status)) {
            boolean allSuccess = true;
            for (io.translab.tantor.server.domain.ClusterServiceAssignment svc : cluster.getServices()) {
                List<Task> hostTasks = currentTask.getClusterId() != null
                    ? taskRepository.findByClusterIdAndHostIdAndCommandOrderByCreatedAtDesc(currentTask.getClusterId(), svc.getHostId(), command)
                    : taskRepository.findByHostIdAndCommandOrderByCreatedAtDesc(svc.getHostId(), command);
                if (hostTasks.isEmpty() || !"SUCCESS".equals(hostTasks.get(0).getStatus())) {
                    allSuccess = false;
                    break;
                }
            }
            if (allSuccess) {
                if ("DELETE_CLUSTER".equals(command)) {
                    cluster.setStatus("DELETED");
                    cluster.setDeletedAt(java.time.Instant.now());
                    releaseClusterHosts(cluster);
                } else if ("UPGRADE_KAFKA".equals(command)) {
                    String targetVersion = taskParameter(currentTask, "target_version");
                    if (targetVersion == null || targetVersion.isBlank()) {
                        targetVersion = taskParameter(currentTask, "version");
                    }
                    if (targetVersion != null && !targetVersion.isBlank()) {
                        cluster.setKafkaVersion(targetVersion);
                    }
                    cluster.setStatus("SUCCESS");
                } else {
                    cluster.setStatus("SUCCESS");
                }
            }
        }
        // Preserve the actor that initiated the deployment. This callback runs asynchronously without the user security context, so replacing updatedBy with system caused completed deployment audit events to be attributed to system.
        clusterRepository.save(cluster);
    }

    private boolean upgradeRollbackCompleted(Task task) {
        String error = task.getErrorMsg() == null ? "" : task.getErrorMsg();
        String logs = task.getLogOutput() == null ? "" : task.getLogOutput();
        return error.contains("Rollback completed") || logs.contains("Rollback completed");
    }

    private boolean isTerminalTaskStatus(String status) {
        return "SUCCESS".equalsIgnoreCase(status) || "FAILED".equalsIgnoreCase(status);
    }
    @SuppressWarnings("unchecked")
    private String taskParameter(Task task, String name) {
        if (task.getParameters() == null || task.getParameters().isBlank()) {
            return null;
        }
        try {
            Map<String, Object> params = objectMapper.readValue(task.getParameters(), Map.class);
            Object value = params.get(name);
            return value == null ? null : String.valueOf(value);
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse task parameters for task {}", task.getId(), e);
            return null;
        }
    }

    private void releaseClusterHosts(io.translab.tantor.server.domain.Cluster cluster) {
        if (cluster.getServices() == null) {
            return;
        }
        for (io.translab.tantor.server.domain.ClusterServiceAssignment svc : cluster.getServices()) {
            hostRepository.findById(svc.getHostId()).ifPresent(host -> {
                if (cluster.getId().equals(host.getClusterId())) {
                    host.setClusterId(null);
                    host.setStatus("ONLINE");
                    hostRepository.save(host);
                }
            });
        }
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }
}
