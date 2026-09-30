package io.translab.tantor.server.web;

import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.service.DeploymentService;
import io.translab.tantor.server.service.JobService;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/actions")
@RequiredArgsConstructor
public class ClusterActionsController {

    private final DeploymentService deploymentService;
    private final ClusterRepository clusterRepository;
    private final JobService jobService;
    private final ObjectMapper objectMapper;
    private final AuditService auditService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    @PostMapping("/rolling-restart")
    public ResponseEntity<Map<String, String>> startRollingRestart(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody(required = false) RollingRestartRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ROLLING_RESTART)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        return clusterRepository.findWithServicesById(clusterId)
                .map(cluster -> {
                    Job job = new Job();
                    job.setType(JobType.ROLLING_RESTART);
                    job.setStatus(JobStatus.PENDING);
                    job.setRollbackSupported(true);
                    job.setResourceKey("cluster:" + clusterId);
                    try {
                        long uniqueHosts = cluster.getServices() == null ? 0 : cluster.getServices().stream()
                                .map(ClusterServiceAssignment::getHostId).distinct().count();
                        boolean confirmedSingleNode = request != null && request.confirmSingleNode;
                        if (uniqueHosts == 1 && !confirmedSingleNode) {
                            return ResponseEntity.badRequest().body(Map.of("error",
                                    "Only one node is present. Explicit confirmation is required because Kafka will be interrupted."));
                        }
                        job.setPayload(objectMapper.writeValueAsString(Map.of(
                                "clusterId", clusterId.toString(),
                                "confirmSingleNode", confirmedSingleNode)));
                    } catch (Exception e) {
                        return ResponseEntity.internalServerError().body(Map.of("error", "Unable to create rolling restart job."));
                    }

                    java.util.List<JobStep> steps = new java.util.ArrayList<>();
                    int order = 1;
                    if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
                        JobStep step = new JobStep();
                        step.setStepOrder(order++);
                        step.setTargetId(clusterId.toString());
                        step.setName("Restart external Kafka cluster");
                        try {
                            step.setPayload(objectMapper.writeValueAsString(Map.of("operation", "external")));
                        } catch (Exception e) {
                            return ResponseEntity.internalServerError().body(Map.of("error", "Unable to create external restart step."));
                        }
                        steps.add(step);
                    }
                    for (ClusterServiceAssignment service : cluster.getServices() == null ? java.util.List.<ClusterServiceAssignment>of() : cluster.getServices()) {
                        JobStep step = new JobStep();
                        step.setStepOrder(order++);
                        step.setTargetId(service.getHostId());
                        step.setName("Restart " + service.getRole() + " node " + service.getNodeId() + " on " + service.getHostId());
                        try {
                            step.setPayload(objectMapper.writeValueAsString(Map.of(
                                    "hostId", service.getHostId(),
                                    "role", service.getRole(),
                                    "nodeId", service.getNodeId() == null ? 0 : service.getNodeId()
                            )));
                        } catch (Exception e) {
                            return ResponseEntity.internalServerError().body(Map.of("error", "Unable to create rolling restart steps."));
                        }
                        steps.add(step);
                    }
                    Job saved = jobService.createJob(job, steps);
                    return ResponseEntity.ok(Map.of("jobId", saved.getId().toString(), "status", saved.getStatus().name()));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/normal-restart")
    public ResponseEntity<Map<String, String>> startNormalRestart(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ROLLING_RESTART)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        return clusterRepository.findById(clusterId)
                .map(cluster -> {
                    int count = 0;
                    if (cluster.getServices() != null) {
                        for (var service : cluster.getServices()) {
                            deploymentService.restartService(clusterId, service.getHostId(),
                                    systemdServiceName(service.getRole()));
                            count++;
                        }
                    }
                    auditService.record("RESTART", "NORMAL_RESTART_REQUESTED", "CLUSTER", clusterId.toString(),
                            clusterId, "REQUESTED", null, Map.of("taskCount", count), null,
                            Map.of("mode", String.valueOf(cluster.getMode())));
                    return ResponseEntity.ok(Map.of(
                            "status", "scheduled",
                            "tasks", String.valueOf(count)));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/enable-monitoring")
    public ResponseEntity<Map<String, String>> enableMonitoring(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody MonitoringRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.MONITORING_ENABLE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        return clusterRepository.findWithServicesById(clusterId)
                .map(cluster -> {
                    String hostId = request.hostId;
                    if (hostId == null || hostId.isBlank()) {
                        hostId = cluster.getServices().stream().findFirst()
                                .map(ClusterServiceAssignment::getHostId).orElse("");
                    }
                    if (hostId.isBlank()) {
                        return ResponseEntity.badRequest().body(Map.of("error", "A target host is required."));
                    }
                    Job job = new Job();
                    job.setType(JobType.MONITORING_ENABLEMENT);
                    job.setStatus(JobStatus.PENDING);
                    job.setRollbackSupported(true);
                    job.setResourceKey("cluster:" + clusterId);
                    JobStep step = new JobStep();
                    step.setStepOrder(1);
                    step.setName("Install Prometheus and Grafana on " + hostId);
                    step.setTargetId(hostId);
                    try {
                        job.setPayload(objectMapper.writeValueAsString(Map.of("clusterId", clusterId.toString())));
                        step.setPayload(objectMapper.writeValueAsString(Map.of(
                                "hostId", hostId,
                                "installDir", request.installDir == null ? "/opt/tantor/monitoring" : request.installDir,
                                "prometheusUrl", request.prometheusUrl == null ? "" : request.prometheusUrl,
                                "grafanaUrl", request.grafanaUrl == null ? "" : request.grafanaUrl
                        )));
                    } catch (Exception e) {
                        return ResponseEntity.internalServerError().body(Map.of("error", "Unable to create monitoring job."));
                    }
                    Job saved = jobService.createJob(job, java.util.List.of(step));
                    return ResponseEntity.ok(Map.of("jobId", saved.getId().toString(), "status", saved.getStatus().name()));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/tasks/{taskId}")
    public ResponseEntity<Map<String, String>> getTaskStatus(@PathVariable UUID clusterId, @PathVariable String taskId) {
        try {
            Job job = jobService.getJob(UUID.fromString(taskId));
            return ResponseEntity.ok(Map.of("taskId", taskId, "status", job.getStatus().name()));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/tasks/{taskId}/retry")
    public ResponseEntity<Void> retryTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        return deploymentService.retryTask(taskId) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @PostMapping("/tasks/{taskId}/resume")
    public ResponseEntity<Void> resumeTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        return deploymentService.resumeTask(taskId) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @PostMapping("/tasks/{taskId}/rollback")
    public ResponseEntity<Void> rollbackTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        return deploymentService.rollbackTask(clusterId, taskId) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @PostMapping("/tasks/{taskId}/cleanup")
    public ResponseEntity<Void> cleanupTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        return deploymentService.cleanupTask(clusterId, taskId) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    private String systemdServiceName(String role) {

        if ("controller".equals(role))
            return "controller";
        if ("zookeeper".equals(role))
            return "zookeeper";
        if ("broker_controller".equals(role) || "broker_zookeeper".equals(role))
            return "kafka";
        return "broker";
    }

    public static class MonitoringRequest {
        public String hostId;
        public String installDir;
        public String prometheusUrl;
        public String grafanaUrl;
    }

    public static class RollingRestartRequest {
        public boolean confirmSingleNode;
    }
}
