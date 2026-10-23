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
    @org.springframework.beans.factory.annotation.Autowired private io.translab.tantor.server.service.ExternalKafkaManagement externalKafka;
    private Cluster managedCluster(UUID id){return externalKafka != null && externalKafka.external(id) ? externalKafka.resolve(id) : clusterRepository.findWithServicesById(id).orElseThrow(() -> new IllegalArgumentException("Cluster not found."));}

    private final DeploymentService deploymentService;
    private final ClusterRepository clusterRepository;
    private final JobService jobService;
    private final ObjectMapper objectMapper;
    private final AuditService auditService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;
    private final io.translab.tantor.server.service.RestartNodeDisplayService restartDisplay;

    @GetMapping("/rolling-restart/topology")
    public ResponseEntity<?> restartTopology(@RequestHeader(value = "Authorization", required = false) String authorization,
                                             @PathVariable UUID clusterId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ROLLING_RESTART))
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        Cluster cluster = managedCluster(clusterId);
        return ResponseEntity.ok(io.translab.tantor.server.service.RollingRestartPlan.topology(cluster).stream().map(service -> {
            var row=new java.util.LinkedHashMap<String,Object>();
            row.put("key",io.translab.tantor.server.service.RollingRestartPlan.key(service));row.put("hostId",service.getHostId());
            row.put("serviceName",io.translab.tantor.server.service.RollingRestartPlan.unit(service));row.put("role",service.getRole());row.put("nodeId",service.getNodeId()==null?0:service.getNodeId());
            row.putAll(restartDisplay.identity(service.getHostId()));
            if(externalKafka != null && externalKafka.target(service.getHostId())) {try{externalKafka.require(service.getHostId(),false,true);}catch(IllegalStateException reason){row.put("blockedReason",reason.getMessage());}}
            return row;
        }).toList());
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/rolling-restart")
    public ResponseEntity<Map<String, String>> startRollingRestart(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody(required = false) RollingRestartRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ROLLING_RESTART))
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        Cluster cluster = managedCluster(clusterId);
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode()))
            return ResponseEntity.badRequest().body(Map.of("error", "Managed rolling restart requires internal node agents."));
        if (!"SUCCESS".equalsIgnoreCase(cluster.getStatus()))
            return ResponseEntity.badRequest().body(Map.of("error", "Cluster must finish deployment before a rolling restart."));
        RollingRestartRequest effective = request == null ? new RollingRestartRequest() : request;
        effective.scope = effective.scope == null ? "BROKERS" : effective.scope.toUpperCase(java.util.Locale.ROOT);
        var topology = io.translab.tantor.server.service.RollingRestartPlan.topology(cluster);
        var selected = io.translab.tantor.server.service.RollingRestartPlan.select(cluster, effective.scope, effective.targets);
        boolean single = topology.stream().map(ClusterServiceAssignment::getHostId).distinct().count() == 1;
        if (single && !effective.confirmSingleNode)
            return ResponseEntity.badRequest().body(Map.of("error", "Confirm interruption for this single-host cluster."));
        if (selected.stream().anyMatch(service -> service.getRole().contains("_") && !effective.confirmCombined))
            return ResponseEntity.badRequest().body(Map.of("error", "Confirm that combined Kafka processes restart both roles together."));
        Job job = new Job();
        job.setType(JobType.ROLLING_RESTART);
        job.setStatus(JobStatus.PENDING);
        job.setRollbackSupported(false);
        job.setResourceKey("cluster:" + clusterId);
        var steps = new java.util.ArrayList<JobStep>();
        try {
            job.setPayload(objectMapper.writeValueAsString(Map.of("clusterId", clusterId.toString(), "scope", effective.scope,
                    "confirmSingleNode", effective.confirmSingleNode, "confirmCombined", effective.confirmCombined)));
            for (var service : selected) {
                JobStep step = new JobStep();
                step.setStepOrder(steps.size() + 1);
                step.setTargetId(service.getHostId());
                var payload=new java.util.LinkedHashMap<String,Object>();
                payload.put("hostId",service.getHostId());payload.put("role",service.getRole());payload.put("nodeId",service.getNodeId()==null?0:service.getNodeId());
                payload.put("serviceName",io.translab.tantor.server.service.RollingRestartPlan.unit(service));
                payload.putAll(restartDisplay.identity(service.getHostId()));
                step.setName(io.translab.tantor.server.service.RestartNodeDisplayService.stepName(payload));
                step.setPayload(objectMapper.writeValueAsString(payload));
                steps.add(step);
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new IllegalStateException("Unable to save restart plan", e); }
        Job saved = jobService.createJob(job, steps);
        return ResponseEntity.ok(Map.of("jobId", saved.getId().toString(), "status", saved.getStatus().name()));
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/normal-restart")
    public ResponseEntity<Map<String, String>> startNormalRestart(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ROLLING_RESTART)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        jobService.assertClusterMutable(clusterId);
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

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/enable-monitoring")
    public ResponseEntity<Map<String, String>> enableMonitoring(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody MonitoringRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.MONITORING_ENABLE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        jobService.assertClusterMutable(clusterId);
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

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/tasks/{taskId}/retry")
    public ResponseEntity<Void> retryTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        jobService.assertClusterMutable(clusterId);
        return deploymentService.retryTask(taskId) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/tasks/{taskId}/resume")
    public ResponseEntity<Void> resumeTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        jobService.assertClusterMutable(clusterId);
        return deploymentService.resumeTask(taskId) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/tasks/{taskId}/rollback")
    public ResponseEntity<Void> rollbackTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        jobService.assertClusterMutable(clusterId);
        return deploymentService.rollbackTask(clusterId, taskId) ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/tasks/{taskId}/cleanup")
    public ResponseEntity<Void> cleanupTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID taskId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.JOB_CONTROL)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        jobService.assertClusterMutable(clusterId);
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
        public String scope = "BROKERS";
        public java.util.List<String> targets = java.util.List.of();
        public boolean confirmCombined;
        public boolean confirmSingleNode;
    }
}
