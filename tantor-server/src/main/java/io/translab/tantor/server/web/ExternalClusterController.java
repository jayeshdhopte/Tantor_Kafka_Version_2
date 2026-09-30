package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.service.ExternalClusterService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class ExternalClusterController {

    private final ExternalClusterService externalClusterService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    @GetMapping("/api/v1/ui/external-clusters")
    public ResponseEntity<List<Map<String, Object>>> listExternalClusters() {
        return ResponseEntity.ok(externalClusterService.listExternalClusters());
    }

    @GetMapping("/api/v1/ui/external-clusters/discoveries")
    public ResponseEntity<List<Map<String, Object>>> listPendingDiscoveries() {
        return ResponseEntity.ok(externalClusterService.listPendingDiscoveries());
    }

    @GetMapping("/api/v1/ui/external-clusters/agents")
    public ResponseEntity<List<Map<String, Object>>> listDiscoveryAgents() {
        return ResponseEntity.ok(externalClusterService.listDiscoveryAgents());
    }

    @GetMapping("/api/v1/ui/external-clusters/name-availability")
    public ResponseEntity<Map<String, Object>> clusterNameAvailability(@RequestParam String name) {
        boolean available = externalClusterService.isClusterNameAvailable(name);
        return ResponseEntity.ok(Map.of(
                "available", available,
                "message", available ? "" : "A cluster with this name already exists. Choose a different name."
        ));
    }

    @GetMapping("/api/v1/ui/external-clusters/discoveries/{discoveryKey}/inspect")
    public ResponseEntity<Map<String, Object>> inspectDiscovery(@PathVariable String discoveryKey) {
        return ResponseEntity.ok(externalClusterService.inspectDiscovery(discoveryKey));
    }

    @PostMapping("/api/v1/ui/external-clusters/bootstrap/test")
    public ResponseEntity<Map<String, Object>> testBootstrap(@RequestBody ExternalClusterService.BootstrapExternalClusterRequest request) {
        return ResponseEntity.ok(externalClusterService.testBootstrap(request));
    }

    @PostMapping("/api/v1/ui/external-clusters/bootstrap/register")
    public ResponseEntity<Map<String, Object>> registerBootstrap(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody ExternalClusterService.BootstrapExternalClusterRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CREATE_CLUSTER)) {
            return unauthorized();
        }
        ExternalCluster cluster = externalClusterService.registerBootstrapCluster(request);
        return ResponseEntity.ok(Map.of("id", cluster.getId(), "name", cluster.getName()));
    }

    @PostMapping("/api/v1/ui/external-clusters/discovery/report")
    public ResponseEntity<Map<String, Object>> reportDiscovery(
            @RequestBody ExternalClusterService.ExternalDiscoveryReport request,
            jakarta.servlet.http.HttpServletRequest httpRequest) {
        String remoteIp = httpRequest.getRemoteAddr();
        request.setIpAddresses("[\"" + remoteIp + "\"]");
        return ResponseEntity.ok(externalClusterService.recordDiscoveryReport(request));
    }

    @PostMapping("/api/v1/ui/external-clusters/discovery/heartbeat")
    public ResponseEntity<Map<String, Object>> heartbeatDiscoveryAgent(
            @RequestBody ExternalClusterService.ExternalDiscoveryReport request,
            jakarta.servlet.http.HttpServletRequest httpRequest) {
        String remoteIp = httpRequest.getRemoteAddr();
        if (request.getIpAddresses() == null || request.getIpAddresses().isBlank()) {
            request.setIpAddresses("[\"" + remoteIp + "\"]");
        }
        return ResponseEntity.ok(externalClusterService.recordDiscoveryAgentHeartbeat(request));
    }

    @PostMapping("/api/v1/ui/external-clusters/discoveries/{discoveryKey}/connect")
    public ResponseEntity<Map<String, Object>> connectDiscovery(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String discoveryKey) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CREATE_CLUSTER)) {
            return unauthorized();
        }
        ExternalCluster cluster = externalClusterService.connectDiscovery(discoveryKey);
        return ResponseEntity.ok(Map.of("id", cluster.getId(), "name", cluster.getName(), "status", "connected"));
    }

    @PostMapping("/api/v1/ui/external-clusters/{clusterId}/restart")
    public ResponseEntity<Map<String, Object>> restartExternalCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ROLLING_RESTART)) {
            return unauthorized();
        }
        return ResponseEntity.ok(externalClusterService.queueRestart(clusterId));
    }

    private ResponseEntity<Map<String, Object>> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.<String, Object>of("error", "Unauthorized"));
    }

    @GetMapping("/api/v1/ui/external-clusters/tasks/{taskId}")
    public ResponseEntity<Map<String, Object>> getExternalTaskStatus(@PathVariable String taskId) {
        return ResponseEntity.ok(externalClusterService.getExternalTaskStatus(taskId));
    }

    @GetMapping("/api/v1/ui/external-clusters/discovery/{clusterName}/tasks")
    public ResponseEntity<Map<String, Object>> pollDiscoveryTask(
            @PathVariable String clusterName,
            @RequestParam String hostname,
            @RequestParam String bootstrap) {
        return ResponseEntity.ok(externalClusterService.pollAgentTask(clusterName, hostname, bootstrap));
    }

    @PostMapping("/api/v1/ui/external-clusters/discovery/{clusterName}/tasks/complete")
    public ResponseEntity<Void> completeDiscoveryTask(
            @PathVariable String clusterName,
            @RequestParam String hostname,
            @RequestParam String bootstrap,
            @RequestBody(required = false) ExternalClusterService.AgentTaskCompletion completion) {
        externalClusterService.completeAgentTask(
                clusterName,
                hostname,
                bootstrap,
                completion == null ? new ExternalClusterService.AgentTaskCompletion() : completion
        );
        return ResponseEntity.ok().build();
    }

    @PostMapping("/api/v1/ui/external-clusters/discovery/{clusterName}/metrics")
    public ResponseEntity<Void> receiveDiscoveryMetrics(
            @PathVariable String clusterName,
            @RequestBody ExternalClusterService.ExternalBrokerMetricsDto metrics) {
        externalClusterService.receiveMetrics(clusterName, metrics);
        return ResponseEntity.ok().build();
    }

    // Compatibility endpoints for the older discovery-agent build.
    @GetMapping("/api/v1/ui/clusters/external/{clusterName}/tasks")
    public ResponseEntity<Map<String, Object>> pollLegacyDiscoveryTask(
            @PathVariable String clusterName,
            @RequestParam String hostname,
            @RequestParam String bootstrap) {
        return pollDiscoveryTask(clusterName, hostname, bootstrap);
    }

    @PostMapping("/api/v1/ui/clusters/external/{clusterName}/tasks/complete")
    public ResponseEntity<Void> completeLegacyDiscoveryTask(
            @PathVariable String clusterName,
            @RequestParam String hostname,
            @RequestParam String bootstrap,
            @RequestBody(required = false) ExternalClusterService.AgentTaskCompletion completion) {
        return completeDiscoveryTask(clusterName, hostname, bootstrap, completion);
    }

    @PostMapping("/api/v1/ui/clusters/external/{clusterName}/tasks/metrics")
    public ResponseEntity<Void> receiveLegacyDiscoveryMetrics(
            @PathVariable String clusterName,
            @RequestBody ExternalClusterService.ExternalBrokerMetricsDto metrics) {
        return receiveDiscoveryMetrics(clusterName, metrics);
    }
}
