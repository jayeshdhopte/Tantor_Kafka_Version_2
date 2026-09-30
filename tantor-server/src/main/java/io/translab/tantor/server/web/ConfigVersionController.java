package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.ConfigVersion;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.service.ConfigVersionService;
import io.translab.tantor.server.service.ConfigurationSanitizer;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/config")
@RequiredArgsConstructor
public class ConfigVersionController {

    private final ClusterRepository clusterRepository;
    private final ConfigVersionService configVersionService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;
    private final ConfigurationSanitizer configurationSanitizer;
    private final io.translab.tantor.server.repository.ExternalClusterNodeRepository externalClusterNodeRepository;

    @PostMapping("/services/{serviceId}/versions/preview")
    public ResponseEntity<?> preview(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID serviceId,
            @RequestBody VersionedConfigRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIG_VERSION_CHANGE)) {
            return unauthorized();
        }
        Cluster cluster = clusterRepository.findById(clusterId).orElse(null);
        if (cluster == null) return ResponseEntity.notFound().build();
        if (findService(cluster, serviceId) == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "Service assignment does not belong to this cluster."));
        }
        return ResponseEntity.ok(configVersionService.preview(
                safeProperties(request.getCurrentProperties()), safeProperties(request.getProperties()), request.isRestart()));
    }

    @PostMapping("/services/{serviceId}/versions")
    public ResponseEntity<?> create(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID serviceId,
            @RequestBody VersionedConfigRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIG_VERSION_CHANGE)) {
            return unauthorized();
        }
        try {
            Cluster cluster = clusterRepository.findById(clusterId).orElse(null);
            if (cluster == null) return ResponseEntity.notFound().build();
            ClusterServiceAssignment service = findService(cluster, serviceId);
            if (service == null) {
                return ResponseEntity.badRequest().body(Map.of("message", "Service assignment does not belong to this cluster."));
            }
            ConfigVersion version = configVersionService.createVersion(
                    cluster, service, request.getConfigFileName(),
                    safeProperties(request.getCurrentProperties()), safeProperties(request.getProperties()),
                    request.isApprovalRequired(), request.isRestart());
            return ResponseEntity.ok(version);
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    @GetMapping("/versions")
    public ResponseEntity<List<ConfigVersion>> history(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestParam(required = false) UUID serviceId
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_READ)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        if (!clusterRepository.existsById(clusterId)) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(configurationSanitizer.sanitizeVersions(configVersionService.history(clusterId, serviceId)));
    }

    @PostMapping("/versions/{versionId}/approve")
    public ResponseEntity<?> approve(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID versionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIG_VERSION_CHANGE)) {
            return unauthorized();
        }
        try {
            return ResponseEntity.ok(configVersionService.approve(clusterId, versionId));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    @PostMapping("/versions/{versionId}/apply")
    public ResponseEntity<?> apply(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID versionId,
            @RequestBody(required = false) ApplyVersionRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIG_VERSION_CHANGE)) {
            return unauthorized();
        }
        try {
            Job job = configVersionService.apply(clusterId, versionId, request == null || request.isRestart());
            return ResponseEntity.ok(Map.of("jobId", job.getId().toString(), "status", job.getStatus().name()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    @PostMapping("/versions/{versionId}/rollback")
    public ResponseEntity<?> rollback(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID versionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIG_VERSION_CHANGE)) {
            return unauthorized();
        }
        try {
            return ResponseEntity.ok(configVersionService.createRollback(clusterId, versionId));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    private ResponseEntity<?> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
    }

    private ClusterServiceAssignment findService(Cluster cluster, UUID serviceId) {
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            return externalClusterNodeRepository.findById(serviceId).map(node -> {
                ClusterServiceAssignment csa = new ClusterServiceAssignment();
                csa.setId(node.getId());
                csa.setCluster(cluster);
                csa.setHostId(node.getHost());
                csa.setNodeId(node.getNodeId());
                csa.setRole(Boolean.TRUE.equals(node.getIsBroker()) ? "Broker" : "Controller");
                return csa;
            }).orElse(null);
        }
        if (cluster.getServices() == null) return null;
        return cluster.getServices().stream().filter(service -> serviceId.equals(service.getId())).findFirst().orElse(null);
    }

    private Map<String, Object> safeProperties(Map<String, Object> properties) {
        return properties == null ? Map.of() : properties;
    }

    @Data
    public static class VersionedConfigRequest {
        private Map<String, Object> currentProperties;
        private Map<String, Object> properties;
        private String configFileName;
        private boolean approvalRequired;
        private boolean restart = true;
    }

    @Data
    public static class ApplyVersionRequest {
        private boolean restart = true;
    }
}
