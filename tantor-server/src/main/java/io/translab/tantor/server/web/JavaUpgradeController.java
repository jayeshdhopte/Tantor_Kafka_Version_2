package io.translab.tantor.server.web;

import io.translab.tantor.server.service.JavaUpgradeJobHandler;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/ui/clusters")
@RequiredArgsConstructor
public class JavaUpgradeController {
    private final JavaUpgradeJobHandler upgrade;
    private final RoleAuthenticationUtil permissions;

    public record Request(UUID artifactId, String version, List<String> hostIds) {}

    @PostMapping("/{clusterId}/java-upgrade")
    public ResponseEntity<Map<String, String>> schedule(
            @PathVariable UUID clusterId, @RequestBody Request request,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!permissions.canAccess(authorization, RoleAuthenticationUtil.ROLLING_RESTART)
                || !permissions.canAccess(authorization, RoleAuthenticationUtil.PARCEL_ACTION)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", "Java upgrade requires parcel and restart permissions."));
        }
        try {
            UUID jobId = upgrade.schedule(clusterId, request.artifactId(), request.hostIds(), request.version());
            return ResponseEntity.accepted().body(Map.of("jobId", jobId.toString()));
        } catch (IllegalArgumentException | IllegalStateException ex) {
            return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
        }
    }
}
