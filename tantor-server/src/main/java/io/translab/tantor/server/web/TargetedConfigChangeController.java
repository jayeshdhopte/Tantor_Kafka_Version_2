package io.translab.tantor.server.web;

import io.translab.tantor.server.service.TargetedConfigChangeService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/config/change")
@RequiredArgsConstructor
public class TargetedConfigChangeController {
    private final TargetedConfigChangeService service;
    private final RoleAuthenticationUtil roles;
    private final io.translab.tantor.server.service.ConfigChangeHistoryService history;

    @GetMapping("/history")
    public ResponseEntity<?> history(@RequestHeader(value = "Authorization", required = false) String authorization,
                                      @PathVariable UUID clusterId, @RequestParam(defaultValue = "0") int page) {
        if (!roles.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_READ))
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        try { return ResponseEntity.ok(history.history(clusterId, page)); }
        catch (IllegalArgumentException e) { return ResponseEntity.badRequest().body(Map.of("message", e.getMessage())); }
    }

    public record FileValuesRequest(String role, List<Integer> targetNodeIds) {}

    @PostMapping("/file-values")
    public ResponseEntity<?> fileValues(@RequestHeader(value = "Authorization", required = false) String authorization,
                                        @PathVariable UUID clusterId,
                                        @RequestBody FileValuesRequest request) {
        if (!roles.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_READ)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        try {
            return ResponseEntity.ok(Map.of("values", service.readStaticProperties(clusterId,
                    request.role(), request.targetNodeIds())));
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage() == null
                    ? "Unable to read active Kafka configuration." : e.getMessage()));
        }
    }

    @PostMapping("/preview")
    public ResponseEntity<?> preview(@RequestHeader(value = "Authorization", required = false) String authorization,
                                     @PathVariable UUID clusterId,
                                     @RequestBody TargetedConfigChangeService.ChangeRequest request) {
        if (!roles.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_READ)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        try {
            return ResponseEntity.ok(service.preview(clusterId, request));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    @PostMapping("/apply")
    public ResponseEntity<?> apply(@RequestHeader(value = "Authorization", required = false) String authorization,
                                   @PathVariable UUID clusterId,
                                   @RequestBody TargetedConfigChangeService.ChangeRequest request) {
        if (!roles.canAccess(authorization, RoleAuthenticationUtil.CONFIG_VERSION_CHANGE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        try {
            return ResponseEntity.ok(service.apply(clusterId, request, roles.extractUsername(authorization)));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }
}
