package io.translab.tantor.server.web;

import io.translab.tantor.server.dto.AddNodeReadiness.*;
import io.translab.tantor.server.service.AddNodeReadinessService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
import java.util.Map;
import org.springframework.web.server.ResponseStatusException;

@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/clusters/{clusterId}/add-node/previews")
public class AddNodeReadinessController {
    private final AddNodeReadinessService service;
    private final RoleAuthenticationUtil permissions;
    private final io.translab.tantor.server.service.BrokerAdditionPlanningService planning;
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<?> readinessError(ResponseStatusException error) {
        return ResponseEntity.status(error.getStatusCode()).body(Map.of("message", error.getReason() == null ? "Readiness request could not be completed." : error.getReason()));
    }
    @GetMapping("/deployment-settings")
    public ResponseEntity<?> settings(@RequestHeader(value="Authorization",required=false) String auth,@PathVariable UUID clusterId){
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();
        return ResponseEntity.ok(service.deploymentSettings(clusterId));
    }
    @PostMapping
    public ResponseEntity<?> start(@RequestHeader(value="Authorization", required=false) String auth,
                                  @PathVariable UUID clusterId, @RequestBody Request request) {
        if (!permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE)) return ResponseEntity.status(403).build();
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(service.start(clusterId, request.hostId(),request.brokerRack(),request.deploymentSettings()));
    }
    @GetMapping("/{previewId}")
    public ResponseEntity<?> get(@RequestHeader(value="Authorization", required=false) String auth,
                                @PathVariable UUID clusterId, @PathVariable UUID previewId) {
        if (!permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE)) return ResponseEntity.status(403).build();
        return ResponseEntity.ok(service.get(clusterId, previewId));
    }
    @GetMapping("/{previewId}/broker-plan")
    public ResponseEntity<?> plan(@RequestHeader(value="Authorization", required=false) String auth,
                                 @PathVariable UUID clusterId, @PathVariable UUID previewId) {
        if (!permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE)) return ResponseEntity.status(403).build();
        return ResponseEntity.ok(planning.review(clusterId, previewId));
    }
}
