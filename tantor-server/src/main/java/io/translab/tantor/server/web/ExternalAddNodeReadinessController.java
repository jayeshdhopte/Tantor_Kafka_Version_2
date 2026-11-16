package io.translab.tantor.server.web;

import io.translab.tantor.server.service.ExternalAddNodeReadinessService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequiredArgsConstructor
public class ExternalAddNodeReadinessController {
  private final ExternalAddNodeReadinessService service;
  private final RoleAuthenticationUtil permissions;

  private boolean allowed(String auth) {
    return permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE);
  }

  @PostMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/readiness")
  public ResponseEntity<?> create(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @RequestBody ExternalAddNodeReadinessService.Request request) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(service.create(clusterId, request));
  }

  @GetMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/readiness/{id}")
  public ResponseEntity<?> get(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(service.get(clusterId, id));
  }

  @PostMapping("/api/v1/ui/external-clusters/discovery/add-node-tasks/{agentId}/claim")
  public ResponseEntity<?> claim(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable String agentId) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(service.claim(agentId));
  }

  @PostMapping("/api/v1/ui/external-clusters/discovery/add-node-tasks/{agentId}/{id}/result")
  public ResponseEntity<?> result(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable String agentId,
      @PathVariable UUID id,
      @RequestBody ExternalAddNodeReadinessService.Result result) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    service.complete(agentId, id, result);
    return ResponseEntity.ok(Map.of("status", "RECORDED", "deploymentEnabled", false));
  }

  @ExceptionHandler(ResponseStatusException.class)
  public ResponseEntity<?> error(ResponseStatusException e) {
    return ResponseEntity.status(e.getStatusCode())
        .body(Map.of("message", Objects.toString(e.getReason(), "Readiness unavailable.")));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<?> invalid(IllegalArgumentException e) {
    return ResponseEntity.badRequest()
        .body(Map.of("message", Objects.toString(e.getMessage(), "Invalid candidate profile.")));
  }
}
