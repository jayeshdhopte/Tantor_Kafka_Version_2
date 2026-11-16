package io.translab.tantor.server.web;

import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequiredArgsConstructor
public class ExternalBrokerAdditionController {
  private final ExternalBrokerAdditionPlanning planning;
  private final ExternalBrokerAdditionLedger ledger;
  private final RoleAuthenticationUtil permissions;

  private final ExternalBrokerRecoveryService recovery;

  public record Accept(UUID previewId, String contextHash, String planDigest, String requestKey) {}

  private boolean allowed(String auth) {
    return permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE);
  }

  @GetMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/broker-review/{previewId}")
  public ResponseEntity<?> review(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @PathVariable UUID previewId) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(planning.review(clusterId, previewId));
  }

  @PostMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/broker-operations")
  public ResponseEntity<?> accept(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @RequestBody Accept body) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    if (body.previewId() == null)
      throw new IllegalArgumentException("Reviewed preview ID is required.");
    return ResponseEntity.ok(
        ledger.accept(
            clusterId, body.previewId(), body.contextHash(), body.planDigest(), body.requestKey()));
  }

  @GetMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/broker-operations/request/{key}")
  public ResponseEntity<?> lookup(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @PathVariable String key) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.lookup(clusterId, key));
  }

  @GetMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/broker-operations/{id}")
  public ResponseEntity<?> get(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.view(clusterId, id));
  }

  @PostMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/broker-operations/{id}/resume")
  public ResponseEntity<?> resume(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(recovery.resume(clusterId, id));
  }

  @PostMapping(
      "/api/v1/ui/external-clusters/{clusterId}/add-node/broker-operations/{id}/cancel-safe")
  public ResponseEntity<?> cancel(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.cancelSafe(clusterId, id));
  }

  @PostMapping("/api/v1/ui/external-clusters/discovery/add-broker-tasks/{agentId}/claim")
  public ResponseEntity<?> claim(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable String agentId) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.claim(agentId));
  }

  @PostMapping("/api/v1/ui/external-clusters/discovery/add-broker-tasks/{agentId}/{id}/result")
  public ResponseEntity<?> complete(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable String agentId,
      @PathVariable UUID id,
      @RequestBody ExternalBrokerAdditionLedger.Result result) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    ledger.complete(agentId, id, result);
    return ResponseEntity.ok(Map.of("status", "RECORDED"));
  }

  @ExceptionHandler(ResponseStatusException.class)
  public ResponseEntity<?> error(ResponseStatusException e) {
    return ResponseEntity.status(e.getStatusCode())
        .body(
            Map.of(
                "message",
                Objects.toString(e.getReason(), "External broker operation unavailable.")));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<?> invalid(IllegalArgumentException e) {
    return ResponseEntity.badRequest()
        .body(Map.of("message", Objects.toString(e.getMessage(), "Invalid broker request.")));
  }
}
