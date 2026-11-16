package io.translab.tantor.server.web;

import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class ExternalControllerAdditionController {
  private final ExternalControllerPlanning planning;
  private final ExternalControllerLedger ledger;
  private final ExternalControllerTasks tasks;
  private final RoleAuthenticationUtil permissions;

  public record Accept(UUID previewId, String contextHash, String planDigest, String requestKey) {}

  public record Claim(boolean executionEnabled) {}

  private boolean allowed(String auth) {
    return permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE);
  }

  @PostMapping("/api/v1/ui/external-clusters/{cluster}/add-node/controller-previews")
  public ResponseEntity<?> create(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID cluster,
      @RequestBody ExternalControllerPlanning.Request request) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(planning.create(cluster, request));
  }

  @GetMapping("/api/v1/ui/external-clusters/{cluster}/add-node/controller-previews/{id}")
  public ResponseEntity<?> preview(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID cluster,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(planning.get(cluster, id));
  }

  @PostMapping("/api/v1/ui/external-clusters/{cluster}/add-node/controller-operations")
  public ResponseEntity<?> accept(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID cluster,
      @RequestBody Accept request) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(
        ledger.accept(
            cluster,
            request.previewId(),
            request.contextHash(),
            request.planDigest(),
            request.requestKey()));
  }

  @GetMapping("/api/v1/ui/external-clusters/{cluster}/add-node/controller-operations/request/{key}")
  public ResponseEntity<?> lookup(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID cluster,
      @PathVariable String key) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.lookup(cluster, key));
  }

  @GetMapping("/api/v1/ui/external-clusters/{cluster}/add-node/controller-operations/{id}")
  public ResponseEntity<?> get(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID cluster,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.view(cluster, id));
  }

  @PostMapping("/api/v1/ui/external-clusters/{cluster}/add-node/controller-operations/{id}/resume")
  public ResponseEntity<?> resume(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID cluster,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.resume(cluster, id));
  }

  @PostMapping(
      "/api/v1/ui/external-clusters/{cluster}/add-node/controller-operations/{id}/cancel-safe")
  public ResponseEntity<?> cancel(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID cluster,
      @PathVariable UUID id) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(ledger.cancel(cluster, id));
  }

  @PostMapping("/api/v1/ui/external-clusters/discovery/controller-tasks/{agent}/claim")
  public ResponseEntity<?> claim(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable String agent,
      @RequestBody(required = false) Claim claim) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    return ResponseEntity.ok(tasks.claim(agent, claim != null && claim.executionEnabled()));
  }

  @PostMapping("/api/v1/ui/external-clusters/discovery/controller-tasks/{agent}/{id}/result")
  public ResponseEntity<?> complete(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable String agent,
      @PathVariable UUID id,
      @RequestBody ExternalControllerTasks.Result result) {
    if (!allowed(auth)) return ResponseEntity.status(403).build();
    tasks.complete(agent, id, result);
    return ResponseEntity.ok(Map.of("status", "RECORDED"));
  }

  @ExceptionHandler(org.springframework.web.server.ResponseStatusException.class)
  public ResponseEntity<?> scope(org.springframework.web.server.ResponseStatusException failure) {
    return ResponseEntity.status(failure.getStatusCode())
        .body(
            Map.of(
                "message",
                Objects.toString(
                    failure.getReason(), "External controller scope is unavailable.")));
  }

  @ExceptionHandler(org.springframework.dao.EmptyResultDataAccessException.class)
  public ResponseEntity<?> absent() {
    return ResponseEntity.status(404)
        .body(
            Map.of(
                "message",
                "Controller observation/operation not found. Keep the original request key when"
                    + " confirming an uncertain submission."));
  }

  @ExceptionHandler(org.springframework.dao.DataAccessException.class)
  public ResponseEntity<?> conflict() {
    return ResponseEntity.status(409)
        .body(
            Map.of(
                "message",
                "Controller reservation or task state conflicts with the current request. Refresh"
                    + " the existing operation."));
  }

  @ExceptionHandler(IllegalStateException.class)
  public ResponseEntity<?> state(IllegalStateException failure) {
    return ResponseEntity.status(409)
        .body(
            Map.of(
                "message",
                Objects.toString(failure.getMessage(), "Controller operation unavailable.")));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<?> invalid(IllegalArgumentException failure) {
    return ResponseEntity.badRequest()
        .body(
            Map.of(
                "message", Objects.toString(failure.getMessage(), "Invalid controller request.")));
  }
}
