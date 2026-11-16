package io.translab.tantor.server.web;

import io.translab.tantor.server.service.ExternalAddNodeInspectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequiredArgsConstructor
public class ExternalAddNodeInspectionController {
  private final ExternalAddNodeInspectionService service;
  private final RoleAuthenticationUtil permissions;

  @PostMapping("/api/v1/ui/external-clusters/discovery/add-node-inspections")
  public ResponseEntity<?> report(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @RequestBody ExternalAddNodeInspectionService.Observation report) {
    if (!permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE))
      return ResponseEntity.status(403).build();
    service.record(report);
    return ResponseEntity.ok(Map.of("status", "RECORDED", "executionEnabled", false));
  }

  @GetMapping("/api/v1/ui/external-clusters/{clusterId}/add-node/candidates")
  public ResponseEntity<?> candidates(
      @RequestHeader(value = "Authorization", required = false) String auth,
      @PathVariable UUID clusterId) {
    if (!permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE))
      return ResponseEntity.status(403).build();
    return ResponseEntity.ok(service.candidates(clusterId));
  }

  @ExceptionHandler(ResponseStatusException.class)
  public ResponseEntity<?> error(ResponseStatusException e) {
    return ResponseEntity.status(e.getStatusCode())
        .body(
            Map.of(
                "message",
                Objects.toString(
                    e.getReason(), "External host inspection could not be completed.")));
  }
}
