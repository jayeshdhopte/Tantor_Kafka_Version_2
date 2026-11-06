package io.translab.tantor.server.web;

import io.translab.tantor.server.service.AddNodeProcessInspectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/clusters/{clusterId}/add-node/process-inspections")
public class AddNodeProcessInspectionController {
    private final AddNodeProcessInspectionService inspections;
    private final RoleAuthenticationUtil permissions;
    public record Selection(UUID serviceId) {}
    @PostMapping public ResponseEntity<?> start(@PathVariable UUID clusterId, @RequestBody Selection selection, @RequestHeader(value="Authorization", required=false) String auth) {
        if (!permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE)) return ResponseEntity.status(403).build();
        if (selection == null || selection.serviceId() == null) return ResponseEntity.badRequest().body(Map.of("message", "Select a service assignment."));
        return ResponseEntity.accepted().body(inspections.start(clusterId, selection.serviceId()));
    }
    @GetMapping("/{taskId}") public ResponseEntity<?> get(@PathVariable UUID clusterId, @PathVariable UUID taskId, @RequestHeader(value="Authorization", required=false) String auth) {
        if (!permissions.canAccess(auth, RoleAuthenticationUtil.ADD_NODE)) return ResponseEntity.status(403).build();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(inspections.get(clusterId, taskId));
    }
    @ExceptionHandler(ResponseStatusException.class) public ResponseEntity<?> error(ResponseStatusException e) { return ResponseEntity.status(e.getStatusCode()).body(Map.of("message", Objects.toString(e.getReason(), "Process inspection unavailable."))); }
}
