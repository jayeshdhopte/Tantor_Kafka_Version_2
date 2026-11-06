package io.translab.tantor.server.web;

import io.translab.tantor.server.service.KafkaExpansionRuntimeInspection;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/clusters/{clusterId}/add-node/runtime-inspections")
public class KafkaExpansionRuntimeInspectionController {
    private final KafkaExpansionRuntimeInspection service;
    private final RoleAuthenticationUtil permissions;
    public record Request(UUID serviceId) {}
    @PostMapping public ResponseEntity<?> start(@PathVariable UUID clusterId,@RequestBody Request request,@RequestHeader(value="Authorization",required=false)String auth){if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();if(request.serviceId()==null)return ResponseEntity.badRequest().body(Map.of("message","A recorded participant service ID is required."));return ResponseEntity.accepted().body(service.start(clusterId,request.serviceId()));}
    @GetMapping("/{taskId}") public ResponseEntity<?> get(@PathVariable UUID clusterId,@PathVariable UUID taskId,@RequestHeader(value="Authorization",required=false)String auth){if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();return ResponseEntity.ok(service.get(clusterId,taskId));}
    @ExceptionHandler(ResponseStatusException.class) public ResponseEntity<?> error(ResponseStatusException error){return ResponseEntity.status(error.getStatusCode()).body(Map.of("message",Objects.toString(error.getReason(),"Approved runtime inspection is unavailable.")));}
}
