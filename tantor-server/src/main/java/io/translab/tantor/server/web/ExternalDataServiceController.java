package io.translab.tantor.server.web;
import io.translab.tantor.server.service.ExternalDataServiceManager;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;
import java.util.*;

@RestController @RequiredArgsConstructor
public class ExternalDataServiceController {
 private final ExternalDataServiceManager manager;
 private final RoleAuthenticationUtil roles;
 @GetMapping("/api/v1/ui/external-clusters/{id}/services")
 public Map<String,Object> options(@PathVariable UUID id) { return manager.options(id); }
 @PostMapping("/api/v1/ui/external-clusters/{id}/services")
 public ResponseEntity<Map<String,String>> create(@RequestHeader(value="Authorization",required=false) String auth,@PathVariable UUID id,@RequestBody Map<String,Map<String,Object>> request) {
  if(!roles.canAccess(auth,RoleAuthenticationUtil.ADD_NODE)) return ResponseEntity.status(403).body(Map.of("error","Not authorized to deploy services."));
  return ResponseEntity.ok(manager.create(id,request,roles.extractUsername(auth)));
 }
 @PostMapping("/api/v1/ui/external-clusters/discovery/service-tasks/claim")
 public Map<String,Object> claim(@RequestParam String agentId) { return manager.claim(agentId); }
 @PostMapping("/api/v1/ui/external-clusters/discovery/service-tasks/{id}/complete")
 public void complete(@RequestParam String agentId,@PathVariable UUID id,@RequestBody Map<String,String> result) { manager.complete(agentId,id,result); }
 @ExceptionHandler(IllegalArgumentException.class)
 public ResponseEntity<Map<String,String>> invalid(IllegalArgumentException e) { return ResponseEntity.badRequest().body(Map.of("error",Objects.toString(e.getMessage(),"Invalid request"))); }
}
