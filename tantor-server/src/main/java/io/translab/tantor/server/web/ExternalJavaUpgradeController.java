package io.translab.tantor.server.web;
import io.translab.tantor.server.service.ExternalJavaUpgradeService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;
import java.util.*;
@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/external-java")
public class ExternalJavaUpgradeController {
 private final ExternalJavaUpgradeService java;
 private final RoleAuthenticationUtil permissions;
 @GetMapping("/hosts") public ResponseEntity<?> hosts(@RequestParam(required=false) UUID clusterId,@RequestHeader(value="Authorization",required=false) String auth) {
  if(!permissions.canAccess(auth,RoleAuthenticationUtil.PARCEL_ACTION))return ResponseEntity.status(403).body(Map.of("error","Parcel permissions required."));
  return ResponseEntity.ok(java.hosts(clusterId));
 }
}
