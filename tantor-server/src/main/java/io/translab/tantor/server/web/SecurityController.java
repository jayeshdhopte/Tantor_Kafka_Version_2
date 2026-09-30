package io.translab.tantor.server.web;

import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.dto.AclDTOs.*;
import io.translab.tantor.server.service.SecurityOperationsService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
public class SecurityController {

    private final SecurityOperationsService securityOperationsService;
    private final AuditService auditService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    @GetMapping("/clusters/{clusterId}/security/acls")
    public ResponseEntity<AclListResponse> listAcls(
            @PathVariable UUID clusterId,
            @RequestParam(required = false) String principal,
            @RequestParam(required = false) String resource_type,
            @RequestParam(required = false) String resource_name) {
        
        return ResponseEntity.ok(securityOperationsService.listAcls(clusterId, principal, resource_type, resource_name));
    }

    @PostMapping("/clusters/{clusterId}/security/acls")
    public ResponseEntity<AclCreateResponse> createAcl(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody AclCreateRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SECURITY_CHANGE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        
        AclCreateResponse response = securityOperationsService.createAcl(clusterId, request);
        
        auditService.record("SECURITY", "ACL_CREATED", "CLUSTER", clusterId.toString(), clusterId, "SUCCESS",
                null, null, null, "Created ACL for principal " + request.getPrincipal() + " on " + request.getResource_type() + " " + request.getResource_name());
                
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/clusters/{clusterId}/security/acls")
    public ResponseEntity<AclDeleteResponse> deleteAcl(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody AclDeleteRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SECURITY_CHANGE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        
        AclDeleteResponse response = securityOperationsService.deleteAcl(clusterId, request);
        
        auditService.record("SECURITY", "ACL_DELETED", "CLUSTER", clusterId.toString(), clusterId, "SUCCESS",
                null, null, null, "Deleted ACLs for principal " + request.getPrincipal() + " on " + request.getResource_type() + " " + request.getResource_name());
                
        return ResponseEntity.ok(response);
    }
}
