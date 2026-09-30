package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.LdapConfig;
import io.translab.tantor.server.dto.LdapDTOs;
import io.translab.tantor.server.repository.LdapConfigRepository;
import io.translab.tantor.server.service.LdapService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/ldap")
public class LdapController {

    private final LdapConfigRepository ldapConfigRepository;
    private final LdapService ldapService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    public LdapController(LdapConfigRepository ldapConfigRepository, LdapService ldapService,
                          RoleAuthenticationUtil roleAuthenticationUtil) {
        this.ldapConfigRepository = ldapConfigRepository;
        this.ldapService = ldapService;
        this.roleAuthenticationUtil = roleAuthenticationUtil;
    }

    @GetMapping("/config")
    public ResponseEntity<LdapDTOs.LdapConfigResponse> getConfig(
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!isAdmin(authorization)) return ResponseEntity.status(401).build();
        return ldapConfigRepository.findAll().stream().findFirst()
                .map(config -> ResponseEntity.ok(toResponse(config)))
                .orElse(ResponseEntity.ok(null));
    }

    @PutMapping("/config")
    public ResponseEntity<LdapDTOs.LdapConfigResponse> updateConfig(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody LdapDTOs.LdapConfigCreateRequest request) {
        if (!isAdmin(authorization)) return ResponseEntity.status(401).build();
        LdapConfig config = ldapConfigRepository.findAll().stream().findFirst().orElse(new LdapConfig());
        
        config.setEnabled(request.isEnabled());
        config.setServerUrl(request.getServerUrl());
        config.setUseSsl(request.isUseSsl());
        config.setTlsValidateCert(request.isTlsValidateCert());
        config.setTlsCaCert(request.getTlsCaCert());
        config.setBindDn(request.getBindDn());
        
        if (request.getBindPassword() != null && !request.getBindPassword().isEmpty()) {
            config.setEncryptedBindPassword(ldapService.encryptPassword(request.getBindPassword()));
        }
        
        config.setUserSearchBase(request.getUserSearchBase());
        config.setUserSearchFilter(request.getUserSearchFilter());
        config.setGroupSearchBase(request.getGroupSearchBase());
        config.setAdminGroupDn(request.getAdminGroupDn());
        config.setMonitorGroupDn(request.getMonitorGroupDn());
        config.setDefaultRole(request.getDefaultRole() != null ? request.getDefaultRole() : "monitor");
        config.setConnectionTimeout(request.getConnectionTimeout());

        config = ldapConfigRepository.save(config);
        return ResponseEntity.ok(toResponse(config));
    }

    @PostMapping("/test")
    public ResponseEntity<LdapDTOs.LdapTestResponse> testConnection(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody LdapDTOs.LdapTestRequest request) {
        if (!isAdmin(authorization)) return ResponseEntity.status(401).build();
        LdapConfig config = ldapConfigRepository.findAll().stream().findFirst().orElse(null);
        if (config == null) {
            return ResponseEntity.badRequest().body(new LdapDTOs.LdapTestResponse(false, "LDAP not configured", null, null));
        }

        String bindPassword = request.getBindPassword();
        if (bindPassword == null || bindPassword.isEmpty()) {
            bindPassword = ldapService.decryptPassword(config.getEncryptedBindPassword());
        }

        if (request.getUsername() != null && !request.getUsername().isEmpty()) {
            // Test auth
            return ResponseEntity.ok(ldapService.authenticate(request.getUsername(), request.getPassword(), config, bindPassword));
        } else {
            // Test connection only
            return ResponseEntity.ok(ldapService.testConnection(config, bindPassword));
        }
    }

    private LdapDTOs.LdapConfigResponse toResponse(LdapConfig config) {
        LdapDTOs.LdapConfigResponse response = new LdapDTOs.LdapConfigResponse();
        if (config.getId() != null) {
            response.setId(config.getId().toString());
        }
        response.setEnabled(config.isEnabled());
        response.setServerUrl(config.getServerUrl());
        response.setUseSsl(config.isUseSsl());
        response.setTlsValidateCert(config.isTlsValidateCert());
        response.setTlsCaCertPresent(config.getTlsCaCert() != null && !config.getTlsCaCert().isEmpty());
        response.setBindDn(config.getBindDn());
        response.setUserSearchBase(config.getUserSearchBase());
        response.setUserSearchFilter(config.getUserSearchFilter());
        response.setGroupSearchBase(config.getGroupSearchBase());
        response.setAdminGroupDn(config.getAdminGroupDn());
        response.setMonitorGroupDn(config.getMonitorGroupDn());
        response.setDefaultRole(config.getDefaultRole());
        response.setConnectionTimeout(config.getConnectionTimeout());
        return response;
    }

    private boolean isAdmin(String authorization) {
        return roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.USER_MANAGEMENT);
    }
}
