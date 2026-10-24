package io.translab.tantor.server.web;

import io.translab.tantor.server.service.ExternalConnectionSettingsService;
import io.translab.tantor.server.dto.ExternalConnectionSettings;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import java.util.UUID;

@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/external-clusters/{id}/connection-settings")
public class ExternalConnectionSettingsController {
    private final ExternalConnectionSettingsService service;
    private final RoleAuthenticationUtil permissions;
    @GetMapping public ResponseEntity<?> get(@RequestHeader(value="Authorization",required=false) String auth,@PathVariable UUID id){
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.CONFIGURATION_READ))return ResponseEntity.status(403).build();
        return ResponseEntity.ok(service.get(id));
    }
    @PutMapping public ResponseEntity<?> save(@RequestHeader(value="Authorization",required=false) String auth,@PathVariable UUID id,@RequestBody ExternalConnectionSettings.Request request){
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.CONFIGURATION_CHANGE))return ResponseEntity.status(403).build();
        return ResponseEntity.ok(service.save(id,request));
    }
}
