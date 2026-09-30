package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.HostParcel;
import io.translab.tantor.server.service.ParcelService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/ui/parcels")
@RequiredArgsConstructor
public class ParcelController {
    private final ParcelService parcelService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    @GetMapping
    public List<HostParcel> listStates() {
        return parcelService.listStates();
    }

    @PostMapping("/{artifactId}/distribute")
    public ResponseEntity<Map<String, Object>> distribute(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID artifactId,
            @RequestBody ParcelService.ParcelActionRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.PARCEL_ACTION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        return scheduled(parcelService.distribute(artifactId, request));
    }

    @PostMapping("/{artifactId}/activate")
    public ResponseEntity<Map<String, Object>> activate(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID artifactId,
            @RequestBody ParcelService.ParcelActionRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.PARCEL_ACTION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        return scheduled(parcelService.activate(artifactId, request));
    }

    @PostMapping("/{artifactId}/deactivate")
    public ResponseEntity<Map<String, Object>> deactivate(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID artifactId,
            @RequestBody ParcelService.ParcelActionRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.PARCEL_ACTION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        return scheduled(parcelService.deactivate(artifactId, request));
    }

    @PostMapping("/{artifactId}/remove")
    public ResponseEntity<Map<String, Object>> remove(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID artifactId,
            @RequestBody ParcelService.ParcelActionRequest request
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.PARCEL_ACTION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        return scheduled(parcelService.remove(artifactId, request));
    }

    private ResponseEntity<Map<String, Object>> scheduled(List<HostParcel> parcels) {
        return ResponseEntity.ok(Map.of(
                "scheduled", parcels.size(),
                "states", parcels
        ));
    }
}
