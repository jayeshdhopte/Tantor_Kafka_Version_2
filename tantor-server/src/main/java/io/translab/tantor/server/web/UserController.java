package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.User;
import io.translab.tantor.server.dto.UserDto;
import io.translab.tantor.server.repository.UserRepository;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/auth/users")
@RequiredArgsConstructor
public class UserController {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final AuditService auditService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    private UserDto.UserResponse mapToDto(User user) {
        UserDto.UserResponse dto = new UserDto.UserResponse();
        dto.setId(user.getId());
        dto.setUsername(user.getUsername());
        dto.setRole(user.getRole());
        dto.set_active(user.isActive());
        dto.setCreated_at(user.getCreatedAt());
        dto.setLast_login(user.getUpdatedAt());
        dto.setAuth_source(user.getAuthSource());
        dto.setLdap_dn(user.getLdapDn());
        return dto;
    }

    @GetMapping
    public ResponseEntity<List<UserDto.UserResponse>> listUsers(
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.USER_MANAGEMENT)) {
            return ResponseEntity.status(401).build();
        }
        
        List<UserDto.UserResponse> users = userRepository.findAll().stream()
                .map(this::mapToDto)
                .collect(Collectors.toList());
        return ResponseEntity.ok(users);
    }

    @PostMapping
    public ResponseEntity<?> createUser(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody UserDto.UserCreateRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.USER_MANAGEMENT)) {
            return ResponseEntity.status(401).build();
        }

        if (userRepository.findByUsername(request.getUsername()).isPresent()) {
            return ResponseEntity.status(409).body("{\"detail\":\"Username already exists\"}");
        }

        if (!"admin".equals(request.getRole()) && !"monitor".equals(request.getRole())) {
            return ResponseEntity.status(400).body("{\"detail\":\"Role must be 'admin' or 'monitor'\"}");
        }

        User user = new User();
        user.setUsername(request.getUsername());
        user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        user.setRole(request.getRole());
        user.setAuthSource("local");
        user.setActive(true);
        
        user = userRepository.save(user);
        auditService.record("PERMISSION", "USER_CREATED", "USER", user.getId().toString(), null, "SUCCESS",
                null, Map.of("username", user.getUsername(), "role", String.valueOf(user.getRole()),
                        "active", user.isActive(), "authSource", String.valueOf(user.getAuthSource())),
                null, Map.of("passwordCaptured", false));
        return ResponseEntity.status(201).body(mapToDto(user));
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateUser(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID id,
            @RequestBody UserDto.UserUpdateRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.USER_MANAGEMENT)) {
            return ResponseEntity.status(401).build();
        }

        User user = userRepository.findById(id).orElse(null);
        if (user == null) {
            return ResponseEntity.status(404).body("{\"detail\":\"User not found\"}");
        }

        String currentUsername = currentUsername();
        boolean isSelf = currentUsername.equals(user.getUsername());
        Map<String, Object> oldValue = Map.of("role", String.valueOf(user.getRole()),
                "active", user.isActive(), "authSource", String.valueOf(user.getAuthSource()));
        boolean roleRequested = request.getRole() != null;

        if (request.getRole() != null) {
            if (!"admin".equals(request.getRole()) && !"monitor".equals(request.getRole())) {
                return ResponseEntity.status(400).body("{\"detail\":\"Role must be 'admin' or 'monitor'\"}");
            }
            if (isSelf && !"admin".equals(request.getRole())) {
                return ResponseEntity.status(400).body("{\"detail\":\"Cannot remove admin role from yourself\"}");
            }
            user.setRole(request.getRole());
        }

        if (request.getPassword() != null) {
            if ("ldap".equals(user.getAuthSource())) {
                return ResponseEntity.status(400).body("{\"detail\":\"Cannot set a local password on an LDAP-synced user.\"}");
            }
            user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        }

        if (request.getIs_active() != null) {
            if (isSelf && !request.getIs_active()) {
                return ResponseEntity.status(400).body("{\"detail\":\"Cannot deactivate yourself\"}");
            }
            user.setActive(request.getIs_active());
        }

        user = userRepository.save(user);
        auditService.record("PERMISSION", roleRequested ? "USER_ROLE_CHANGED" : "USER_ACCESS_UPDATED",
                "USER", user.getId().toString(), null, "SUCCESS", oldValue,
                Map.of("role", String.valueOf(user.getRole()), "active", user.isActive(),
                        "authSource", String.valueOf(user.getAuthSource())), null,
                Map.of("username", user.getUsername(), "passwordChanged", request.getPassword() != null));
        return ResponseEntity.ok(mapToDto(user));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteUser(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.USER_MANAGEMENT)) {
            return ResponseEntity.status(401).build();
        }

        User user = userRepository.findById(id).orElse(null);
        if (user == null) {
            return ResponseEntity.status(404).body("{\"detail\":\"User not found\"}");
        }

        if (currentUsername().equals(user.getUsername())) {
            return ResponseEntity.status(400).body("{\"detail\":\"Cannot delete yourself\"}");
        }

        Map<String, Object> oldValue = Map.of("username", user.getUsername(), "role", String.valueOf(user.getRole()),
                "active", user.isActive(), "authSource", String.valueOf(user.getAuthSource()));
        userRepository.delete(user);
        auditService.record("PERMISSION", "USER_REMOVED", "USER", user.getId().toString(), null, "SUCCESS",
                oldValue, Map.of("deleted", true), null, null);
        return ResponseEntity.ok("{\"deleted\": true, \"username\": \"" + user.getUsername() + "\"}");
    }

    private String currentUsername() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth == null ? "" : auth.getName();
    }
}
