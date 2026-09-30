package io.translab.tantor.artifact.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class RoleAuthenticationUtil {

    public static final String ARTIFACT_UPLOAD = "ARTIFACT_UPLOAD";
    public static final String ARTIFACT_DELETE = "ARTIFACT_DELETE";
    public static final String BUNDLE_IMPORT = "BUNDLE_IMPORT";

    private final ObjectMapper objectMapper;
    private final Map<String, List<String>> permissions;

    public RoleAuthenticationUtil(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.permissions = loadPermissions();
    }

    public boolean canAccess(String authorizationHeader, String action) {
        List<String> allowedRoles = permissions.getOrDefault(action, List.of());
        if (allowedRoles.isEmpty()) {
            return false;
        }
        Set<String> userRoles = roles(authorizationHeader);
        return allowedRoles.stream()
                .map(this::normalizeRole)
                .anyMatch(userRoles::contains);
    }

    public String username(String authorizationHeader) {
        Map<String, Object> claims = claims(authorizationHeader);
        for (String key : List.of("preferred_username", "name", "email", "sub")) {
            Object value = claims.get(key);
            if (value instanceof String text && !text.isBlank()) {
                return text;
            }
        }
        return "system";
    }

    private Map<String, List<String>> loadPermissions() {
        try {
            ClassPathResource resource = new ClassPathResource("config/config.json");
            try (var in = resource.getInputStream()) {
                return objectMapper.readValue(in, new TypeReference<>() {});
            }
        } catch (Exception e) {
            return Map.of();
        }
    }

    private Set<String> roles(String authorizationHeader) {
        Map<String, Object> claims = claims(authorizationHeader);
        Set<String> roles = new HashSet<>();
        collectRoles(claims.get("role"), roles);
        collectRoles(claims.get("roles"), roles);
        collectRoles(claims.get("authorities"), roles);

        Object realmAccess = claims.get("realm_access");
        if (realmAccess instanceof Map<?, ?> realmMap) {
            collectRoles(realmMap.get("roles"), roles);
        }

        Object resourceAccess = claims.get("resource_access");
        if (resourceAccess instanceof Map<?, ?> resourceMap) {
            for (Object value : resourceMap.values()) {
                if (value instanceof Map<?, ?> clientMap) {
                    collectRoles(clientMap.get("roles"), roles);
                }
            }
        }
        return roles;
    }

    private Map<String, Object> claims(String authorizationHeader) {
        try {
            String token = bearerToken(authorizationHeader);
            if (token == null) {
                return Map.of();
            }
            String[] parts = token.split("\\.");
            if (parts.length < 2) {
                return Map.of();
            }
            byte[] payload = Base64.getUrlDecoder().decode(parts[1]);
            String json = new String(payload, StandardCharsets.UTF_8);
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            return Map.of();
        }
    }

    private String bearerToken(String authorizationHeader) {
        if (authorizationHeader == null || authorizationHeader.isBlank()) {
            return null;
        }
        String trimmed = authorizationHeader.trim();
        if (!trimmed.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return null;
        }
        return trimmed.substring(7).trim();
    }

    private void collectRoles(Object value, Set<String> roles) {
        for (String role : asStrings(value)) {
            String normalized = normalizeRole(role);
            if (!normalized.isBlank()) {
                roles.add(normalized);
            }
        }
    }

    private List<String> asStrings(Object value) {
        if (value instanceof String role) {
            return List.of(role);
        }
        if (value instanceof Iterable<?> iterable) {
            List<String> roles = new ArrayList<>();
            for (Object item : iterable) {
                if (item instanceof String role) {
                    roles.add(role);
                }
            }
            return roles;
        }
        return List.of();
    }

    private String normalizeRole(String role) {
        String normalized = role == null ? "" : role.trim().toLowerCase();
        return normalized.startsWith("role_") ? normalized.substring(5) : normalized;
    }
}
