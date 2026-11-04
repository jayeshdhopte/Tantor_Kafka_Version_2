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
    @org.springframework.beans.factory.annotation.Value("${tantor.auth.identity-url:http://127.0.0.1:8443/api/v1/auth/identity}")
    private String identityUrl="http://127.0.0.1:8443/api/v1/auth/identity";
    private final java.net.http.HttpClient identityClient=java.net.http.HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(3)).followRedirects(java.net.http.HttpClient.Redirect.NEVER).build();

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
        String actor=authenticatedUsername(authorizationHeader);
        return actor==null?"unknown":actor;
    }

    public String authenticatedUsername(String authorizationHeader) {
        Map<String, Object> claims = claims(authorizationHeader);
        for (String key : List.of("username", "preferred_username", "name", "email", "sub")) {
            Object value = claims.get(key);
            if (value instanceof String text && !text.isBlank()) {
                return text;
            }
        }
        return null;
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

    private Map<String,Object> claims(String authorizationHeader) {
        if(bearerToken(authorizationHeader)==null)return Map.of();
        // Cache only within this HTTP request; no global token/principal cache can outlive revocation/expiry.
        var attributes=org.springframework.web.context.request.RequestContextHolder.getRequestAttributes();
        String cacheKey=getClass().getName()+".verified-identity";
        if(attributes!=null){
            Object cached=attributes.getAttribute(cacheKey,0);
            if(cached instanceof Map<?,?> entry && authorizationHeader.equals(entry.get("authorization"))) {
                @SuppressWarnings("unchecked") var value=(Map<String,Object>)entry.get("identity");return value;
            }
        }
        Map<String,Object> identity=Map.of();
        try {
            var request=java.net.http.HttpRequest.newBuilder(java.net.URI.create(identityUrl))
                    .timeout(java.time.Duration.ofSeconds(5)).header("Authorization",authorizationHeader).GET().build();
            var response=identityClient.send(request,java.net.http.HttpResponse.BodyHandlers.ofString());
            if(response.statusCode()==200)identity=objectMapper.readValue(response.body(),new TypeReference<>(){});
        } catch(InterruptedException interrupted){Thread.currentThread().interrupt();}
        catch(Exception unavailable){/* Fail closed: never decode unverified token claims as an identity. */}
        if(attributes!=null)attributes.setAttribute(cacheKey,Map.of("authorization",authorizationHeader,"identity",identity),0);
        return identity;
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
