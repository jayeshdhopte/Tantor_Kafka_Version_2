package io.translab.tantor.server.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import io.translab.tantor.server.config.OidcProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Issues Tantor local tokens and verifies every token before exposing identity or roles. */
@Component
public class JwtUtils {
    @Value("${tantor.security.jwt.secret}")
    private String jwtSecret;
    @Value("${tantor.security.jwt.expiration-ms}")
    private int jwtExpirationMs;
    @Value("${tantor.security.jwt.issuer:tantor-server}")
    private String issuer;
    @Value("${tantor.security.jwt.audience:tantor-api}")
    private String audience;
    private final OidcProperties oidcProperties;

    public JwtUtils() {
        this(new OidcProperties());
    }

    @Autowired
    public JwtUtils(OidcProperties oidcProperties) {
        this.oidcProperties = oidcProperties;
    }

    private volatile JwtDecoder externalDecoder;
    private volatile String decoderIssuer;

    public record VerifiedPrincipal(String username, List<String> roles, Map<String, Object> claims) { }

    private SecretKey key() {
        return Keys.hmacShaKeyFor(Decoders.BASE64.decode(jwtSecret));
    }

    public String generateToken(String username, String role, String authSource) {
        Date now = new Date();
        String normalizedRole = normalizeRole(role);
        if (normalizedRole.isBlank()) normalizedRole = "monitor";
        return Jwts.builder()
                .issuer(issuer)
                .subject(username)
                .audience().add(audience).and()
                .claim("roles", List.of(normalizedRole))
                .claim("auth_source", authSource)
                .issuedAt(now)
                .expiration(new Date(now.getTime() + jwtExpirationMs))
                .signWith(key())
                .compact();
    }

    /** @deprecated Use {@link #generateToken(String, String, String)} so the role is explicit. */
    @Deprecated
    public String generateTokenFromUsername(String username) {
        return generateToken(username, "monitor", "local");
    }

    public VerifiedPrincipal verify(String token) {
        if (token == null || token.isBlank()) return null;
        try {
            Claims claims = Jwts.parser().verifyWith(key()).build().parseSignedClaims(token).getPayload();
            if (!issuer.equals(claims.getIssuer()) || !claims.getAudience().contains(audience)) return null;
            return principal(claims.getSubject(), claims);
        } catch (Exception ignored) {
            // A local signature failure is not a licence to read JWT payloads.
        }

        JwtDecoder decoder = externalDecoder();
        if (decoder == null) return null;
        try {
            Jwt jwt = decoder.decode(token);
            String username = firstClaim(jwt, "preferred_username", "email", "sub");
            return principal(username, jwt.getClaims());
        } catch (Exception ignored) {
            return null;
        }
    }

    public boolean validateJwtToken(String token) { return verify(token) != null; }
    public String getUserNameFromJwtToken(String token) {
        VerifiedPrincipal principal = verify(token);
        return principal == null ? null : principal.username();
    }
    public String getIdentityFromJwtToken(String token) { return getUserNameFromJwtToken(token); }

    private JwtDecoder externalDecoder() {
        String configuredIssuer = oidcProperties.getIssuerUri() == null ? "" : oidcProperties.getIssuerUri().toString();
        if (configuredIssuer.isBlank()) return null;
        if (configuredIssuer.equals(decoderIssuer) && externalDecoder != null) return externalDecoder;
        synchronized (this) {
            if (!configuredIssuer.equals(decoderIssuer) || externalDecoder == null) {
                JwtDecoder configuredDecoder = JwtDecoders.fromIssuerLocation(configuredIssuer);
                if (configuredDecoder instanceof NimbusJwtDecoder nimbusDecoder) {
                    OAuth2TokenValidator<Jwt> issuerValidator = JwtValidators.createDefaultWithIssuer(configuredIssuer);
                    String expectedAudience = oidcProperties.getAudience() == null ? "" : oidcProperties.getAudience().trim();
                    if (!expectedAudience.isBlank()) {
                        OAuth2TokenValidator<Jwt> audienceValidator = jwt -> isAudienceAccepted(jwt, expectedAudience)
                                ? OAuth2TokenValidatorResult.success()
                                : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", "Token audience is not permitted", null));
                        nimbusDecoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(issuerValidator, audienceValidator));
                    } else {
                        nimbusDecoder.setJwtValidator(issuerValidator);
                    }
                }
                externalDecoder = configuredDecoder;
                decoderIssuer = configuredIssuer;
            }
        }
        return externalDecoder;
    }

    /**
     * Keycloak access tokens normally carry the resource audience in {@code aud}.
     * Some standard client configurations instead expose the requesting client in
     * {@code azp} while retaining only Keycloak's account service in {@code aud}.
     * Both values remain issuer-signed claims, so accepting an exact configured
     * client match preserves token binding without weakening signature, issuer or
     * expiry validation.
     */
    boolean isAudienceAccepted(Jwt jwt, String expectedAudience) {
        return jwt.getAudience().contains(expectedAudience)
                || expectedAudience.equals(jwt.getClaimAsString("azp"));
    }

    private VerifiedPrincipal principal(String username, Map<String, Object> claims) {
        if (username == null || username.isBlank()) return null;
        return new VerifiedPrincipal(username, extractRoles(claims),
                java.util.Collections.unmodifiableMap(new LinkedHashMap<>(claims)));
    }

    private String firstClaim(Jwt jwt, String... names) {
        for (String name : names) {
            String value = jwt.getClaimAsString(name);
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private String normalizeRole(String role) {
        if (role == null) return "";
        String normalized = role.trim().toLowerCase();
        int lastSeparator = normalized.lastIndexOf('/');
        if (lastSeparator >= 0) normalized = normalized.substring(lastSeparator + 1);
        if (normalized.startsWith("role_")) normalized = normalized.substring(5);
        if ("administrator".equals(normalized)) return "admin";
        if ("admin".equals(normalized)) return "admin";
        if ("monitor".equals(normalized) || "viewer".equals(normalized)
                || "readonly".equals(normalized) || "read_only".equals(normalized)) return "monitor";
        return "";
    }

    private List<String> extractRoles(Map<String, Object> claims) {
        Set<String> roles = new LinkedHashSet<>();
        collectRoles(claims.get("role"), roles);
        collectRoles(claims.get("roles"), roles);
        collectRoles(claims.get("groups"), roles);
        Object realmAccess = claims.get("realm_access");
        if (realmAccess instanceof Map<?, ?> realm) collectRoles(realm.get("roles"), roles);
        Object resourceAccess = claims.get("resource_access");
        if (resourceAccess instanceof Map<?, ?> resources) {
            resources.values().forEach(value -> {
                if (value instanceof Map<?, ?> resource) collectRoles(resource.get("roles"), roles);
            });
        }
        return List.copyOf(roles);
    }

    private void collectRoles(Object value, Set<String> roles) {
        if (value instanceof Collection<?> collection) {
            collection.forEach(item -> collectRoles(item, roles));
        } else if (value != null) {
            String role = normalizeRole(String.valueOf(value));
            if (!role.isBlank()) roles.add(role);
        }
    }
}
