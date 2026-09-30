package io.translab.tantor.server.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class JwtUtilsTest {

    @Test
    void signsAndVerifiesIdentityAndRoleClaims() {
        JwtUtils jwtUtils = configuredJwtUtils();
        String token = jwtUtils.generateToken("jayesh", "admin", "ldap");

        JwtUtils.VerifiedPrincipal principal = jwtUtils.verify(token);

        assertThat(principal).isNotNull();
        assertThat(principal.username()).isEqualTo("jayesh");
        assertThat(principal.roles()).containsExactly("admin");
        assertThat(principal.claims()).containsEntry("auth_source", "ldap");
    }

    @Test
    void rejectsUnsignedOrForgedJwtPayloads() {
        JwtUtils jwtUtils = configuredJwtUtils();
        String header = encode("{\"alg\":\"none\"}");
        String payload = encode("{\"sub\":\"attacker\",\"roles\":[\"admin\"]}");

        assertThat(jwtUtils.verify(header + "." + payload + ".signature")).isNull();
    }

    @Test
    void acceptsKeycloakAuthorizedPartyWhenAudienceMapperIsNotConfigured() {
        JwtUtils jwtUtils = configuredJwtUtils();
        Jwt token = Jwt.withTokenValue("test-token")
                .header("alg", "RS256")
                .claim("aud", java.util.List.of("account"))
                .claim("azp", "apb-kafka")
                .build();

        assertThat(jwtUtils.isAudienceAccepted(token, "apb-kafka")).isTrue();
        assertThat(jwtUtils.isAudienceAccepted(token, "another-client")).isFalse();
    }

    private JwtUtils configuredJwtUtils() {
        JwtUtils jwtUtils = new JwtUtils();
        ReflectionTestUtils.setField(jwtUtils, "jwtSecret",
                Base64.getEncoder().encodeToString("0123456789abcdef0123456789abcdef".getBytes()));
        ReflectionTestUtils.setField(jwtUtils, "jwtExpirationMs", 60_000);
        ReflectionTestUtils.setField(jwtUtils, "issuer", "tantor-server");
        ReflectionTestUtils.setField(jwtUtils, "audience", "tantor-api");
        return jwtUtils;
    }

    private String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes());
    }
}
