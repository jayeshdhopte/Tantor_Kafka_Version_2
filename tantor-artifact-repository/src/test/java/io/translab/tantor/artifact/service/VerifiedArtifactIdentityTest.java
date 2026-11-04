package io.translab.tantor.artifact.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.translab.tantor.artifact.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.net.InetSocketAddress;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class VerifiedArtifactIdentityTest {
    @Test void callerNameAndRoleComeFromTheVerificationEndpointRatherThanUnsignedTokenClaims() throws Exception {
        var body=new java.util.concurrent.atomic.AtomicReference<>(Map.<String,Object>of("username","verified.operator","roles",List.of("ROLE_ADMIN")));
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/identity",exchange->{
            byte[] response=new ObjectMapper().writeValueAsBytes(body.get());
            exchange.sendResponseHeaders(200,response.length);try(var out=exchange.getResponseBody()){out.write(response);}
        });server.start();
        try {
            var roles=new RoleAuthenticationUtil(new ObjectMapper());ReflectionTestUtils.setField(roles,"identityUrl","http://127.0.0.1:"+server.getAddress().getPort()+"/identity");
            String token="Bearer unsigned.payload.is.not.trusted";
            assertThat(roles.username(token)).isEqualTo("verified.operator");assertThat(roles.canAccess(token,RoleAuthenticationUtil.ARTIFACT_UPLOAD)).isTrue();
            body.set(Map.of("username","system","roles",List.of("ROLE_MONITOR")));
            assertThat(roles.username(token)).isEqualTo("system");assertThat(roles.canAccess(token,RoleAuthenticationUtil.ARTIFACT_UPLOAD)).isFalse();
        } finally {server.stop(0);}
    }
    @Test void rejectedCredentialsCannotBecomeAnActorOrAcquirePermissions() throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/identity",exchange->{exchange.sendResponseHeaders(401,-1);exchange.close();});server.start();
        try {
            var roles=new RoleAuthenticationUtil(new ObjectMapper());ReflectionTestUtils.setField(roles,"identityUrl","http://127.0.0.1:"+server.getAddress().getPort()+"/identity");
            String token="Bearer eyJhbGciOiJub25lIn0.eyJzdWIiOiJhZG1pbjAxIiwicm9sZXMiOlsiYWRtaW4iXX0.";
            assertThat(roles.authenticatedUsername(token)).isNull();assertThat(roles.username(token)).isEqualTo("unknown");
            assertThat(roles.canAccess(token,RoleAuthenticationUtil.ARTIFACT_UPLOAD)).isFalse();
        } finally {server.stop(0);}
    }
}
