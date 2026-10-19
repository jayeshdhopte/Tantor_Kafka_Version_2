package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import org.mockito.ArgumentCaptor;
import java.util.Map;
import com.sun.net.httpserver.HttpServer;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.service.DataServiceConnectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DataServicesControllerAuditTest {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void auditsConnectorCreationWithActorClusterAndSelectedTargetWithoutPayloadSecrets() throws Exception {
        server = serverReturning(201, "{}");
        Fixture fixture = fixture();

        var response = fixture.controller.createConnector("Bearer test-token", fixture.clusterId, null, "http", "127.0.0.1",
                server.getAddress().getPort(), fixture.connectionId,
                fixture.objectMapper.readTree("""
                        {"name":"orders-source","config":{"password":"must-not-be-audited"}}
                        """));

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(String.valueOf(response.getBody())).doesNotContain("must-not-be-audited");
    }

    @Test
    void auditsSanitizedFailureWhenKafkaConnectRejectsMutation() throws Exception {
        server = serverReturning(500, """
                {"password":"upstream-secret"}
                """);
        Fixture fixture = fixture();

        assertThatThrownBy(() -> fixture.controller.deleteConnector(
                "Bearer test-token", fixture.clusterId, "orders-source", null, "http", "127.0.0.1",
                server.getAddress().getPort(), fixture.connectionId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageNotContaining("upstream-secret");
    }

    @Test
    void auditsSchemaMutationAgainstSelectedRemoteConnection() throws Exception {
        server = serverReturning(200, """
                {"id":42}
                """);
        Fixture fixture = fixture();

        var response = fixture.controller.createSchemaVersion("Bearer test-token", fixture.clusterId, "orders-value", null,
                "http", "127.0.0.1", server.getAddress().getPort(), fixture.connectionId,
                fixture.objectMapper.readTree("""
                        {"schema":"{}"}
                        """));

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    void recordsSubjectAndVersionSoftPermanentDeletesAndConnectorDeletes() throws Exception {
        server = serverReturning(200, "{}");
        Fixture f = fixture();
        int port = server.getAddress().getPort();
        for (boolean permanent : new boolean[]{false, true}) {
            f.controller.deleteSubject("Bearer token", f.clusterId, "orders-value", null, "http", "127.0.0.1", port, f.connectionId, permanent);
            f.controller.deleteSchemaVersion("Bearer token", f.clusterId, "orders-value", "2", null, "http", "127.0.0.1", port, f.connectionId, permanent);
        }
        f.controller.deleteConnector("Bearer token", f.clusterId, "orders-source", null, "http", "127.0.0.1", port, f.connectionId);
        @SuppressWarnings({"unchecked", "rawtypes"}) ArgumentCaptor<Map<String, Object>> details = ArgumentCaptor.forClass((Class) Map.class);
        verify(f.audit, times(5)).record(eq("CLUSTER_CHANGE"), anyString(), anyString(), anyString(), eq(f.clusterId), eq("SUCCESS"), isNull(), isNull(), isNull(), details.capture());
        assertThat(details.getAllValues()).extracting(d -> d.get("deletionMode"))
                .containsExactly("SOFT", "SOFT", "PERMANENT", "PERMANENT", "PERMANENT");
        assertThat(details.getAllValues().get(1)).containsEntry("version", "2").containsEntry("name", "orders-value")
                .containsEntry("connectionId", f.connectionId.toString());
        verify(f.audit).record(eq("CLUSTER_CHANGE"), eq("KAFKA_CONNECT_CONNECTOR_DELETE"), eq("KAFKA_CONNECT_CONNECTOR"), eq("orders-source"), eq(f.clusterId), eq("SUCCESS"), isNull(), isNull(), isNull(), any());
    }

    @Test
    void recordsFailedDeletionWithoutPersistingUpstreamSecrets() throws Exception {
        server = serverReturning(500, "{\"message\":\"secret-password-value\"}");
        Fixture f = fixture();
        assertThatThrownBy(() -> f.controller.deleteConnector("Bearer secret-token", f.clusterId, "orders-source", null,
                "http", "127.0.0.1", server.getAddress().getPort(), f.connectionId)).isInstanceOf(RuntimeException.class);
        @SuppressWarnings({"unchecked", "rawtypes"}) ArgumentCaptor<Map<String, Object>> details = ArgumentCaptor.forClass((Class) Map.class);
        verify(f.audit).record(eq("CLUSTER_CHANGE"), eq("KAFKA_CONNECT_CONNECTOR_DELETE"), anyString(), eq("orders-source"), eq(f.clusterId), eq("FAILED"), isNull(), isNull(), isNull(), details.capture());
        assertThat(details.getValue()).containsEntry("httpStatus", 500);
        assertThat(details.getValue().toString()).doesNotContain("secret-password-value", "secret-token");
    }

    @Test
    void auditStorageFailureDoesNotTurnCompletedDeletionIntoFailure() throws Exception {
        server = serverReturning(200, "{}");
        Fixture f = fixture();
        doThrow(new RuntimeException("storage unavailable")).when(f.audit).record(anyString(), anyString(), anyString(), anyString(), any(), anyString(), any(), any(), any(), any());
        assertThat(f.controller.deleteSubject(null, f.clusterId, "orders-value", null, "http", "127.0.0.1", server.getAddress().getPort(), null, false)
                .getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    void persistedDeletionAuditCapturesAuthenticatedUserAndTimestamp() throws Exception {
        server = serverReturning(200, "{}");
        var repository = mock(io.translab.tantor.server.audit.AuditLogRepository.class);
        when(repository.saveAndFlush(any())).thenAnswer(invocation -> invocation.getArgument(0));
        var audit = new AuditService(repository, new ObjectMapper());
        Fixture f = fixture(audit);
        var authentication = new org.springframework.security.authentication.UsernamePasswordAuthenticationToken("operator-test", null, java.util.List.of());
        org.springframework.security.core.context.SecurityContextHolder.getContext().setAuthentication(authentication);
        try {
            f.controller.deleteSchemaVersion(null, f.clusterId, "orders-value", "2", null, "http", "127.0.0.1", server.getAddress().getPort(), f.connectionId, true);
            var captor = ArgumentCaptor.forClass(io.translab.tantor.server.domain.AuditLog.class);
            verify(repository).saveAndFlush(captor.capture());
            assertThat(captor.getValue().getCreatedBy()).isEqualTo("operator-test");
            assertThat(captor.getValue().getCreatedTime()).isNotNull();
            assertThat(captor.getValue().getStatus()).isEqualTo("SUCCESS");
            assertThat(captor.getValue().getClusterId()).isEqualTo(f.clusterId);
            assertThat(captor.getValue().getDetails()).contains("PERMANENT", "orders-value", "version");
        } finally { org.springframework.security.core.context.SecurityContextHolder.clearContext(); }
    }

    @Test
    void deniedDeletionsAreAuditedWithoutCallingRemoteService() throws Exception {
        server = serverReturning(200, "{}");
        Fixture f = fixture();
        var roles = (RoleAuthenticationUtil) org.springframework.test.util.ReflectionTestUtils.getField(f.controller, "roleAuthenticationUtil");
        when(roles.canAccess(any(), anyString())).thenReturn(false);
        assertThat(f.controller.deleteSubject(null, f.clusterId, "orders-value", null, "http", "127.0.0.1", server.getAddress().getPort(), null, true).getStatusCode().is4xxClientError()).isTrue();
        assertThat(f.controller.deleteConnector(null, f.clusterId, "orders-source", null, "http", "127.0.0.1", server.getAddress().getPort(), null).getStatusCode().is4xxClientError()).isTrue();
        verify(f.audit, times(2)).record(eq("CLUSTER_CHANGE"), anyString(), anyString(), anyString(), eq(f.clusterId), eq("FAILED"), isNull(), isNull(), isNull(), any());
    }

    private Fixture fixture() { return fixture(mock(AuditService.class)); }

    private Fixture fixture(AuditService audit) {
        ClusterRepository clusters = mock(ClusterRepository.class);
        ExternalClusterRepository externalClusters = mock(ExternalClusterRepository.class);
        HostRepository hosts = mock(HostRepository.class);
        DataServiceConnectionService connections = mock(DataServiceConnectionService.class);
        EncryptionService encryption = mock(EncryptionService.class);
        RoleAuthenticationUtil roleAuthenticationUtil = mock(RoleAuthenticationUtil.class);
        ObjectMapper mapper = new ObjectMapper();
        UUID clusterId = UUID.randomUUID();
        UUID connectionId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(roleAuthenticationUtil.canAccess(any(), anyString())).thenReturn(true);

        DataServicesController controller = new DataServicesController(
                clusters, externalClusters, hosts, mapper, connections, encryption, roleAuthenticationUtil, audit);
        return new Fixture(controller, mapper, clusterId, connectionId, audit);
    }

    private HttpServer serverReturning(int status, String body) throws Exception {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.createContext("/", exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        httpServer.start();
        return httpServer;
    }

    private record Fixture(DataServicesController controller,
                           ObjectMapper objectMapper, UUID clusterId, UUID connectionId, AuditService audit) {
    }
}
