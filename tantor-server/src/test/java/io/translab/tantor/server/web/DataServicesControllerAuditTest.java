package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private Fixture fixture() {
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
                clusters, externalClusters, hosts, mapper, connections, encryption, roleAuthenticationUtil);
        return new Fixture(controller, mapper, clusterId, connectionId);
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
                           ObjectMapper objectMapper, UUID clusterId, UUID connectionId) {
    }
}
