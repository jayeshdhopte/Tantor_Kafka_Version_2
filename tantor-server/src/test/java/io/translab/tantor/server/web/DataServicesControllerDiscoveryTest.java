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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataServicesControllerDiscoveryTest {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void discoversConfiguredSchemaRegistryOnlyAfterSubjectsRespondWithJsonArray() throws Exception {
        server = serverReturning("/subjects", "[]");
        Fixture fixture = fixture("schemaRegistryUrl");

        var result = fixture.controller().discoverServiceEndpoint(
                fixture.cluster(), DataServicesController.ServiceKind.SCHEMA_REGISTRY);

        assertThat(result.detected()).isTrue();
        assertThat(result.protocol()).isEqualTo("http");
        assertThat(result.host()).isEqualTo("127.0.0.1");
        assertThat(result.port()).isEqualTo(server.getAddress().getPort());
    }

    @Test
    void discoversConfiguredKafkaConnectOnlyAfterConnectorsRespondWithJsonArray() throws Exception {
        server = serverReturning("/connectors", "[]");
        Fixture fixture = fixture("kafkaConnectUrl");

        var result = fixture.controller().discoverServiceEndpoint(
                fixture.cluster(), DataServicesController.ServiceKind.KAFKA_CONNECT);

        assertThat(result.detected()).isTrue();
        assertThat(result.endpoint()).isEqualTo("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Test
    void doesNotClaimDetectionWhenNoCandidateHostOrConfiguredUrlExists() {
        Fixture fixture = fixture(null);

        var result = fixture.controller().discoverServiceEndpoint(
                fixture.cluster(), DataServicesController.ServiceKind.SCHEMA_REGISTRY);

        assertThat(result.detected()).isFalse();
        assertThat(result.message()).contains("No Schema Registry endpoint could be detected");
    }

    @Test
    void rejectsAnHttpEndpointThatDoesNotReturnTheExpectedServicePayload() throws Exception {
        server = serverReturning("/subjects", "{}");
        Fixture fixture = fixture("schemaRegistryUrl");

        var result = fixture.controller().discoverServiceEndpoint(
                fixture.cluster(), DataServicesController.ServiceKind.SCHEMA_REGISTRY);

        assertThat(result.detected()).isFalse();
    }

    @Test
    void summaryDoesNotCallAnUnverifiedKafkaHostAndConventionalPort() {
        Fixture fixture = fixture(null);

        assertThatThrownBy(() -> fixture.controller().schemaRegistrySummary(
                fixture.cluster().getId(), null, null, null, null, null))
                .hasMessageContaining("No verified Schema Registry connection is configured");
    }

    private Fixture fixture(String configKey) {
        ClusterRepository clusters = mock(ClusterRepository.class);
        ExternalClusterRepository externalClusters = mock(ExternalClusterRepository.class);
        HostRepository hosts = mock(HostRepository.class);
        DataServiceConnectionService connections = mock(DataServiceConnectionService.class);
        EncryptionService encryption = mock(EncryptionService.class);
        RoleAuthenticationUtil roles = mock(RoleAuthenticationUtil.class);
        Cluster cluster = new Cluster();
        cluster.setId(UUID.randomUUID());
        if (configKey != null) {
            cluster.setConfigJson("{\"" + configKey + "\":\"http://127.0.0.1:"
                    + server.getAddress().getPort() + "\"}");
        }
        when(connections.resolveBaseUrlFromDb(any(), anyString(), any())).thenReturn(Optional.empty());
        when(connections.getActiveConnection(any(), anyString(), any())).thenReturn(Optional.empty());
        when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));

        return new Fixture(new DataServicesController(clusters, externalClusters, hosts,
                new ObjectMapper(), connections, encryption, roles), cluster);
    }

    private HttpServer serverReturning(String path, String body) throws Exception {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.createContext(path, exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        httpServer.start();
        return httpServer;
    }

    private record Fixture(DataServicesController controller, Cluster cluster) { }
}
