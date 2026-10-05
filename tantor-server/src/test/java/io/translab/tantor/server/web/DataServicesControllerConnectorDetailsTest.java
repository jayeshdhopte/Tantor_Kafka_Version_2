package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.service.DataServiceConnectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataServicesControllerConnectorDetailsTest {

    @Test
    @SuppressWarnings("unchecked")
    void connectSummaryIncludesConnectorConfigurationAndTasks() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        responses(server, "/", "{\"version\":\"3.9.0\"}");
        responses(server, "/connectors", "[\"sample-connector\"]");
        responses(server, "/connector-plugins", "[]");
        responses(server, "/connectors/sample-connector/config",
                "{\"connector.class\":\"FileStreamSource\",\"tasks.max\":\"1\"}");
        responses(server, "/connectors/sample-connector/status",
                "{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"}]}");
        server.start();
        try {
            UUID clusterId = UUID.randomUUID();
            Cluster cluster = new Cluster();
            cluster.setId(clusterId);
            ClusterRepository clusters = mock(ClusterRepository.class);
            when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
            DataServicesController controller = new DataServicesController(clusters,
                    mock(ExternalClusterRepository.class), mock(HostRepository.class), new ObjectMapper(),
                    mock(DataServiceConnectionService.class), mock(EncryptionService.class),
                    mock(RoleAuthenticationUtil.class));

            Map<String, Object> summary = (Map<String, Object>) controller.kafkaConnectSummary(clusterId,
                    null, "http", "127.0.0.1", server.getAddress().getPort(), null).getBody();
            assertThat(summary).containsEntry("connectorCount", 1).containsEntry("runningTasks", 1);
            Map<String, Object> connector = ((List<Map<String, Object>>) summary.get("connectors")).getFirst();
            assertThat(connector).containsEntry("name", "sample-connector").containsEntry("state", "RUNNING");
            assertThat(((JsonNode) connector.get("config")).path("tasks.max").asText()).isEqualTo("1");
            assertThat(((JsonNode) connector.get("status")).path("tasks").get(0).path("state").asText())
                    .isEqualTo("RUNNING");
        } finally {
            server.stop(0);
        }
    }

    private static void responses(HttpServer server, String path, String body) {
        server.createContext(path, exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var stream = exchange.getResponseBody()) {
                stream.write(bytes);
            }
        });
    }
}
