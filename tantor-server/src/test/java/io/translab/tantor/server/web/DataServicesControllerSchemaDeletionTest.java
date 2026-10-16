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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataServicesControllerSchemaDeletionTest {
    private HttpServer server;
    private boolean rejectSubjectConfig;
    private final CopyOnWriteArrayList<String> requests = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> bodies = new CopyOnWriteArrayList<>();

    @AfterEach
    void stopServer() { if (server != null) server.stop(0); }

    @Test
    void listsDeletedSubjectsAndVersionsWithoutConfusingThemWithActiveOnes() throws Exception {
        startServer();
        var fixture = fixture(true);
        @SuppressWarnings("unchecked") var summary = (Map<String, Object>) fixture.controller.schemaRegistrySummary(
                fixture.clusterId, null, "http", "127.0.0.1", server.getAddress().getPort(), null, true).getBody();
        @SuppressWarnings("unchecked") var subjects = (java.util.List<Map<String, Object>>) summary.get("subjects");
        assertThat(subjects).extracting(row -> row.get("subject") + ":" + row.get("deleted"))
                .containsExactly("active-value:false", "old-value:true");
        @SuppressWarnings("unchecked") var details = (Map<String, Object>) fixture.controller.getSubjectDetails(
                fixture.clusterId, "active-value", null, "http", "127.0.0.1", server.getAddress().getPort(), null, true).getBody();
        @SuppressWarnings("unchecked") var versions = (java.util.List<Map<String, Object>>) details.get("versions");
        assertThat(versions).extracting(row -> row.get("version") + ":" + row.get("deleted"))
                .containsExactly("1:true", "2:false");
        @SuppressWarnings("unchecked") var deletedDetails = (Map<String, Object>) fixture.controller.getSubjectDetails(
                fixture.clusterId, "old-value", null, "http", "127.0.0.1", server.getAddress().getPort(), null, true).getBody();
        @SuppressWarnings("unchecked") var deletedLatest = (Map<String, Object>) deletedDetails.get("latest");
        assertThat(deletedLatest).containsEntry("version", 1).containsEntry("deleted", true);
        assertThat(requests).contains("GET /subjects?deleted=true", "GET /subjects/active-value/versions?deleted=true");
    }

    @Test
    void forwardsPermanentDeleteOnlyWithItsSeparateAdminPermission() throws Exception {
        startServer();
        var denied = fixture(false);
        assertThat(denied.controller.deleteSubject(null, denied.clusterId, "old-value", null,
                "http", "127.0.0.1", server.getAddress().getPort(), null, true).getStatusCode().is2xxSuccessful()).isFalse();
        assertThat(requests).isEmpty();

        var allowed = fixture(true);
        allowed.controller.deleteSchemaVersion(null, allowed.clusterId, "old-value", "1", null,
                "http", "127.0.0.1", server.getAddress().getPort(), null, true);
        assertThat(requests).contains("DELETE /subjects/old-value/versions/1?permanent=true");
        allowed.controller.deleteSubject(null, allowed.clusterId, "active-value", null,
                "http", "127.0.0.1", server.getAddress().getPort(), null, false);
        assertThat(requests).contains("DELETE /subjects/active-value");
        allowed.controller.deleteSchemaVersion(null, allowed.clusterId, "active-value", "2", null,
                "http", "127.0.0.1", server.getAddress().getPort(), null, false);
        assertThat(requests).contains("DELETE /subjects/active-value/versions/2");
    }

    @Test
    void readsEffectiveModesAndForwardsScopedModeUpdates() throws Exception {
        startServer();
        var f = fixture(true);
        int port = server.getAddress().getPort();
        f.controller.getSchemaRegistryMode(f.clusterId, null, null, "http", "127.0.0.1", port, null);
        f.controller.getSchemaRegistryMode(f.clusterId, "active-value", null, "http", "127.0.0.1", port, null);
        var body = new ObjectMapper().readTree("{\"mode\":\"READONLY\"}");
        assertThat(f.controller.updateSchemaRegistryMode(null, f.clusterId, null, null, "http", "127.0.0.1", port, null, body)
                .getStatusCode().is2xxSuccessful()).isTrue();
        f.controller.updateSchemaRegistryMode(null, f.clusterId, "active-value", null, "http", "127.0.0.1", port, null, body);
        assertThat(requests).contains("GET /mode", "GET /mode/active-value?defaultToGlobal=true",
                "PUT /mode", "PUT /mode/active-value");
        assertThat(bodies).contains("{\"mode\":\"READONLY\"}");
    }

    @Test
    void rejectsUnsupportedModesAndUnauthorizedWritesWithoutCallingRegistry() throws Exception {
        startServer();
        var f = fixture(true);
        var body = new ObjectMapper().readTree("{\"mode\":\"IMPORT\"}");
        assertThat(f.controller.updateSchemaRegistryMode(null, f.clusterId, null, null, "http", "127.0.0.1",
                server.getAddress().getPort(), null, body).getStatusCode().value()).isEqualTo(400);
        assertThat(requests).isEmpty();
        var roles = mock(RoleAuthenticationUtil.class);
        var denied = new DataServicesController(mock(ClusterRepository.class), mock(ExternalClusterRepository.class),
                mock(HostRepository.class), new ObjectMapper(), mock(DataServiceConnectionService.class),
                mock(EncryptionService.class), roles);
        body = new ObjectMapper().readTree("{\"mode\":\"READWRITE\"}");
        assertThat(denied.updateSchemaRegistryMode(null, f.clusterId, null, null, "http", "127.0.0.1",
                server.getAddress().getPort(), null, body).getStatusCode().is2xxSuccessful()).isFalse();
        assertThat(requests).isEmpty();
    }

    @Test
    void forwardsNoneCompatibilityForGlobalAndSubjectSettings() throws Exception {
        startServer();
        var f = fixture(true);
        var body = new ObjectMapper().readTree("{\"compatibility\":\"NONE\"}");
        f.controller.updateGlobalCompatibility(null, f.clusterId, null, "http", "127.0.0.1", server.getAddress().getPort(), null, body);
        f.controller.updateSubjectCompatibility(null, f.clusterId, "active-value", null, "http", "127.0.0.1", server.getAddress().getPort(), null, body);
        assertThat(requests).contains("PUT /config", "PUT /config/active-value");
        assertThat(bodies).contains("{\"compatibility\":\"NONE\"}");
    }

    @Test
    void routesModeRequestsAndExplainsWhenRegistryDisallowsChanges() throws Exception {
        startServer();
        var f = fixture(true);
        var mvc = org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup(f.controller).build();
        String base = "/api/v1/clusters/" + f.clusterId + "/data-services/schema-registry";
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(base + "/mode")
                .param("protocol", "http").param("ip", "127.0.0.1").param("port", "" + server.getAddress().getPort()))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.mode").value("READWRITE"));
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put(base + "/subjects/locked/mode")
                .param("protocol", "http").param("ip", "127.0.0.1").param("port", "" + server.getAddress().getPort())
                .contentType("application/json").content("{\"mode\":\"READONLY\"}"))
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isUnprocessableEntity())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.message")
                        .value(org.hamcrest.Matchers.containsString("mode.mutability=true")));
    }

    @Test
    void includesSubjectCompatibilityAndDistinguishesInheritedFromUnavailable() throws Exception {
        startServer();
        var f = fixture(true);
        var response = f.controller.schemaRegistrySummary(f.clusterId, null, "http", "127.0.0.1",
                server.getAddress().getPort(), null, true);
        JsonAssertions.assertCompatibility(response.getBody(), "active-value", "BACKWARD");
        JsonAssertions.assertCompatibility(response.getBody(), "old-value", "DEFAULT");
        rejectSubjectConfig = true;
        response = f.controller.schemaRegistrySummary(f.clusterId, null, "http", "127.0.0.1",
                server.getAddress().getPort(), null, false);
        JsonAssertions.assertCompatibility(response.getBody(), "active-value", "UNKNOWN");
    }

    private static class JsonAssertions {
        static void assertCompatibility(Object body, String subject, String compatibility) {
            var json = new ObjectMapper().valueToTree(body);
            for (var row : json.path("subjects")) {
                if (subject.equals(row.path("subject").asText())) {
                    assertThat(row.path("compatibility").asText()).isEqualTo(compatibility); return;
                }
            }
            org.junit.jupiter.api.Assertions.fail("Subject missing: " + subject);
        }
    }

    private Fixture fixture(boolean permanentAllowed) {
        ClusterRepository clusters = mock(ClusterRepository.class);
        DataServiceConnectionService connections = mock(DataServiceConnectionService.class);
        RoleAuthenticationUtil roles = mock(RoleAuthenticationUtil.class);
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = new Cluster(); cluster.setId(clusterId);
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(connections.resolveBaseUrlFromDb(any(), anyString(), any())).thenReturn(Optional.empty());
        when(connections.getActiveConnection(any(), anyString(), any())).thenReturn(Optional.empty());
        when(roles.canAccess(null, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)).thenReturn(true);
        when(roles.canAccess(null, RoleAuthenticationUtil.SCHEMA_REGISTRY_PERMANENT_DELETE)).thenReturn(permanentAllowed);
        return new Fixture(new DataServicesController(clusters, mock(ExternalClusterRepository.class), mock(HostRepository.class),
                new ObjectMapper(), connections, mock(EncryptionService.class), roles), clusterId);
    }

    private void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String target = exchange.getRequestURI().toString();
            requests.add(exchange.getRequestMethod() + " " + target);
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            if (target.equals("/mode/locked") && exchange.getRequestMethod().equals("PUT")) {
                byte[] failure = "{\"error_code\":42205,\"message\":\"Mode changes are not allowed\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(422, failure.length);
                exchange.getResponseBody().write(failure); exchange.close(); return;
            }
            if (target.equals("/config/old-value") || (target.equals("/config/active-value") && rejectSubjectConfig)) {
                byte[] failure = "{\"message\":\"Configuration unavailable\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(target.equals("/config/old-value") ? 404 : 503, failure.length);
                exchange.getResponseBody().write(failure); exchange.close(); return;
            }
            String response = switch (target) {
                case "/mode", "/mode/active-value", "/mode/active-value?defaultToGlobal=true" -> "{\"mode\":\"READWRITE\"}";
                case "/subjects" -> "[\"active-value\"]";
                case "/subjects?deleted=true" -> "[\"active-value\",\"old-value\"]";
                case "/subjects/active-value/versions/latest", "/subjects/active-value/versions/2?deleted=true" -> schema(2);
                case "/subjects/active-value/versions" -> "[2]";
                case "/subjects/active-value/versions?deleted=true" -> "[1,2]";
                case "/subjects/active-value/versions/1?deleted=true" -> schema(1);
                case "/subjects/old-value/versions?deleted=true" -> "[1]";
                case "/subjects/old-value/versions/1?deleted=true" -> schema(1);
                case "/config/active-value", "/config" -> "{\"compatibilityLevel\":\"BACKWARD\"}";
                default -> "[]";
            };
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }

    private String schema(int version) {
        return "{\"version\":" + version + ",\"id\":" + version
                + ",\"schemaType\":\"AVRO\",\"schema\":\"{}\"}";
    }

    private record Fixture(DataServicesController controller, UUID clusterId) { }
}
