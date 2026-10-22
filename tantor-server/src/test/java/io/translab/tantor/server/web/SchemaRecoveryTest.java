package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.*;
import com.sun.net.httpserver.HttpServer;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.service.DataServiceConnectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SchemaRecoveryTest {
    HttpServer server;
    ObjectMapper mapper = new ObjectMapper();
    List<String> requests = new ArrayList<>();
    List<JsonNode> registrations = new ArrayList<>();
    boolean active, missing, rejected, lookupFails, subjectDeleted;
    int recoveredVersion = 7;
    UUID clusterId = UUID.randomUUID(), connectionId = UUID.randomUUID();
    AuditService audit = mock(AuditService.class);
    RoleAuthenticationUtil roles = mock(RoleAuthenticationUtil.class);
    DataServiceConnectionService connections = mock(DataServiceConnectionService.class);

    @BeforeEach void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().toString(), method = exchange.getRequestMethod();
            requests.add(method + " " + path);
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String response = "{}"; int status = 200;
            if (path.endsWith("/versions/2?deleted=true")) {
                if (missing) { status = 404; response = "{\"message\":\"not found\"}"; }
                else response = """
                    {"subject":"orders value","version":2,"id":42,"schemaType":"AVRO",
                     "schema":"{\\\"type\\\":\\\"string\\\"}",
                     "references":[{"name":"dependency","subject":"common","version":1}],
                     "metadata":{"properties":{"owner":"sensitive-schema-owner"}},"ruleSet":{"domainRules":[]}}
                    """;
            } else if (path.endsWith("/versions") && method.equals("GET")) {
                if (subjectDeleted) { status = 404; response = "{}"; }
                else response = active ? "[2,3]" : "[3]";
            } else if (path.endsWith("/versions") && method.equals("POST")) {
                registrations.add(mapper.readTree(body));
                if (rejected) { status = 409; response = "{\"message\":\"Incompatible schema\"}"; }
                else response = "{\"id\":42}";
            } else if (method.equals("POST")) {
                if (lookupFails) { status = 503; response = "{}"; }
                else response = "{\"id\":42,\"version\":" + recoveredVersion + "}";
            }
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes); exchange.close();
        });
        server.start();
        when(roles.canAccess("token", RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)).thenReturn(true);
        when(connections.resolveBaseUrlFromDb(any(), anyString(), any())).thenReturn(Optional.empty());
        when(connections.getActiveConnection(any(), anyString(), any())).thenReturn(Optional.empty());
    }
    @AfterEach void stop() { server.stop(0); }
    DataServicesController controller() {
        var clusters = mock(ClusterRepository.class);
        var cluster = new Cluster(); cluster.setId(clusterId);
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        return new DataServicesController(clusters, mock(ExternalClusterRepository.class), mock(HostRepository.class),
                mapper, connections, mock(EncryptionService.class), roles, audit);
    }
    org.springframework.http.ResponseEntity<?> recover() {
        return controller().recoverSchemaVersion("token", clusterId, "orders value", "2", null,
                "http", "127.0.0.1", server.getAddress().getPort(), connectionId);
    }
    @Test void reRegistersOriginalSchemaAndReferencesAndReturnsActualRegistryVersion() {
        var result = mapper.valueToTree(recover().getBody());
        assertThat(result.path("version").asInt()).isEqualTo(7);
        assertThat(result.path("id").asInt()).isEqualTo(42);
        assertThat(result.path("verified").asBoolean()).isTrue();
        assertThat(registrations).hasSize(1);
        var payload = registrations.getFirst();
        assertThat(payload.path("references").get(0).path("subject").asText()).isEqualTo("common");
        assertThat(payload.has("metadata")).isTrue(); assertThat(payload.has("ruleSet")).isTrue();
        assertThat(payload.has("id") || payload.has("version") || payload.has("subject")).isFalse();
        assertThat(requests).contains("GET /subjects/orders%20value/versions/2?deleted=true",
                "POST /subjects/orders%20value/versions", "POST /subjects/orders%20value");
        assertThat(requests).noneMatch(r -> r.contains("/mode") || r.contains("/config") || r.startsWith("DELETE"));
        var details = ArgumentCaptor.forClass(Map.class);
        verify(audit).record(eq("CLUSTER_CHANGE"), eq("SCHEMA_REGISTRY_VERSION_RECOVER"), eq("SCHEMA_VERSION"),
                eq("orders value/2"), eq(clusterId), eq("SUCCESS"), isNull(), isNull(), isNull(), details.capture());
        assertThat(details.getValue().toString()).contains(connectionId.toString(), "recoveredVersion=7")
                .doesNotContain("sensitive-schema-owner", "domainRules");
    }
    @Test void allowsRegistryToReturnTheSameVersionRatherThanInventingAnIncrement() {
        recoveredVersion = 2;
        assertThat(mapper.valueToTree(recover().getBody()).path("version").asInt()).isEqualTo(2);
    }
    @Test void recoversOneVersionWhenTheWholeSubjectWasSoftDeleted() {
        subjectDeleted = true;
        assertThat(mapper.valueToTree(recover().getBody()).path("verified").asBoolean()).isTrue();
        assertThat(registrations).hasSize(1);
    }
    @Test void rejectsAlreadyActiveAndHardDeletedVersionsWithoutRegistration() {
        active = true;
        assertThat(recover().getStatusCode().value()).isEqualTo(409);
        assertThat(registrations).isEmpty();
        active = false; missing = true;
        assertThatThrownBy(this::recover).isInstanceOf(RuntimeException.class);
        assertThat(registrations).isEmpty();
    }
    @Test void permissionsAndNumberedVersionAreRequiredBeforeContactingRegistry() {
        when(roles.canAccess("token", RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)).thenReturn(false);
        assertThat(recover().getStatusCode().is2xxSuccessful()).isFalse();
        assertThat(requests).isEmpty();
        when(roles.canAccess("token", RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)).thenReturn(true);
        assertThatThrownBy(() -> controller().recoverSchemaVersion("token", clusterId, "orders", "latest", null,
                "http", "127.0.0.1", server.getAddress().getPort(), connectionId)).isInstanceOf(IllegalArgumentException.class);
        assertThat(requests).isEmpty();
    }
    @Test void reportsRegistrationFailureAndAuditsIt() {
        rejected = true;
        assertThatThrownBy(this::recover).isInstanceOf(RuntimeException.class);
        verify(audit).record(eq("CLUSTER_CHANGE"), eq("SCHEMA_REGISTRY_VERSION_RECOVER"), eq("SCHEMA_VERSION"),
                anyString(), eq(clusterId), eq("FAILED"), isNull(), isNull(), isNull(), anyMap());
    }
    @Test void successfulRegistrationWithFailedLookupIsNotReportedAsFailedOrRetried() {
        lookupFails = true;
        var result = mapper.valueToTree(recover().getBody());
        assertThat(result.path("verified").asBoolean()).isFalse();
        assertThat(result.has("version")).isFalse();
        assertThat(result.path("message").asText()).contains("registration succeeded");
        assertThat(registrations).hasSize(1);
    }
}
