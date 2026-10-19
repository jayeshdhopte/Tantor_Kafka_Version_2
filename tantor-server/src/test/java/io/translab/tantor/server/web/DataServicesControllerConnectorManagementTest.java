package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.service.DataServiceConnectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class DataServicesControllerConnectorManagementTest {
    private HttpServer server;
    private final UUID id = UUID.randomUUID();
    private final ObjectMapper mapper = new ObjectMapper();
    private final CopyOnWriteArrayList<String> requests = new CopyOnWriteArrayList<>();
    private boolean rejectRestart;
    private String savedSourceConfig = "{\"connector.class\":\"SourcePlugin\"}";
    private boolean rejectConfig;
    @AfterEach void close() { if (server != null) server.stop(0); }

    @Test void summaryIncludesNativeTypeWorkerTopicsAndActualTaskCounts() throws Exception {
        var controller = controller(true);
        var result = mapper.valueToTree(controller.kafkaConnectSummary(id, null, "http", "127.0.0.1", server.getAddress().getPort(), null).getBody());
        assertThat(result.path("failedConnectors").asInt()).isZero();
        assertThat(result.path("taskCount").asInt()).isEqualTo(3);
        var source = result.path("connectors").get(0);
        assertThat(source.path("type").asText()).isEqualTo("SOURCE");
        assertThat(source.path("worker").asText()).isEqualTo("worker1:8083");
        assertThat(source.path("runningTasks").asInt()).isEqualTo(1);
        assertThat(source.path("failedTasks").asInt()).isEqualTo(1);
        assertThat(source.path("topics").get(0).asText()).isEqualTo("orders");
        assertThat(source.path("topicsAvailable").asBoolean()).isTrue();
        var sink = result.path("connectors").get(1);
        assertThat(sink.path("type").asText()).isEqualTo("SINK");
        assertThat(sink.path("consumerGroup").asText()).isEqualTo("warehouse-readers");
        assertThat(sink.path("topicsAvailable").asBoolean()).isFalse();
        assertThat(result.path("connectors").get(2).path("topicsAvailable").asBoolean()).isTrue();
        assertThat(result.path("connectors").get(2).path("topics").isEmpty()).isTrue();
    }

    @Test void detailsRefreshAndRestartRequestsUseTheNativeApi() throws Exception {
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller(true)).build();
        mvc.perform(target(get(base() + "/source/details"))).andExpect(status().isOk()).andExpect(jsonPath("$.worker").value("worker1:8083"));
        mvc.perform(target(get(base() + "/sink/details"))).andExpect(status().isOk()).andExpect(jsonPath("$.type").value("SINK"));
        mvc.perform(target(post(base() + "/source/restart")).param("includeTasks", "true").param("onlyFailed", "true"))
                .andExpect(status().isOk());
        mvc.perform(target(post(base() + "/source/tasks/1/restart"))).andExpect(status().isOk());
        mvc.perform(target(put(base() + "/source/stop"))).andExpect(status().isOk());
        assertThat(requests).contains("POST /connectors/source/restart?includeTasks=true&onlyFailed=true",
                "POST /connectors/source/tasks/1/restart", "PUT /connectors/source/stop");
    }

    @Test void unauthorizedMutationsNeverReachConnectAndInvalidTaskIdsAreRejected() throws Exception {
        MockMvc denied = MockMvcBuilders.standaloneSetup(controller(false)).build();
        denied.perform(target(post(base() + "/source/restart"))).andExpect(status().is4xxClientError());
        denied.perform(target(put(base() + "/source/stop"))).andExpect(status().is4xxClientError());
        denied.perform(target(post(base() + "/source/tasks/0/restart"))).andExpect(status().is4xxClientError());
        assertThat(requests).isEmpty();
        server.stop(0); server = null;
        MockMvc allowed = MockMvcBuilders.standaloneSetup(controller(true)).build();
        allowed.perform(target(post(base() + "/source/tasks/-1/restart"))).andExpect(status().isBadRequest());
        assertThat(requests).isEmpty();
    }

    @Test void returnsNativeConflictInsteadOfReportingRestartSuccess() throws Exception {
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller(true)).build();
        rejectRestart = true;
        mvc.perform(target(post(base() + "/source/restart"))).andExpect(status().isConflict())
                .andExpect(jsonPath("$.backendStatus").value(409));
    }

    @Test void configReadBeforeSaveAndReadBackUseTheNativeConfigApi() throws Exception {
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller(true)).build();
        mvc.perform(target(get(base() + "/source/config"))).andExpect(status().isOk())
                .andExpect(jsonPath("$['connector.class']").value("SourcePlugin"));
        String updated = "{\"connector.class\":\"SourcePlugin\",\"tasks.max\":\"2\",\"topic\":\"orders-new\"}";
        mvc.perform(target(put(base() + "/source/config")).contentType("application/json").content(updated))
                .andExpect(status().isOk());
        mvc.perform(target(get(base() + "/source/config"))).andExpect(status().isOk())
                .andExpect(jsonPath("$['tasks.max']").value("2"))
                .andExpect(jsonPath("$.topic").value("orders-new"));
        assertThat(mapper.readTree(savedSourceConfig)).isEqualTo(mapper.readTree(updated));
        assertThat(requests).containsExactly("GET /connectors/source/config", "PUT /connectors/source/config", "GET /connectors/source/config");
    }

    @Test void configValidationErrorsAreReturnedWithoutChangingStoredConfiguration() throws Exception {
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller(true)).build();
        rejectConfig = true;
        mvc.perform(target(put(base() + "/source/config")).contentType("application/json").content("{\"tasks.max\":\"invalid\"}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.backendStatus").value(400));
        assertThat(savedSourceConfig).isEqualTo("{\"connector.class\":\"SourcePlugin\"}");
    }

    @Test void configUpdatesStillRequireManagementPermission() throws Exception {
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller(false)).build();
        mvc.perform(target(put(base() + "/source/config")).contentType("application/json").content("{}"))
                .andExpect(status().is4xxClientError());
        assertThat(requests).isEmpty();
    }

    private String base() { return "/api/v1/clusters/" + id + "/data-services/kafka-connect/connectors"; }
    private MockHttpServletRequestBuilder target(MockHttpServletRequestBuilder request) {
        return request.param("protocol", "http").param("ip", "127.0.0.1").param("port", String.valueOf(server.getAddress().getPort()));
    }
    private DataServicesController controller(boolean admin) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            int status = 200;
            String body = "{}";
            if (path.equals("/")) body = "{\"version\":\"4.3.0\"}";
            else if (path.equals("/connectors")) body = "[\"source\",\"sink\",\"stopped\"]";
            else if (path.equals("/connector-plugins")) body = "[{\"class\":\"SinkPlugin\",\"type\":\"sink\"}]";
            else if (path.endsWith("/config")) {
                if (exchange.getRequestMethod().equals("PUT")) {
                    if (rejectConfig) { status = 400; body = "{\"message\":\"Invalid configuration\"}"; }
                    else { savedSourceConfig = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8); body = savedSourceConfig; }
                } else body = path.contains("/sink/")
                        ? "{\"connector.class\":\"SinkPlugin\",\"consumer.override.group.id\":\"warehouse-readers\"}" : savedSourceConfig;
            }
            else if (path.endsWith("/status")) body = path.contains("/source/")
                    ? "{\"type\":\"source\",\"connector\":{\"state\":\"RUNNING\",\"worker_id\":\"worker1:8083\"},\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"},{\"id\":1,\"state\":\"FAILED\",\"trace\":\"error trace\"}]}"
                    : path.contains("/sink/") ? "{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"}]}"
                    : "{\"type\":\"source\",\"connector\":{\"state\":\"STOPPED\"},\"tasks\":[]}";
            else if (path.endsWith("/topics")) {
                if (path.contains("/sink/")) { status = 503; body = "{\"message\":\"tracking unavailable\"}"; }
                else body = path.contains("/source/") ? "{\"source\":{\"topics\":[\"orders\"]}}" : "{\"stopped\":{\"topics\":[]}}";
            } else if (path.endsWith("/restart") && rejectRestart) { status = 409; body = "{\"message\":\"rebalance in progress\"}"; }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length); exchange.getResponseBody().write(bytes); exchange.close();
        });
        server.start();
        Cluster cluster = new Cluster(); cluster.setId(id);
        ClusterRepository clusters = mock(ClusterRepository.class); when(clusters.findById(id)).thenReturn(Optional.of(cluster));
        RoleAuthenticationUtil roles = mock(RoleAuthenticationUtil.class); when(roles.canAccess(null, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)).thenReturn(admin);
        return new DataServicesController(clusters, mock(ExternalClusterRepository.class), mock(HostRepository.class), mapper,
                mock(DataServiceConnectionService.class), mock(EncryptionService.class), roles, mock(io.translab.tantor.server.audit.AuditService.class));
    }
}
