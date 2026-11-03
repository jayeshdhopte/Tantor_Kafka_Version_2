package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.service.DataServiceConnectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class DataServicesControllerConnectorCreationRaceTest {
    @ParameterizedTest
    @CsvSource({"source,false", "sink,false", "source,true", "sink,true"})
    void successfulCreationSurvivesStatusPropagationLagAndHealthyConnectorsRemainVisible(String type, boolean external) throws Exception {
        try (var fixture = new Fixture(type, external)) {
            fixture.missingStatus = 1;
            fixture.mvc.perform(fixture.target(post(fixture.base + "/connectors"))
                    .contentType("application/json").content("{\"name\":\"new\",\"config\":{\"connector.class\":\"FileStream\"}}"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.name").value("new"));
            fixture.mvc.perform(fixture.target(get(fixture.base + "/summary")))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.connectorCount").value(2))
                    .andExpect(jsonPath("$.runningConnectors").value(1))
                    .andExpect(jsonPath("$.connectors[1].name").value("new"))
                    .andExpect(jsonPath("$.connectors[1].state").value("INITIALIZING"))
                    .andExpect(jsonPath("$.connectors[1].initializing").value(true));
            fixture.mvc.perform(fixture.target(get(fixture.base + "/summary")))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.runningConnectors").value(2))
                    .andExpect(jsonPath("$.runningTasks").value(2))
                    .andExpect(jsonPath("$.connectors[1].state").value("RUNNING"))
                    .andExpect(jsonPath("$.connectors[1].type").value(type.toUpperCase()));
            assertThat(fixture.posts.get()).isEqualTo(1);
        }
    }

    @Test void configLagInTheListedInventoryDoesNotFailTheWholeSummary() throws Exception {
        try (var fixture = new Fixture("sink", false)) {
            fixture.created = true; fixture.missingConfig = 1;
            fixture.mvc.perform(fixture.target(get(fixture.base + "/summary")))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.connectorCount").value(2))
                    .andExpect(jsonPath("$.connectors[0].state").value("RUNNING"))
                    .andExpect(jsonPath("$.connectors[1].state").value("INITIALIZING"));
            fixture.mvc.perform(fixture.target(get(fixture.base + "/summary")))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.runningTasks").value(2));
        }
    }

    @ParameterizedTest @CsvSource({"config,403", "status,503"})
    void genuineAuthorizationAndServiceFailuresAreStillReported(String suffix, int code) throws Exception {
        try (var fixture = new Fixture("source", false)) {
            fixture.created = true; fixture.forcedSuffix = suffix; fixture.forcedCode = code;
            fixture.mvc.perform(fixture.target(get(fixture.base + "/summary")))
                    .andExpect(status().is(code)).andExpect(jsonPath("$.backendStatus").value(code));
        }
    }

    @Test void aDirectRequestForAnUnknownConnectorStillReturns404() throws Exception {
        try (var fixture = new Fixture("source", false)) {
            fixture.mvc.perform(fixture.target(get(fixture.base + "/connectors/missing/details")))
                    .andExpect(status().isNotFound()).andExpect(jsonPath("$.backendStatus").value(404));
        }
    }

    private static class Fixture implements AutoCloseable {
        final HttpServer server;
        final MockMvc mvc;
        final String base;
        final AtomicInteger posts = new AtomicInteger(), configReads = new AtomicInteger(), statusReads = new AtomicInteger();
        volatile boolean created;
        int missingConfig, missingStatus, forcedCode;
        String forcedSuffix = "";

        Fixture(String type, boolean external) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                String path = exchange.getRequestURI().getPath(), body = "{}";
                int code = 200;
                if (path.equals("/")) body = "{\"version\":\"4.3.0\"}";
                else if (path.equals("/connector-plugins")) body = "[{\"class\":\"FileStream\",\"type\":\"" + type + "\"}]";
                else if (path.equals("/connectors")) {
                    if (exchange.getRequestMethod().equals("POST")) {
                        created = true; posts.incrementAndGet(); code = 201;
                        body = "{\"name\":\"new\",\"config\":{\"connector.class\":\"FileStream\"},\"type\":\"" + type + "\"}";
                    } else body = created ? "[\"healthy\",\"new\"]" : "[\"healthy\"]";
                } else if (path.startsWith("/connectors/healthy/")) {
                    body = path.endsWith("/config") ? "{\"connector.class\":\"FileStream\"}"
                            : path.endsWith("/status") ? "{\"type\":\"source\",\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"}]}"
                            : "{\"healthy\":{\"topics\":[]}}";
                } else if (path.startsWith("/connectors/new/") && created) {
                    if (forcedCode > 0 && path.endsWith("/" + forcedSuffix)) code = forcedCode;
                    else if (path.endsWith("/config")) {
                        if (configReads.getAndIncrement() < missingConfig) code = 404;
                        else body = "{\"connector.class\":\"FileStream\"}";
                    } else if (path.endsWith("/status")) {
                        if (statusReads.getAndIncrement() < missingStatus) code = 404;
                        else body = "{\"type\":\"" + type + "\",\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"}]}";
                    } else body = "{\"new\":{\"topics\":[]}}";
                } else code = 404;
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(code, bytes.length);
                exchange.getResponseBody().write(bytes); exchange.close();
            });
            server.start();
            UUID id = UUID.randomUUID(); base = "/api/v1/clusters/" + id + "/data-services/kafka-connect";
            var clusters = mock(ClusterRepository.class); var externalClusters = mock(ExternalClusterRepository.class);
            if (external) {
                var cluster = new ExternalCluster(); cluster.setId(id);
                when(externalClusters.findById(id)).thenReturn(Optional.of(cluster));
            } else {
                var cluster = new Cluster(); cluster.setId(id);
                when(clusters.findById(id)).thenReturn(Optional.of(cluster));
            }
            var roles = mock(RoleAuthenticationUtil.class);
            when(roles.canAccess(null, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)).thenReturn(true);
            mvc = MockMvcBuilders.standaloneSetup(new DataServicesController(clusters, externalClusters,
                    mock(HostRepository.class), new ObjectMapper(), mock(DataServiceConnectionService.class),
                    mock(EncryptionService.class), roles, mock(AuditService.class))).build();
        }

        MockHttpServletRequestBuilder target(MockHttpServletRequestBuilder request) {
            return request.param("protocol", "http").param("ip", "127.0.0.1")
                    .param("port", String.valueOf(server.getAddress().getPort()));
        }
        public void close() { server.stop(0); }
    }
}
