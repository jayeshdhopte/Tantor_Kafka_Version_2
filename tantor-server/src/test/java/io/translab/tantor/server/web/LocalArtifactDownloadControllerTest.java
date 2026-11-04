package io.translab.tantor.server.web;

import com.sun.net.httpserver.HttpServer;
import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LocalArtifactDownloadControllerTest {
    @Test
    void streamsExactArtifactBytesThroughConfiguredRepository() throws Exception {
        check(200, 200);
    }

    @Test
    void preservesMissingArtifactStatus() throws Exception {
        check(404, 404);
    }

    @Test void preservesRejectedTaskContextStatus() throws Exception {check(403,403);}

    @Test
    void refusesRepositoryRedirects() throws Exception {
        check(302, 502);
    }

    private void check(int upstreamStatus, int expected) throws Exception {
        UUID id = UUID.randomUUID();
        byte[] bytes = "artifact-test-payload".getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/artifacts/" + id + "/download", exchange -> {
            assertEquals("Bearer forwarded-identity",exchange.getRequestHeaders().getFirst("Authorization"));
            assertEquals("downloadToken=11111111-1111-4111-8111-111111111111",exchange.getRequestURI().getRawQuery());
            exchange.getResponseHeaders().add("Location", "http://127.0.0.1:1/untrusted");
            exchange.sendResponseHeaders(upstreamStatus, bytes.length);
            try (var out = exchange.getResponseBody()) { out.write(bytes); }
        });
        server.start();
        try {
            var properties = new ArtifactRepositoryProperties();
            properties.setInternalUrl(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            var response = new MockHttpServletResponse();
            var incoming=new org.springframework.mock.web.MockHttpServletRequest();
            incoming.addHeader("Authorization","Bearer forwarded-identity");
            incoming.addParameter("downloadToken","11111111-1111-4111-8111-111111111111");
            new LocalArtifactDownloadController(properties).download(id, incoming, response);
            assertEquals(expected, response.getStatus());
            assertNull(response.getHeader("Location"));
            if (expected == 200) {
                assertArrayEquals(bytes, response.getContentAsByteArray());
                assertEquals(bytes.length, response.getContentLength());
            }
        } finally { server.stop(0); }
    }
}
