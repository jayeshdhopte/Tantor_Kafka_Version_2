package io.translab.tantor.server.web;

import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

/** Agent-facing artifact download proxy for control-plane URLs in every profile. */
@RestController
public class LocalArtifactDownloadController {
    private final ArtifactRepositoryProperties repository;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER).build();

    public LocalArtifactDownloadController(ArtifactRepositoryProperties repository) {
        this.repository = repository;
    }

    @GetMapping("/api/v1/artifacts/{id}/download")
    public void download(@PathVariable UUID id, HttpServletResponse response) throws IOException {
        String base = repository.getInternalUrl().toString().replaceAll("/+$", "");
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/api/v1/artifacts/" + id + "/download"))
                .timeout(Duration.ofSeconds(60)).GET().build();
        HttpResponse<InputStream> upstream;
        try {
            upstream = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            response.sendError(502, "Artifact repository request interrupted");
            return;
        } catch (IOException e) {
            response.sendError(502, "Artifact repository unavailable");
            return;
        }
        try (InputStream body = upstream.body()) {
            if (upstream.statusCode() != 200) {
                response.sendError(upstream.statusCode() == 404 ? 404 : 502, "Artifact download unavailable");
                return;
            }
            response.setStatus(200);
            response.setContentType("application/octet-stream");
            response.setHeader("Content-Disposition", "attachment; filename=\"" + id + ".tgz\"");
            upstream.headers().firstValueAsLong("Content-Length").ifPresent(response::setContentLengthLong);
            body.transferTo(response.getOutputStream());
        }
    }
}
