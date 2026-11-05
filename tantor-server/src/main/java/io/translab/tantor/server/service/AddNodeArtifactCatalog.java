package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

@Service @RequiredArgsConstructor
public class AddNodeArtifactCatalog {
    private final ArtifactRepositoryProperties properties;
    private final ObjectMapper mapper;
    public record Artifact(String id, String version, String checksum) {}

    public Artifact resolve(String version, String pinnedId) {
        return resolveType("KAFKA",version,pinnedId);
    }
    public Artifact resolveType(String type,String version,String pinnedId) {
        if(!Set.of("KAFKA","JMX_EXPORTER").contains(type))throw new IllegalArgumentException("Unsupported Add Node artifact type.");
        try {
            boolean pinned = pinnedId != null && !pinnedId.isBlank();
            String path = pinned ? "/api/v1/artifacts/" + UUID.fromString(pinnedId) : "/api/v1/artifacts?serviceType="+type+"&size=1000";
            var uri = URI.create(properties.getInternalUrl().toString().replaceAll("/+$", "") + path);
            var response = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build().send(
                    HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) throw new IllegalStateException("Approved artifact catalog is unavailable.");
            JsonNode root = mapper.readTree(response.body());
            JsonNode rows = pinned ? mapper.createArrayNode().add(root) : root.isArray() ? root : root.path("content");
            if (!rows.isArray() || root.path("totalElements").asInt(rows.size()) > rows.size())
                throw new IllegalStateException("Artifact catalog response is incomplete; pin the approved artifact.");
            var matches = new ArrayList<Artifact>();
            for (JsonNode row : rows) {
                String hash = row.path("sha256").asText(row.path("checksum").asText());
                if (type.equals(row.path("serviceType").asText()) && "AVAILABLE".equals(row.path("status").asText())
                        && (version==null||Objects.equals(version, row.path("version").asText())) && hash.matches("(?i)[a-f0-9]{64}")
                        && (pinnedId == null || pinnedId.isBlank() || pinnedId.equals(row.path("id").asText()))) {
                    matches.add(new Artifact(row.path("id").asText(), row.path("version").asText(), hash.toLowerCase(Locale.ROOT)));
                }
            }
            if (matches.size() != 1) throw new IllegalStateException("A unique available Kafka artifact with SHA-256 must match this cluster version; pin artifact_id when ambiguous.");
            return matches.getFirst();
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Artifact observation interrupted.", e); }
        catch (java.io.IOException e) { throw new IllegalStateException("Artifact metadata could not be read.", e); }
    }
}
