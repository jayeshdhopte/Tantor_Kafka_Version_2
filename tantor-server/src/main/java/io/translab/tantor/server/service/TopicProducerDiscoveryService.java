package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class TopicProducerDiscoveryService {
    private static final int MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024;
    private static final int MAX_RESULTS = 500;
    private final Path directory;
    private final long maxAgeSeconds;
    private final ClusterRepository clusters;
    private final ExternalClusterRepository externalClusters;
    private final ObjectMapper mapper;

    public TopicProducerDiscoveryService(
            @Value("${tantor.client-discovery.snapshot-dir:}") String directory,
            @Value("${tantor.client-discovery.max-age-seconds:1800}") long maxAgeSeconds,
            ClusterRepository clusters, ExternalClusterRepository externalClusters, ObjectMapper mapper) {
        this.directory = directory.isBlank() ? null : Path.of(directory).toAbsolutePath().normalize();
        this.maxAgeSeconds = Math.max(1, maxAgeSeconds);
        this.clusters = clusters;
        this.externalClusters = externalClusters;
        this.mapper = mapper;
    }

    public Snapshot get(UUID clusterId, String topic) {
        if (directory == null) return Snapshot.unavailable();
        String kafkaId = externalClusters.findById(clusterId)
                .filter(cluster -> !"DELETED".equalsIgnoreCase(cluster.getStatus()))
                .map(ExternalCluster::getKafkaClusterId)
                .or(() -> clusters.findById(clusterId)
                        .filter(cluster -> !"DELETED".equalsIgnoreCase(cluster.getStatus()))
                        .map(Cluster::getKafkaClusterId))
                .orElse("");
        if (kafkaId.isBlank()) return Snapshot.unavailable();

        Path file = directory.resolve(clusterId + ".json");
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) return Snapshot.unavailable();
        try (var stream = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            byte[] raw = stream.readNBytes(MAX_SNAPSHOT_BYTES + 1);
            if (raw.length > MAX_SNAPSHOT_BYTES) return Snapshot.unavailable();
            return read(mapper.readTree(raw), clusterId, kafkaId, topic, Instant.now());
        } catch (IOException | DateTimeParseException e) {
            return Snapshot.unavailable();
        }
    }

    private Snapshot read(JsonNode root, UUID clusterId, String kafkaId, String topic, Instant now) {
        if (root == null || root.path("schema_version").asInt() != 1
                || !clusterId.toString().equals(text(root, "cluster_id"))
                || !kafkaId.equals(text(root, "kafka_cluster_id"))) return Snapshot.unavailable();

        Instant captured = timestamp(text(root, "snapshot_timestamp"));
        JsonNode coverage = root.path("telemetry_coverage");
        int total = coverage.path("brokers_total").asInt(-1);
        int available = coverage.path("brokers_available").asInt(-1);
        JsonNode rows = root.path("producers");
        if (captured == null || captured.isAfter(now.plusSeconds(60)) || !rows.isArray()
                || total <= 0 || available < 0 || available > total) return Snapshot.unavailable();

        Map<Client, Producer> producers = new HashMap<>();
        for (JsonNode row : rows) {
            if (!topic.equals(text(row, "topic"))
                    || !"PRODUCER".equals(text(row, "client_type"))
                    || !"CONFIRMED".equals(text(row, "mapping_confidence"))
                    || !"broker_request_telemetry".equals(text(row, "evidence_source"))) continue;

            String ip = normalizeIp(text(row, "client_ip"));
            String clientId = text(row, "client_id");
            Instant seen = timestamp(text(row, "last_seen"));
            if (ip == null || clientId.isBlank() || clientId.length() > 255
                    || seen == null || seen.isAfter(captured.plusSeconds(60))) continue;

            Producer producer = new Producer(ip, clientId, seen);
            producers.merge(new Client(ip, clientId), producer,
                    (left, right) -> left.lastSeen().isAfter(right.lastSeen()) ? left : right);
        }

        String status = captured.isBefore(now.minusSeconds(maxAgeSeconds)) ? "STALE"
                : available == total ? "AVAILABLE" : "PARTIAL";
        List<Producer> result = producers.values().stream()
                .sorted(Comparator.comparing(Producer::lastSeen).reversed()
                        .thenComparing(Producer::clientIp).thenComparing(Producer::clientId))
                .limit(MAX_RESULTS).toList();
        return new Snapshot(status, captured, available, total, result, producers.size() > result.size());
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.textValue() : "";
    }

    private static Instant timestamp(String value) {
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static String normalizeIp(String value) {
        // Validate literal addresses without DNS lookups.
        if (value.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}")) {
            for (String part : value.split("\\.")) {
                if (Integer.parseInt(part) > 255 || (part.length() > 1 && part.startsWith("0"))) return null;
            }
            return value;
        }
        if (value.length() > 45 || !value.contains(":") || !value.matches("[0-9a-fA-F:.]+")) return null;
        try {
            return InetAddress.getByName(value).getHostAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    private record Client(String ip, String id) {}
    public record Producer(String clientIp, String clientId, Instant lastSeen) {}
    public record Snapshot(String status, Instant capturedAt, Integer brokersAvailable, Integer brokersTotal,
                           List<Producer> producers, boolean truncated) {
        private static Snapshot unavailable() {
            return new Snapshot("UNAVAILABLE", null, null, null, List.of(), false);
        }
    }
}
