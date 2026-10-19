package io.translab.tantor.server.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/** Physical segment counts from broker JMX; never estimated from bytes or offsets. */
@Service
@RequiredArgsConstructor
public class TopicSegmentMetricsService {
    private final PrometheusMonitoringService monitoring;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final Map<String, CachedScrape> cache = new ConcurrentHashMap<>();
    private static final long TTL_NANOS = Duration.ofSeconds(10).toNanos();
    private static final Pattern SAMPLE = Pattern.compile("^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\\{(.*)})?\\s+([^\\s]+)(?:\\s+.*)?$");
    private static final Pattern LABEL = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"((?:\\\\.|[^\"\\\\])*)\"");

    public record SegmentCount(Long count, String unavailableReason) {}
    private record CachedScrape(long started, CompletableFuture<String> text) {}

    public SegmentCount segmentCount(UUID clusterId, TopicDescription topic) {
        try {
            Map<Integer, String> targets = monitoring.brokerJmxTargets(clusterId);
            Map<Integer, CompletableFuture<String>> scrapes = new HashMap<>();
            for (var partition : topic.partitions()) {
                if (partition.leader() == null || partition.leader().id() < 0)
                    return unavailable("A partition has no available leader.");
                int broker = partition.leader().id();
                String address = targets.get(broker);
                if (address == null) return unavailable("Broker JMX exporter is not configured for every partition leader.");
                scrapes.computeIfAbsent(broker, ignored -> scrape(address));
            }
            Map<Integer, Map<Integer, Long>> values = new HashMap<>();
            scrapes.forEach((broker, future) -> values.put(broker, parseSegmentCounts(future.join(), topic.name())));
            long total = 0;
            for (var partition : topic.partitions()) {
                Long count = values.get(partition.leader().id()).get(partition.partition());
                if (count == null) return unavailable("Segment metrics are unavailable for one or more partition leaders. Check broker JMX exporter reachability and NumLogSegments metrics.");
                total = Math.addExact(total, count);
            }
            return new SegmentCount(total, null);
        } catch (RuntimeException ignored) {
            return unavailable("Unable to read broker segment metrics. Check broker JMX exporter configuration and reachability.");
        }
    }

    private SegmentCount unavailable(String reason) { return new SegmentCount(null, reason); }

    private CompletableFuture<String> scrape(String address) {
        long now = System.nanoTime();
        cache.entrySet().removeIf(entry -> now - entry.getValue().started() >= TTL_NANOS);
        if (cache.size() >= 256 && !cache.containsKey(address)) return request(address);
        return cache.computeIfAbsent(address, ignored -> new CachedScrape(now, request(address))).text();
    }

    private CompletableFuture<String> request(String address) {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + address + "/metrics"))
                .timeout(Duration.ofSeconds(3)).GET().build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> response.statusCode() == 200 ? response.body() : "")
                .exceptionally(ignored -> "");
    }

    static Map<Integer, Long> parseSegmentCounts(String text, String topic) {
        Map<Integer, Long> legacy = new HashMap<>(), labelled = new HashMap<>();
        // Older Tantor exporters include topic/partition in the metric name.
        String safeTopic = topic.replaceAll("[^a-zA-Z0-9_:]", "_").replaceAll("_+", "_").replaceAll("^_|_$", "");
        Pattern legacyName = Pattern.compile("(?i:kafka_log_log_numlogsegments_topic_)" + Pattern.quote(safeTopic) + "(?i:_partition_([0-9]+)_value)");
        for (String line : text.split("\\R")) {
            var sample = SAMPLE.matcher(line);
            if (!sample.matches()) continue;
            String name = sample.group(1).toLowerCase(Locale.ROOT);
            try {
                double number = Double.parseDouble(sample.group(3));
                if (!Double.isFinite(number) || number < 0 || number >= Long.MAX_VALUE || number != Math.rint(number)) continue;
                Map<String, String> labels = new HashMap<>();
                var label = LABEL.matcher(Objects.toString(sample.group(2), ""));
                while (label.find()) labels.put(label.group(1), label.group(2));
                boolean segmentMetric = name.equals("kafka_log_log_numlogsegments") || name.equals("kafka_log_log_numlogsegments_value")
                        || (name.equals("kafka_log_log_value") && "NumLogSegments".equals(labels.get("name")));
                if (segmentMetric && topic.equals(labels.get("topic")) && labels.containsKey("partition")) {
                    labelled.put(Integer.parseInt(labels.get("partition")), (long) number);
                } else if (sample.group(2) == null) {
                    var match = legacyName.matcher(sample.group(1));
                    if (match.matches()) legacy.put(Integer.parseInt(match.group(1)), (long) number);
                }
            } catch (NumberFormatException ignored) { /* Malformed samples are unavailable, never zero. */ }
        }
        legacy.putAll(labelled); // Labelled metrics take precedence; never double count.
        return legacy;
    }
}
