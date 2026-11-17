package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequiredArgsConstructor
public class ClusterLiveLogsController {
    private static final int MAX_BYTES = 512 * 1024;
    private static final Map<String, String> UNITS = Map.of(
            "broker", "broker", "controller", "controller", "broker_controller", "kafka",
            "broker_zookeeper", "kafka");
    private final ClusterRepository clusters;
    private final ClusterServiceAssignmentRepository assignments;
    private final ObjectMapper mapper;
    private final HostRepository hosts;
    private final Map<String, Pending> pending = new ConcurrentHashMap<>();

    // Each agent receives only its own distinct 64-character random hex token.
    @Value("${tantor.live-logs.agent-tokens:${TANTOR_LIVE_LOGS_AGENT_TOKENS:}}")
    private String tokens;

    record Source(UUID id, String hostId, String role, String hostname, Integer nodeId) {}
    record Query(UUID id, String unit, String from, String to) {}
    record Result(String journal, String error) {}
    private record Pending(Query query, CompletableFuture<Result> result, AtomicBoolean claimed) {}

    @GetMapping("/api/v1/ui/clusters/{clusterId}/live-logs/sources")
    public ResponseEntity<List<Source>> sources(@PathVariable UUID clusterId) {
        if (!internal(clusterId)) return ResponseEntity.notFound().build();
        var services = assignments.findByClusterId(clusterId).stream().filter(this::active).toList();
        var names = new java.util.HashMap<String, String>();
        hosts.findAllById(services.stream().map(ClusterServiceAssignment::getHostId).distinct().toList())
                .forEach(host -> names.put(host.getId(), host.getHostname()));
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(services.stream()
                .map(a -> new Source(a.getId(), a.getHostId(), a.getRole(), names.get(a.getHostId()), a.getNodeId()))
                .toList());
    }

    @GetMapping("/api/v1/ui/clusters/{clusterId}/live-logs")
    public CompletableFuture<ResponseEntity<?>> read(@PathVariable UUID clusterId, @RequestParam UUID source,
            @RequestParam Instant from, @RequestParam(required = false) Instant to) {
        Instant end = to == null ? Instant.now() : to;
        if (from.isAfter(end) || Duration.between(from, end).compareTo(Duration.ofDays(7)) > 0) {
            return CompletableFuture.completedFuture(error(400, "Choose a valid log range of up to seven days."));
        }
        if (!internal(clusterId)) return CompletableFuture.completedFuture(error(404, "Internal cluster not found."));
        var assignment = assignments.findById(source).filter(a -> active(a) && clusterId.equals(a.getCluster().getId()));
        if (assignment.isEmpty()) return CompletableFuture.completedFuture(error(404, "Log source not found."));
        var a = assignment.get();
        String host = a.getHostId();
        if (tokenFor(host).isEmpty()) return CompletableFuture.completedFuture(error(503, "Live logs are not configured for this agent."));
        Pending request = new Pending(new Query(UUID.randomUUID(), unit(a), from.toString(), end.toString()),
                new CompletableFuture<>(), new AtomicBoolean());
        if (pending.putIfAbsent(host, request) != null) {
            return CompletableFuture.completedFuture(error(429, "This agent already has a log request. Try again shortly."));
        }
        return request.result().orTimeout(12, TimeUnit.SECONDS).handle((result, failure) -> {
            pending.remove(host, request);
            if (failure != null) return error(504, "Agent did not return logs. Check its connection and live logs configuration.");
            if (result.error() != null) return error(502, "The agent could not read this service journal.");
            return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(result);
        });
    }

    @GetMapping("/api/v1/agents/{host}/live-logs")
    public ResponseEntity<?> claim(@PathVariable String host,
            @RequestHeader(value = "X-Tantor-Log-Token", defaultValue = "") String token) {
        if (!authorized(host, token)) return ResponseEntity.status(401).build();
        Pending request = pending.get(host);
        if (request == null || !request.claimed().compareAndSet(false, true)) return ResponseEntity.noContent().build();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(request.query());
    }

    @PostMapping("/api/v1/agents/{host}/live-logs/{queryId}")
    public ResponseEntity<Void> complete(@PathVariable String host, @PathVariable UUID queryId,
            @RequestHeader(value = "X-Tantor-Log-Token", defaultValue = "") String token,
            jakarta.servlet.http.HttpServletRequest body) throws java.io.IOException {
        if (!authorized(host, token)) return ResponseEntity.status(401).build();
        Pending request = pending.get(host);
        if (request == null || !request.claimed().get() || !request.query().id().equals(queryId)) {
            return ResponseEntity.notFound().build();
        }
        byte[] bytes = body.getInputStream().readNBytes(MAX_BYTES + 1);
        if (bytes.length > MAX_BYTES) return ResponseEntity.status(413).build();
        request.result().complete(new Result(new String(bytes, StandardCharsets.UTF_8),
                "true".equals(body.getHeader("X-Tantor-Log-Error")) ? "JOURNAL_READ_FAILED" : null));
        return ResponseEntity.noContent().build();
    }

    private boolean internal(UUID id) {
        return clusters.findById(id).filter(c -> !"EXTERNAL".equalsIgnoreCase(c.getMode())
                && !"DELETED".equalsIgnoreCase(c.getStatus())).isPresent();
    }

    private boolean active(ClusterServiceAssignment a) {
        return a.getRole() != null && UNITS.containsKey(a.getRole()) && !"DELETED".equalsIgnoreCase(a.getStatus());
    }

    private String unit(ClusterServiceAssignment assignment) {
        try {
            if (assignment.getConfigJson() != null) {
                var config = mapper.readTree(assignment.getConfigJson());
                if (config == null) return UNITS.get(assignment.getRole()) + ".service";
                String name = config.path("systemd_service").asText(config.path("service_name").asText(""));
                if (name.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,79}(\\.service)?")) {
                    return name.endsWith(".service") ? name : name + ".service";
                }
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException ignored) {
            // Older assignments may have no usable service configuration.
        }
        return UNITS.get(assignment.getRole()) + ".service";
    }

    private String tokenFor(String host) {
        try {
            var config = mapper.readTree(tokens);
            String token = config == null ? "" : config.path(host).asText("");
            return token.matches("[a-f0-9]{64}") ? token : "";
        } catch (com.fasterxml.jackson.core.JsonProcessingException ignored) {
            return "";
        }
    }

    private boolean authorized(String host, String provided) {
        String expected = tokenFor(host);
        return !expected.isEmpty() && provided.length() == 64 && MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8));
    }

    private ResponseEntity<?> error(int status, String message) {
        return ResponseEntity.status(status).cacheControl(CacheControl.noStore()).body(Map.of("message", message));
    }
}
