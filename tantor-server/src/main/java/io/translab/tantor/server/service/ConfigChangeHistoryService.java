package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.*;

/** Read-only history of submitted changes; step status distinguishes intent from successful application. */
@Service
@RequiredArgsConstructor
public class ConfigChangeHistoryService {
    private final JobRepository jobs;
    private final JobStepRepository steps;
    private final ObjectMapper mapper;
    public record Change(String key, String oldValue, String newValue, String previousSource) {}
    public record Node(String nodeId, String hostId, String role, String mode, String configPath,
                       String status, Instant startedAt, Instant completedAt, List<Change> changes) {}
    public record Version(long version, UUID jobId, String status, String requestedBy, Instant createdAt,
                          Instant completedAt, int retryCount, List<Node> nodes) {}
    public record History(List<Version> items, long total, int page, boolean hasMore) {}

    @Transactional(readOnly = true, isolation = org.springframework.transaction.annotation.Isolation.REPEATABLE_READ)
    public History history(UUID clusterId, int page) {
        if (page < 0 || page > 10000) throw new IllegalArgumentException("Invalid history page.");
        var result = jobs.findByResourceKeyAndType("cluster:" + clusterId, JobType.TARGETED_CONFIG_CHANGE,
                PageRequest.of(page, 20, Sort.by(Sort.Direction.DESC, "createdAt", "id")));
        List<Version> versions = new ArrayList<>();
        long ordinal = result.getTotalElements() - (long) page * 20;
        for (Job job : result) {
            List<Node> nodes = steps.findByJobIdOrderByStepOrderAsc(job.getId()).stream().map(this::node).toList();
            versions.add(new Version(ordinal--, job.getId(), job.getStatus().name(), job.getRequestedBy(), job.getCreatedAt(),
                    job.getEndTime(), job.getRetryCount() == null ? 0 : job.getRetryCount(), nodes));
        }
        return new History(versions, result.getTotalElements(), page, result.hasNext());
    }

    Node node(JobStep step) {
        Map<String, Object> payload;
        try { payload = mapper.readValue(step.getPayload(), new com.fasterxml.jackson.core.type.TypeReference<>() {}); }
        catch (Exception e) { return new Node(step.getTargetId(), null, null, "UNKNOWN", null, step.getStatus().name(), step.getStartTime(), step.getEndTime(), List.of()); }
        Map<?, ?> changes = payload.get("changes") instanceof Map<?, ?> batch ? batch : Collections.singletonMap(payload.get("key"), payload.get("value"));
        Map<?, ?> old = payload.get("expectedValues") instanceof Map<?, ?> batch ? batch : Collections.singletonMap(payload.get("key"), payload.get("expectedOldValue"));
        Map<?, ?> dynamic = payload.get("dynamicRemovalExpected") instanceof Map<?, ?> batch ? batch : Map.of();
        List<Change> values = new ArrayList<>();
        for (var entry : changes.entrySet()) {
            String key = string(entry.getKey());
            if (key == null || !KafkaPropertyCatalog.editableName(key)) continue;
            Object previous = old.get(key); String source = null;
            if (dynamic.get(key) instanceof Map<?, ?> override) {
                previous = override.get("value"); source = string(override.get("source"));
            }
            values.add(new Change(key, string(previous), string(entry.getValue()), source));
        }
        String mode = Boolean.TRUE.equals(payload.get("removal")) ? "DELETE" : Objects.toString(payload.get("operation"), "UNKNOWN");
        return new Node(Objects.toString(payload.getOrDefault("nodeId", payload.getOrDefault("brokerId", step.getTargetId()))),
                string(payload.get("hostId")), string(payload.get("role")), mode, string(payload.get("configPath")),
                step.getStatus().name(), step.getStartTime(), step.getEndTime(), values);
    }
    private static String string(Object value) { return value == null ? null : String.valueOf(value); }
}
