package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.JobRepository;
import io.translab.tantor.server.repository.JobStepRepository;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

/** Read-only recovery eligibility. History labels are not durable configuration identities. */
@RequiredArgsConstructor
public final class ConfigRollbackPolicy {
    private final JobRepository jobs;
    private final JobStepRepository steps;
    private final ObjectMapper mapper;

    public record Eligibility(boolean allowed, String reason, UUID conflictingJobId) {}
    private static final List<JobType> CONFIG_TYPES = List.of(
            JobType.CONFIG_CHANGE, JobType.TARGETED_CONFIG_CHANGE, JobType.ROLLING_CONFIG_UPDATE);

    public static boolean applies(Job job) {
        return job.getType() == JobType.CONFIG_CHANGE || job.getType() == JobType.TARGETED_CONFIG_CHANGE;
    }

    public Eligibility evaluate(Job job) {
        if (!applies(job)) return allowed();
        if (job.getCreatedAt() == null || job.getResourceKey() == null
                || !job.getResourceKey().startsWith("cluster:")) return unverifiable();
        try {
            List<JobStep> original = steps.findByJobIdOrderByStepOrderAsc(job.getId());
            if (original.isEmpty()) return unverifiable();
            List<Scope> recovery = new ArrayList<>();
            for (JobStep step : original) {
                if (step.getStatus() == JobStepStatus.ROLLED_BACK) continue;
                Map<String, Object> payload = read(step.getPayload());
                if (Boolean.FALSE.equals(payload.get("configMutationStarted"))) continue;
                recovery.add(scope(step, payload));
            }
            if (recovery.isEmpty()) return allowed();
            List<Job> candidates = jobs.findConfigRollbackCandidates(job.getResourceKey(), job.getId(),
                    CONFIG_TYPES, job.getCreatedAt());
            Map<UUID, List<JobStep>> byJob = candidates.isEmpty() ? Map.of()
                    : steps.findByJob_IdIn(candidates.stream().map(Job::getId).toList()).stream()
                    .collect(Collectors.groupingBy(step -> step.getJob().getId()));
            for (Job newer : candidates) {
                if (newer.getStatus() == JobStatus.ROLLED_BACK || newer.getStatus() == JobStatus.PENDING) continue;
                List<JobStep> newerSteps = byJob.getOrDefault(newer.getId(), List.of());
                if (newerSteps.isEmpty() && newer.getStatus() == JobStatus.SUCCESS) return unverifiable();
                for (JobStep step : newerSteps) {
                    if (step.getStatus() == JobStepStatus.ROLLED_BACK) continue;
                    boolean rolling = newer.getType() == JobType.ROLLING_CONFIG_UPDATE;
                    if (rolling && (step.getName() == null || !step.getName().startsWith("WRITE_CONFIG"))) continue;
                    Map<String, Object> payload = rolling ? rollingPayload(newer, step) : read(step.getPayload());
                    if (Boolean.FALSE.equals(payload.get("configMutationStarted"))) continue;
                    if (step.getStatus() != JobStepStatus.SUCCESS
                            && !Boolean.TRUE.equals(payload.get("configMutationStarted"))) continue;
                    Scope changed = scope(step, payload);
                    if (recovery.stream().anyMatch(target -> overlaps(target, changed))) {
                        return new Eligibility(false, "A newer configuration change (job " + newer.getId()
                                + ") affects this job's recovery targets. Retry rollback is unavailable. "
                                + "Review the current configuration and create a new recovery plan.", newer.getId());
                    }
                }
            }
            return allowed();
        } catch (IllegalArgumentException ex) {
            return unverifiable();
        }
    }

    private Map<String, Object> rollingPayload(Job job, JobStep step) {
        Map<String, Object> root = read(job.getPayload());
        if (root.get("changes") instanceof List<?> changes) {
            for (Object value : changes) {
                if (!(value instanceof Map<?, ?> change)
                        || !Objects.equals(text(change.get("host")), text(step.getTargetId()))) continue;
                Map<String, Object> result = new HashMap<>();
                result.put("hostId", change.get("host"));
                result.put("nodeId", change.get("nodeId"));
                result.put("role", root.get("role"));
                result.put("configPath", change.get("configFilePath"));
                Map<String, Object> properties = new HashMap<>();
                if (change.get("properties") instanceof List<?> entries) {
                    for (Object entry : entries) {
                        if (entry instanceof Map<?, ?> property && text(property.get("key")) != null)
                            properties.put(text(property.get("key")), property.get("newValue"));
                    }
                }
                result.put("changes", properties);
                return result;
            }
        }
        throw new IllegalArgumentException("Missing rolling configuration scope");
    }

    private Eligibility allowed() { return new Eligibility(true, "", null); }
    private Eligibility unverifiable() {
        return new Eligibility(false, "Cannot verify this configuration rollback against the current history. "
                + "Refresh and review the current configuration before creating a new recovery plan.", null);
    }

    private Map<String, Object> read(String json) {
        try {
            if (json == null || json.isBlank()) throw new IllegalArgumentException("Missing configuration scope");
            Map<String, Object> result = mapper.readValue(json, new TypeReference<>() {});
            if (result == null || result.isEmpty()) throw new IllegalArgumentException("Missing configuration scope");
            return result;
        } catch (java.io.IOException ex) { throw new IllegalArgumentException("Invalid configuration scope", ex); }
    }

    private Scope scope(JobStep step, Map<String, Object> p) {
        String host = text(p.get("hostId"));
        if (host == null && text(step.getTargetId()) != null && !step.getTargetId().matches("\\d+"))
            host = text(step.getTargetId());
        String node = text(p.getOrDefault("nodeId", p.get("brokerId")));
        String role = text(p.get("role"));
        if (role != null) role = role.toLowerCase(Locale.ROOT).replace('+', '_');
        String operation = Objects.toString(p.get("operation"), "");
        boolean dynamic = operation.equalsIgnoreCase("DYNAMIC") || p.containsKey("brokerId");
        Set<String> keys = new HashSet<>();
        addKeys(keys, p.get("changes"));
        addKeys(keys, p.get("expected"));
        addKeys(keys, p.get("expectedValues"));
        if (p.get("recoveryCheckpoint") instanceof Map<?, ?> checkpoint) {
            addKeys(keys, checkpoint.get("file"));
            addKeys(keys, checkpoint.get("dynamic"));
            if (checkpoint.get("dynamic") instanceof Map<?, ?> overrides && !overrides.isEmpty()) dynamic = true;
        }
        for (String field : List.of("key", "configKey")) {
            String key = text(p.get(field));
            if (key != null) keys.add(key);
        }
        // Missing keys mean a legacy whole-file change: treat them as a wildcard.
        return new Scope(host, node, role, text(p.get("configPath")), dynamic, keys);
    }

    private void addKeys(Set<String> keys, Object value) {
        if (value instanceof Map<?, ?> map) map.keySet().forEach(key -> keys.add(String.valueOf(key)));
    }
    private String text(Object value) {
        if (value == null) return null;
        String result = String.valueOf(value).trim();
        return result.isEmpty() || result.equals("null") ? null : result;
    }
    private boolean overlaps(Scope a, Scope b) {
        // Legacy jobs may identify the same host by hostname rather than the inventory UUID.
        // A shared Kafka node identity must not be treated as a different recovery target.
        if (a.node != null && b.node != null) {
            if (different(a.node, b.node)) return false;
        } else if (different(a.host, b.host)) return false;
        if (different(a.role, b.role) && !combined(a.role) && !combined(b.role)) return false;
        if (!a.dynamic && !b.dynamic && different(a.path, b.path)) return false;
        return a.keys.isEmpty() || b.keys.isEmpty() || !Collections.disjoint(a.keys, b.keys);
    }
    private boolean different(String a, String b) { return a != null && b != null && !a.equals(b); }
    private boolean combined(String role) { return role != null && role.contains("broker") && role.contains("controller"); }
    private record Scope(String host, String node, String role, String path, boolean dynamic, Set<String> keys) {}
}
