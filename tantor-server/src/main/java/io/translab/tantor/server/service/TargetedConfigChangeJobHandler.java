package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Applies a property batch per node; failure stops the remaining targets. */
@Component
@RequiredArgsConstructor
public class TargetedConfigChangeJobHandler implements JobHandler {
    private final JobService jobs;
    private final KafkaAdminService kafka;
    private final RollingRestartHealthService health;
    private final DeploymentService deployment;
    private final AgentTaskAwaiter taskAwaiter;
    private final ObjectMapper mapper;

    @Value("${tantor.config-change.dynamic-verification-timeout-ms:30000}")
    private long dynamicVerificationTimeoutMs = 30000;

    @Value("${tantor.config-change.dynamic-verification-poll-ms:500}")
    private long dynamicVerificationPollMs = 500;

    @Override
    public boolean supports(JobType type) { return type == JobType.TARGETED_CONFIG_CHANGE; }

    @Override
    public void execute(Job job) {
        UUID clusterId = UUID.fromString(String.valueOf(read(job.getPayload()).get("clusterId")));
        for (JobStep step : jobs.getSteps(job.getId())) {
            if (step.getStatus() == JobStepStatus.SUCCESS) continue;
            Map<String, Object> payload = read(step.getPayload());
            ensureTaskSettled(payload.get("configMutationTaskId"));
            if ("STATIC".equals(payload.get("operation"))) {
                executeStatic(clusterId, step, payload);
                continue;
            }
            if (payload.get("changes") instanceof Map<?, ?>) {
                executeDynamicBatch(clusterId, step, payload);
                continue;
            }
            int brokerId = ((Number) payload.get("brokerId")).intValue();
            String key = String.valueOf(payload.get("key"));
            String desired = String.valueOf(payload.get("value"));
            String expected = String.valueOf(payload.get("expectedOldValue"));
            int count = ((Number) payload.get("expectedBrokerCount")).intValue();
            String checkpoint = "";
            try {
                jobs.startStep(step.getId());
                var snapshot = health.inspect(clusterId, count, Set.of());
                if (!snapshot.healthy()) throw new IllegalStateException("Health precheck failed: " + snapshot.failureReason());
                Map<?, ?> observed = currentEntry(clusterId, brokerId, key);
                String current = observed.get("value") == null ? null : String.valueOf(observed.get("value"));
                if (!desired.equals(current)) {
                    if (!expected.equals(current == null ? "" : current)) {
                        throw new IllegalStateException("Broker " + brokerId + " value changed since preview; refresh the plan.");
                    }
                }
                String hostId = String.valueOf(payload.getOrDefault("hostId", ""));
                String configPath = String.valueOf(payload.getOrDefault("configPath", ""));
                if (hostId.isBlank() || configPath.isBlank()) throw new IllegalStateException("Refresh the change plan: managed host and configuration path are required for backup.");
                Map<String, Object> dynamicSnapshot = new java.util.LinkedHashMap<>();
                dynamicSnapshot.put("mode", "DYNAMIC"); dynamicSnapshot.put("key", key); dynamicSnapshot.put("previous", observed);
                dynamicSnapshot.put("requestedValue", desired); dynamicSnapshot.put("brokerId", brokerId);
                dynamicSnapshot.put("clusterId", clusterId.toString()); dynamicSnapshot.put("capturedAt", java.time.Instant.now().toString());
                UUID backupTask = deployment.backupKafkaConfig(clusterId, hostId, String.valueOf(payload.get("role")),
                        String.valueOf(brokerId), String.valueOf(payload.get("configJson")), configPath, key, dynamicSnapshot, "dynamic-" + step.getId());
                jobs.attachAgentTask(step.getId(), backupTask);
                Task backup = taskAwaiter.await(backupTask);
                checkpoint = java.util.Objects.toString(backup.getLogOutput(), "");
                if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Interrupted before configuration change");
                // Agent round trips may take time; reject intervening changes rather than overwrite them.
                Map<?, ?> latestEntry = currentEntry(clusterId, brokerId, key);
                String latest = latestEntry.get("value") == null ? null : String.valueOf(latestEntry.get("value"));
                if (!java.util.Objects.equals(observed, latestEntry)) throw new IllegalStateException("Broker " + brokerId + " value changed while backing up; refresh the plan.");
                if (!desired.equals(latest)) {
                    jobs.checkpointConfigChange(step.getId(), Map.of("dynamic", Map.of(key, observed)));
                    jobs.markConfigMutation(step.getId());
                    kafka.alterBrokerConfig(clusterId, brokerId, key, desired);
                } else {
                    jobs.checkpointConfigChange(step.getId(), Map.of("untouched", true));
                }
                verifyDynamic(clusterId, brokerId, key, desired);
                jobs.completeStep(step.getId(), "Verified " + key + " on broker " + brokerId + ".\n" + backup.getLogOutput());
            } catch (Exception e) {
                jobs.failStep(step.getId(), e.getMessage() + (checkpoint.isBlank() ? "" : "\n" + checkpoint));
                throw e;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void executeDynamicBatch(UUID clusterId, JobStep step, Map<String, Object> payload) {
        int brokerId = ((Number) payload.get("brokerId")).intValue();
        Map<String, String> changes = (Map<String, String>) payload.get("changes");
        Map<String, Object> expected = (Map<String, Object>) payload.get("expectedValues");
        String checkpoint = "";
        try {
            jobs.startStep(step.getId());
            checkHealth(clusterId, ((Number) payload.get("expectedBrokerCount")).intValue(), String.valueOf(payload.get("hostId")));
            Map<String, Object> observed = batchEntries(clusterId, brokerId, changes.keySet());
            Map<String, String> pending = new java.util.LinkedHashMap<>();
            for (var entry : changes.entrySet()) {
                Object actual = ((Map<?, ?>) observed.get(entry.getKey())).get("value");
                if (java.util.Objects.equals(actual, entry.getValue())) continue;
                if (!java.util.Objects.equals(actual, expected.get(entry.getKey()))) throw new IllegalStateException("Broker " + brokerId + " property " + entry.getKey() + " changed since preview; refresh the plan.");
                pending.put(entry.getKey(), entry.getValue());
            }
            if (!pending.isEmpty()) {
                String hostId = String.valueOf(payload.getOrDefault("hostId", ""));
                String path = String.valueOf(payload.getOrDefault("configPath", ""));
                if (hostId.isBlank() || path.isBlank()) throw new IllegalStateException("Managed host and configuration path are required for backup.");
                for (Map<String, Object> snapshot : dynamicSnapshots(clusterId, brokerId, observed, changes)) {
                    UUID task = deployment.backupKafkaConfig(clusterId, hostId, String.valueOf(payload.get("role")), String.valueOf(brokerId),
                            String.valueOf(payload.get("configJson")), path, "batch", snapshot, "dynamic-" + step.getId());
                    jobs.attachAgentTask(step.getId(), task);
                    String detail = java.util.Objects.toString(taskAwaiter.await(task).getLogOutput(), "");
                    checkpoint += (checkpoint.isBlank() ? "" : "\n") + detail;
                }
                if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Interrupted before configuration change");
                if (!observed.equals(batchEntries(clusterId, brokerId, changes.keySet()))) throw new IllegalStateException("Broker values changed while backing up; refresh the plan.");
                jobs.checkpointConfigChange(step.getId(), Map.of("dynamic", observed));
                jobs.markConfigMutation(step.getId());
                kafka.alterBrokerConfigs(clusterId, brokerId, pending);
            } else {
                jobs.checkpointConfigChange(step.getId(), Map.of("untouched", true));
            }
            for (var entry : changes.entrySet()) verifyDynamic(clusterId, brokerId, entry.getKey(), entry.getValue());
            jobs.completeStep(step.getId(), "Verified " + changes.size() + " properties on broker " + brokerId + ".\n" + checkpoint);
        } catch (RuntimeException e) {
            jobs.failStep(step.getId(), e.getMessage() + (checkpoint.isBlank() ? "" : "\n" + checkpoint));
            throw e;
        }
    }

    // Existing agents validate key/mode and a 16 KiB snapshot limit. Keep every chunk compatible.
    java.util.List<Map<String, Object>> dynamicSnapshots(UUID clusterId, int brokerId,
                                                        Map<String, Object> observed, Map<String, String> changes) {
        var snapshots = new java.util.ArrayList<Map<String, Object>>();
        Map<String, Object> previous = new java.util.LinkedHashMap<>();
        Map<String, String> requested = new java.util.LinkedHashMap<>();
        for (var entry : changes.entrySet()) {
            previous.put(entry.getKey(), observed.get(entry.getKey())); requested.put(entry.getKey(), entry.getValue());
            if (snapshotSize(dynamicSnapshot(clusterId, brokerId, previous, requested)) > 16384) {
                previous.remove(entry.getKey()); requested.remove(entry.getKey());
                if (!requested.isEmpty()) snapshots.add(dynamicSnapshot(clusterId, brokerId, previous, requested));
                previous = new java.util.LinkedHashMap<>(); requested = new java.util.LinkedHashMap<>();
                previous.put(entry.getKey(), observed.get(entry.getKey())); requested.put(entry.getKey(), entry.getValue());
                if (snapshotSize(dynamicSnapshot(clusterId, brokerId, previous, requested)) > 16384)
                    throw new IllegalStateException("Dynamic checkpoint for " + entry.getKey() + " exceeds the agent's 16 KiB limit; no configuration was written.");
            }
        }
        if (!requested.isEmpty()) snapshots.add(dynamicSnapshot(clusterId, brokerId, previous, requested));
        return snapshots;
    }

    private Map<String, Object> dynamicSnapshot(UUID clusterId, int brokerId, Map<String, Object> previous,
                                               Map<String, String> requested) {
        Map<String, Object> snapshot = new java.util.LinkedHashMap<>();
        snapshot.put("mode", "DYNAMIC"); snapshot.put("key", "batch");
        snapshot.put("previous", new java.util.LinkedHashMap<>(previous)); snapshot.put("changes", new java.util.LinkedHashMap<>(requested));
        snapshot.put("brokerId", brokerId); snapshot.put("clusterId", clusterId.toString()); snapshot.put("capturedAt", java.time.Instant.now().toString());
        return snapshot;
    }

    private int snapshotSize(Map<String, Object> snapshot) {
        try { return mapper.writeValueAsBytes(snapshot).length; }
        catch (Exception e) { throw new IllegalStateException("Cannot serialize dynamic checkpoint", e); }
    }

    private Map<String, Object> batchEntries(UUID clusterId, int brokerId, Set<String> keys) {
        Map<String, Object> live = kafka.getBrokerConfigs(clusterId).get(brokerId);
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        for (String key : keys) {
            if (live == null || !(live.get(key) instanceof Map<?, ?>)) throw new IllegalStateException("Broker " + brokerId + " or property " + key + " is unavailable.");
            result.put(key, live.get(key));
        }
        return result;
    }

    private void verifyDynamic(UUID clusterId, int brokerId, String key, String desired) {
        // Kafka may acknowledge the write before the broker exposes it through DescribeConfigs.
        long timeoutMs = Math.max(0, dynamicVerificationTimeoutMs);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        String lastObservation = "No value was observed";
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new IllegalStateException("Interrupted during dynamic configuration verification");
            }
            try {
                String actual = currentValue(clusterId, brokerId, key);
                if (desired.equals(actual)) return;
                lastObservation = "last observed value=" + actual;
            } catch (RuntimeException e) {
                lastObservation = "last read failed: " + e.getMessage();
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            try {
                TimeUnit.NANOSECONDS.sleep(Math.min(remaining,
                        TimeUnit.MILLISECONDS.toNanos(Math.max(1, dynamicVerificationPollMs))));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted during dynamic configuration verification", e);
            }
        }
        throw new IllegalStateException("Kafka did not report the requested value on broker " + brokerId
                + " within " + timeoutMs + " ms: " + key + " expected=" + desired + ", "
                + lastObservation + ". The change may already be applied; refresh the broker configuration before retrying.");
    }

    void executeStatic(UUID clusterId, JobStep step, Map<String, Object> payload) {
        ensureTaskSettled(payload.get("configMutationTaskId"));
        int nodeId = ((Number) payload.get("nodeId")).intValue();
        String hostId = String.valueOf(payload.get("hostId"));
        String role = String.valueOf(payload.get("role"));
        Map<String, Object> change = new java.util.LinkedHashMap<>();
        Map<String, Object> expected = new java.util.LinkedHashMap<>();
        if (payload.get("changes") instanceof Map<?, ?> batch) {
            batch.forEach((key, value) -> change.put(String.valueOf(key), value));
            ((Map<?, ?>) payload.get("expectedValues")).forEach((key, value) -> expected.put(String.valueOf(key), value));
        } else {
            change.put(String.valueOf(payload.get("key")), payload.get("value"));
            expected.put(String.valueOf(payload.get("key")), payload.get("expectedOldValue"));
        }
        int expectedBrokers = ((Number) payload.get("expectedBrokerCount")).intValue();
        try {
            for (var entry : change.entrySet()) {
                if (entry.getValue() instanceof String value && value.isBlank())
                    throw new IllegalArgumentException("Blank configuration values cannot be applied or retried. Create a property deletion request for " + entry.getKey() + ".");
                if (entry.getValue() == null && !Boolean.TRUE.equals(payload.get("removal")))
                    throw new IllegalArgumentException("Missing configuration value; use an explicit property deletion request.");
            }
            if (payload.get("configMutationTaskId") != null
                    && "SUCCESS".equals(taskAwaiter.status(UUID.fromString(String.valueOf(payload.get("configMutationTaskId")))))) {
                verifyStatic(clusterId, hostId, role, nodeId, change, expectedBrokers);
                if (Boolean.TRUE.equals(payload.get("removal"))) verifyRemovedFile(clusterId, payload, change);
                jobs.completeStep(step.getId(), "Existing agent mutation succeeded; recovery verified without repeating the restart.");
                return;
            }
            if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                Task completed = taskAwaiter.await(step.getAgentTaskId());
                verifyStatic(clusterId, hostId, role, nodeId, change, expectedBrokers);
                if (Boolean.TRUE.equals(payload.get("removal"))) verifyRemovedFile(clusterId, payload, change);
                jobs.completeStep(step.getId(), completed.getLogOutput());
                return;
            }
            jobs.startStep(step.getId());
            checkHealth(clusterId, expectedBrokers, hostId);
            if ("controller".equalsIgnoreCase(role)) checkQuorum(clusterId, nodeId);
            else health.assertBrokerCanStop(clusterId, nodeId);
            jobs.checkpointConfigChange(step.getId(), Map.of("file", expected,
                    "dynamic", payload.getOrDefault("dynamicRemovalExpected", Map.of())));
            if (Boolean.TRUE.equals(payload.get("removal"))) prepareRemoval(clusterId, step, payload, change, expected);
            UUID taskId = jobs.queueConfigMutation(step.getId(), false, () -> deployment.patchKafkaConfig(clusterId, hostId, role, String.valueOf(nodeId),
                    String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath")),
                    change, expected, true, "targeted-" + step.getId()));
            jobs.attachAgentTask(step.getId(), taskId);
            Task completed = taskAwaiter.await(taskId);
            verifyStatic(clusterId, hostId, role, nodeId, change, expectedBrokers);
                if (Boolean.TRUE.equals(payload.get("removal"))) verifyRemovedFile(clusterId, payload, change);
            jobs.completeStep(step.getId(), completed.getLogOutput());
        } catch (Exception e) {
            jobs.failStep(step.getId(), e.getMessage());
            throw e;
        }
    }

    private void prepareRemoval(UUID clusterId, JobStep step, Map<String, Object> payload,
                                Map<String, Object> changes, Map<String, Object> expected) {
        String host = String.valueOf(payload.get("hostId")), role = String.valueOf(payload.get("role"));
        int id = ((Number) payload.get("nodeId")).intValue();
        Task fileTask = taskAwaiter.await(deployment.readKafkaProperties(clusterId, host, role, String.valueOf(id),
                String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath"))), 90);
        Object rawFile = read(fileTask.getLogOutput()).get("properties");
        if (!(rawFile instanceof Map<?, ?> file)) throw new IllegalStateException("Agent did not return active configuration values.");
        for (String key : changes.keySet()) {
            Object actual = file.get(key);
            if (actual != null && !java.util.Objects.equals(actual, expected.get(key))) throw new IllegalStateException("Property " + key + " changed since deletion preview; refresh the plan.");
        }
        if (!(payload.get("dynamicRemovalExpected") instanceof Map<?, ?> prior) || prior.isEmpty()) return;
        Map<String, Object> observed = batchEntries(clusterId, id, prior.keySet().stream().map(String::valueOf).collect(java.util.stream.Collectors.toSet()));
        Map<String, String> pending = new java.util.LinkedHashMap<>();
        for (String key : observed.keySet()) {
            Map<?, ?> entry = (Map<?, ?>) observed.get(key);
            if (!"DYNAMIC_BROKER_CONFIG".equals(entry.get("source"))) continue;
            if (!java.util.Objects.equals(prior.get(key), entry)) throw new IllegalStateException("Dynamic override " + key + " changed since deletion preview.");
            pending.put(key, null);
        }
        if (pending.isEmpty()) return;
        for (var snapshot : dynamicSnapshots(clusterId, id, observed, pending)) {
            UUID backup = deployment.backupKafkaConfig(clusterId, host, role, String.valueOf(id), String.valueOf(payload.get("configJson")),
                    String.valueOf(payload.get("configPath")), "batch", snapshot, "delete-" + step.getId());
            Task saved = taskAwaiter.await(backup);
            jobs.appendLog(step.getJob().getId(), "Deletion checkpoint on " + host + ": " + saved.getLogOutput());
        }
        if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("Interrupted before configuration deletion");
        if (!observed.equals(batchEntries(clusterId, id, observed.keySet()))) throw new IllegalStateException("Dynamic override changed while backing up; refresh the plan.");
        jobs.markConfigMutation(step.getId());
        kafka.removeBrokerConfigs(clusterId, id, pending.keySet(), false);
    }

    private void verifyRemovedFile(UUID clusterId, Map<String, Object> payload, Map<String, Object> changes) {
        UUID readTask = deployment.readKafkaProperties(clusterId, String.valueOf(payload.get("hostId")), String.valueOf(payload.get("role")),
                String.valueOf(payload.get("nodeId")), String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath")));
        Object raw = read(taskAwaiter.await(readTask, 90).getLogOutput()).get("properties");
        if (!(raw instanceof Map<?, ?> file)) throw new IllegalStateException("Unable to verify deleted file properties.");
        for (var entry : changes.entrySet()) if (entry.getValue() == null && file.containsKey(entry.getKey()))
            throw new IllegalStateException("Deleted property still exists in the active file: " + entry.getKey());
    }

    private void verifyStatic(UUID clusterId, String hostId, String role, int nodeId, Map<String, Object> changes, int expectedBrokers) {
        String lastFailure = "Kafka did not recover after restart";
        int stableSamples = 0;
        for (int attempt = 0; attempt < 45; attempt++) {
            try {
                checkHealth(clusterId, expectedBrokers, hostId);
                if ("controller".equalsIgnoreCase(role)) {
                    checkQuorum(clusterId, nodeId);
                } else {
                    Map<String, Object> live = batchEntries(clusterId, nodeId, changes.keySet());
                    for (var entry : changes.entrySet()) {
                        Object actual = ((Map<?, ?>) live.get(entry.getKey())).get("value");
                        Map<?, ?> details = (Map<?, ?>) live.get(entry.getKey());
                        if (entry.getValue() == null) {
                            if ("DYNAMIC_BROKER_CONFIG".equals(details.get("source"))) throw new IllegalStateException("Broker override still exists for " + entry.getKey());
                        } else if (!java.util.Objects.equals(entry.getValue(), actual)) throw new IllegalStateException("Kafka reports " + entry.getKey() + "=" + actual + " after restart");
                    }
                }
                if (++stableSamples >= 2) return;
                try { Thread.sleep(2000); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Health verification interrupted", e); }
            } catch (Exception e) {
                stableSamples = 0;
                lastFailure = e.getMessage();
                try { Thread.sleep(2000); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted during post-restart verification", interrupted);
                }
            }
        }
        throw new IllegalStateException("Node " + nodeId + " failed post-restart verification: " + lastFailure);
    }

    private void checkHealth(UUID clusterId, int expectedBrokers, String hostId) {
        var snapshot = health.inspect(clusterId, expectedBrokers, Set.of(hostId));
        if (!snapshot.healthy()) throw new IllegalStateException(snapshot.failureReason());
    }

    private void checkQuorum(UUID clusterId, int nodeId) {
        try {
            var quorum = kafka.getAdminClient(clusterId).describeMetadataQuorum().quorumInfo().get(15, TimeUnit.SECONDS);
            if (!RollingRestartQuorumHealth.healthy(quorum, false)
                    || quorum.voters().stream().noneMatch(voter -> voter.replicaId() == nodeId)) {
                throw new IllegalStateException("KRaft quorum or selected controller voter is unavailable");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during KRaft quorum check", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("KRaft quorum check failed", e);
        }
    }

    @Override
    public void rollback(Job job) {
        UUID clusterId = UUID.fromString(String.valueOf(read(job.getPayload()).get("clusterId")));
        var steps = new java.util.ArrayList<>(jobs.getSteps(job.getId()));
        // Recover the failed node first, then undo completed nodes in reverse order.
        steps.sort(java.util.Comparator.comparingInt((JobStep s) -> s.getStatus() == JobStepStatus.SUCCESS ? 1 : 0)
                .thenComparing(java.util.Comparator.comparingInt(JobStep::getStepOrder).reversed()));
        for (JobStep step : steps) {
            if (step.getStatus() == JobStepStatus.ROLLED_BACK) continue;
            Map<String, Object> payload = read(step.getPayload());
            if (!Boolean.TRUE.equals(payload.get("configMutationStarted"))) {
                if (step.getStatus() == JobStepStatus.SUCCESS && !payload.containsKey("recoveryCheckpoint"))
                    throw new IllegalStateException("This legacy step has no recovery checkpoint; use an explicit reviewed configuration change.");
                jobs.rolledBackStep(step.getId(), "No configuration mutation was dispatched; node left unchanged.");
                continue;
            }
            try {
                ensureTaskSettled(payload.get("configMutationTaskId"));
                ensureTaskSettled(payload.get("recoveryTaskId"));
                Map<String, Object> checkpoint = objectMap(payload.get("recoveryCheckpoint"));
                if (checkpoint.isEmpty()) throw new IllegalStateException("Pre-job checkpoint missing; recovery stopped.");
                jobs.startStep(step.getId());
                jobs.appendStepLog(step.getId(), "Restoring this job's pre-change values; unrelated settings are preserved.");
                if ("STATIC".equals(payload.get("operation")) || "patch_service".equals(payload.get("operation"))) rollbackStatic(clusterId, step, payload, checkpoint);
                else rollbackDynamic(clusterId, step, payload, objectMap(checkpoint.get("dynamic")), false);
                jobs.rolledBackStep(step.getId(), "Pre-job configuration restored and cluster health verified.");
            } catch (RuntimeException e) {
                jobs.rollbackFailedStep(step.getId(), e.getMessage());
                throw e;
            }
        }
    }

    private void ensureTaskSettled(Object task) {
        if (task == null) return;
        String status = taskAwaiter.status(UUID.fromString(String.valueOf(task)));
        if (!Set.of("SUCCESS", "FAILED", "CANCELLED").contains(status))
            throw new IllegalStateException("Agent task is still running or its outcome is unknown; wait for it to finish before recovery.");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> objectMap(Object value) {
        return value instanceof Map<?, ?> ? new java.util.LinkedHashMap<>((Map<String, Object>) value) : new java.util.LinkedHashMap<>();
    }

    private Map<String, Object> requestedChanges(Map<String, Object> payload) {
        if (payload.get("changes") instanceof Map<?, ?>) return objectMap(payload.get("changes"));
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put(String.valueOf(payload.get("key")), payload.get("value"));
        return result;
    }

    private void rollbackStatic(UUID clusterId, JobStep step, Map<String, Object> payload, Map<String, Object> checkpoint) {
        int nodeId = ((Number) payload.get("nodeId")).intValue();
        int brokers = ((Number) payload.get("expectedBrokerCount")).intValue();
        String host = String.valueOf(payload.get("hostId")), role = String.valueOf(payload.get("role"));
        deployment.requireConfigRecoveryAgent(host, String.valueOf(nodeId), String.valueOf(payload.get("configPath")), String.valueOf(payload.get("configJson")));
        Map<String, Object> previous = objectMap(checkpoint.get("file"));
        if (previous.isEmpty()) throw new IllegalStateException("File checkpoint is missing.");
        Task activeFile = taskAwaiter.await(deployment.readKafkaProperties(clusterId, host, role, String.valueOf(nodeId),
                String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath"))), 90);
        Map<String, Object> current = objectMap(read(activeFile.getLogOutput()).get("properties"));
        Map<String, Object> expected = new java.util.LinkedHashMap<>();
        Map<String, Object> forward = requestedChanges(payload);
        for (String key : previous.keySet()) {
            Object value = current.get(key);
            if (!java.util.Objects.equals(value, previous.get(key)) && !java.util.Objects.equals(value, forward.get(key)))
                throw new IllegalStateException("Property " + key + " changed outside this job; refresh and review before recovery.");
            expected.put(key, value);
        }
        boolean recoveredTaskSucceeded = payload.get("recoveryTaskId") != null
                && "SUCCESS".equals(taskAwaiter.status(UUID.fromString(String.valueOf(payload.get("recoveryTaskId")))));
        if (recoveredTaskSucceeded && previous.entrySet().stream().allMatch(e -> java.util.Objects.equals(e.getValue(), current.get(e.getKey())))) {
            Map<String, Object> dynamic = objectMap(checkpoint.get("dynamic"));
            if (!dynamic.isEmpty()) rollbackDynamic(clusterId, step, payload, dynamic, true);
            verifyStatic(clusterId, host, role, nodeId, Map.of(), brokers);
            return;
        }
        Task service = taskAwaiter.await(deployment.verifyKafkaService(clusterId, host, "controller".equalsIgnoreCase(role) ? "controller.service" : "broker_controller".equalsIgnoreCase(role) ? "kafka.service" : "broker.service"), 90);
        String state = String.valueOf(read(service.getLogOutput()).get("state"));
        if ("active".equals(state)) {
            checkHealth(clusterId, brokers, host);
            if ("controller".equalsIgnoreCase(role)) checkQuorum(clusterId, nodeId);
            else health.assertBrokerCanStop(clusterId, nodeId);
        } else if (!Set.of("failed", "inactive").contains(state)) {
            throw new IllegalStateException("Service is transitioning or its state is unknown; wait before recovery.");
        }
        // Only this job's already-stopped node may bypass the healthy-cluster precheck.
        // Recovery must bring it back before any other node can be restarted.
        Map<String, Object> dynamic = objectMap(checkpoint.get("dynamic"));
        UUID task = jobs.queueConfigMutation(step.getId(), true, () -> deployment.rollbackKafkaConfig(clusterId, host, role,
                String.valueOf(nodeId), String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath")),
                previous, expected, "rollback-" + step.getId()));
        taskAwaiter.await(task);
        if (!dynamic.isEmpty()) rollbackDynamic(clusterId, step, payload, dynamic, true);
        Task restored = taskAwaiter.await(deployment.readKafkaProperties(clusterId, host, role, String.valueOf(nodeId),
                String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath"))), 90);
        Map<String, Object> file = objectMap(read(restored.getLogOutput()).get("properties"));
        for (var entry : previous.entrySet())
            if (!java.util.Objects.equals(entry.getValue(), file.get(entry.getKey()))) throw new IllegalStateException("Restored file verification failed for " + entry.getKey());
        // File absence is valid; effective Kafka defaults need not equal null.
        verifyStatic(clusterId, host, role, nodeId, Map.of(), brokers);
    }

    private void rollbackDynamic(UUID clusterId, JobStep step, Map<String, Object> payload, Map<String, Object> previous, boolean deletion) {
        int id = ((Number) payload.getOrDefault("brokerId", payload.get("nodeId"))).intValue();
        deployment.requireConfigRecoveryAgent(String.valueOf(payload.get("hostId")), String.valueOf(id), String.valueOf(payload.get("configPath")), String.valueOf(payload.get("configJson")));
        if (!deletion) checkHealth(clusterId, ((Number) payload.get("expectedBrokerCount")).intValue(), String.valueOf(payload.get("hostId")));
        Map<String, Object> observed = batchEntries(clusterId, id, previous.keySet());
        Map<String, Object> forward = requestedChanges(payload);
        Map<String, String> sets = new java.util.LinkedHashMap<>();
        Set<String> removals = new java.util.LinkedHashSet<>();
        for (String key : previous.keySet()) {
            Map<String, Object> before = objectMap(previous.get(key)), now = objectMap(observed.get(key));
            if (before.isEmpty() || now.isEmpty()) throw new IllegalStateException("Dynamic configuration checkpoint is incomplete for " + key);
            if (sameDynamicOverride(before, now)) continue;
            boolean ownChange = deletion ? !"DYNAMIC_BROKER_CONFIG".equals(now.get("source"))
                    : "DYNAMIC_BROKER_CONFIG".equals(now.get("source")) && java.util.Objects.equals(now.get("value"), forward.get(key));
            if (!ownChange) throw new IllegalStateException("Dynamic override " + key + " changed outside this job; recovery stopped.");
            if ("DYNAMIC_BROKER_CONFIG".equals(before.get("source"))) sets.put(key, String.valueOf(before.get("value")));
            else removals.add(key);
        }
        if (!sets.isEmpty()) kafka.validateBrokerConfigs(clusterId, id, sets);
        if (!removals.isEmpty()) kafka.removeBrokerConfigs(clusterId, id, removals, true);
        if (!sets.isEmpty() || !removals.isEmpty()) {
            Map<String, String> recoveryChanges = new java.util.LinkedHashMap<>(sets);
            removals.forEach(key -> recoveryChanges.put(key, null));
            for (var snapshot : dynamicSnapshots(clusterId, id, observed, recoveryChanges)) {
                taskAwaiter.await(deployment.backupKafkaConfig(clusterId, String.valueOf(payload.get("hostId")), String.valueOf(payload.get("role")),
                        String.valueOf(id), String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath")), "batch", snapshot, "rollback-" + step.getId()));
            }
            if (!observed.equals(batchEntries(clusterId, id, previous.keySet()))) throw new IllegalStateException("Dynamic values changed during recovery backup.");
            if (!sets.isEmpty()) kafka.alterBrokerConfigs(clusterId, id, sets);
            if (!removals.isEmpty()) kafka.removeBrokerConfigs(clusterId, id, removals, false);
        }
        for (int attempt = 0; attempt < 30; attempt++) {
            Map<String, Object> live = batchEntries(clusterId, id, previous.keySet());
            if (previous.keySet().stream().allMatch(k -> sameDynamicOverride(objectMap(previous.get(k)), objectMap(live.get(k))))) {
                if (!deletion) checkHealth(clusterId, ((Number) payload.get("expectedBrokerCount")).intValue(), String.valueOf(payload.get("hostId")));
                return;
            }
            try { Thread.sleep(1000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Recovery verification interrupted", e); }
        }
        throw new IllegalStateException("Kafka has not verified the restored dynamic overrides; recovery stopped.");
    }

    private boolean sameDynamicOverride(Map<String, Object> previous, Map<String, Object> current) {
        boolean priorOverride = "DYNAMIC_BROKER_CONFIG".equals(previous.get("source"));
        boolean currentOverride = "DYNAMIC_BROKER_CONFIG".equals(current.get("source"));
        return priorOverride == currentOverride && java.util.Objects.equals(previous.get("source"), current.get("source"))
                && java.util.Objects.equals(previous.get("value"), current.get("value"));
    }

    private String currentValue(UUID clusterId, int brokerId, String key) {
        Object value = currentEntry(clusterId, brokerId, key).get("value");
        return value == null ? null : String.valueOf(value);
    }

    private Map<?, ?> currentEntry(UUID clusterId, int brokerId, String key) {
        Map<String, Object> configs = kafka.getBrokerConfigs(clusterId).get(brokerId);
        if (configs == null || !(configs.get(key) instanceof Map<?, ?> entry)) throw new IllegalStateException("Broker " + brokerId + " or property " + key + " is unavailable.");
        return entry;
    }

    private Map<String, Object> read(String json) {
        try { return mapper.readValue(json, new TypeReference<>() {}); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid configuration job payload", e); }
    }
}
