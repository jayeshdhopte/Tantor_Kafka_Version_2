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

/** Applies one dynamic broker property at a time; failure stops the remaining targets. */
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
            if ("STATIC".equals(payload.get("operation"))) {
                executeStatic(clusterId, step, payload);
                continue;
            }
            int brokerId = ((Number) payload.get("brokerId")).intValue();
            String key = String.valueOf(payload.get("key"));
            String desired = String.valueOf(payload.get("value"));
            String expected = String.valueOf(payload.get("expectedOldValue"));
            int count = ((Number) payload.get("expectedBrokerCount")).intValue();
            try {
                jobs.startStep(step.getId());
                var snapshot = health.inspect(clusterId, count, Set.of());
                if (!snapshot.healthy()) throw new IllegalStateException("Health precheck failed: " + snapshot.failureReason());
                String current = currentValue(clusterId, brokerId, key);
                if (!desired.equals(current)) {
                    if (!expected.equals(current == null ? "" : current)) {
                        throw new IllegalStateException("Broker " + brokerId + " value changed since preview; refresh the plan.");
                    }
                    kafka.alterBrokerConfig(clusterId, brokerId, key, desired);
                }
                verifyDynamic(clusterId, brokerId, key, desired);
                jobs.completeStep(step.getId(), "Verified " + key + " on broker " + brokerId + ".");
            } catch (Exception e) {
                jobs.failStep(step.getId(), e.getMessage());
                throw e;
            }
        }
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

    private void executeStatic(UUID clusterId, JobStep step, Map<String, Object> payload) {
        int nodeId = ((Number) payload.get("nodeId")).intValue();
        String hostId = String.valueOf(payload.get("hostId"));
        String role = String.valueOf(payload.get("role"));
        String key = String.valueOf(payload.get("key"));
        String desired = String.valueOf(payload.get("value"));
        int expectedBrokers = ((Number) payload.get("expectedBrokerCount")).intValue();
        try {
            if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                Task completed = taskAwaiter.await(step.getAgentTaskId());
                verifyStatic(clusterId, hostId, role, nodeId, key, desired, expectedBrokers);
                jobs.completeStep(step.getId(), completed.getLogOutput());
                return;
            }
            jobs.startStep(step.getId());
            checkHealth(clusterId, expectedBrokers, hostId);
            if ("controller".equalsIgnoreCase(role)) checkQuorum(clusterId, nodeId);
            Map<String, Object> change = new java.util.LinkedHashMap<>();
            change.put(key, desired);
            Map<String, Object> expected = new java.util.LinkedHashMap<>();
            expected.put(key, payload.get("expectedOldValue"));
            UUID taskId = deployment.patchKafkaConfig(clusterId, hostId, role, String.valueOf(nodeId),
                    String.valueOf(payload.get("configJson")), String.valueOf(payload.get("configPath")),
                    change, expected, true, "targeted-" + step.getId());
            jobs.attachAgentTask(step.getId(), taskId);
            Task completed = taskAwaiter.await(taskId);
            verifyStatic(clusterId, hostId, role, nodeId, key, desired, expectedBrokers);
            jobs.completeStep(step.getId(), completed.getLogOutput());
        } catch (Exception e) {
            jobs.failStep(step.getId(), e.getMessage());
            throw e;
        }
    }

    private void verifyStatic(UUID clusterId, String hostId, String role, int nodeId, String key,
                              String desired, int expectedBrokers) {
        String lastFailure = "Kafka did not recover after restart";
        for (int attempt = 0; attempt < 45; attempt++) {
            try {
                checkHealth(clusterId, expectedBrokers, hostId);
                if ("controller".equalsIgnoreCase(role)) {
                    checkQuorum(clusterId, nodeId);
                } else {
                    String actual = currentValue(clusterId, nodeId, key);
                    if (!desired.equals(actual)) throw new IllegalStateException("Kafka reports " + actual + " after restart");
                }
                return;
            } catch (Exception e) {
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
            var quorum = kafka.getAdminClient(clusterId).describeMetadataQuorum().quorumInfo().get();
            if (quorum.leaderId() < 0 || quorum.voters().size() < 3
                    || quorum.voters().stream().noneMatch(voter -> voter.replicaId() == nodeId)) {
                throw new IllegalStateException("KRaft quorum or selected controller voter is unavailable");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during KRaft quorum check", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("KRaft quorum check failed", e);
        }
    }

    @Override
    public void rollback(Job job) {
        throw new UnsupportedOperationException("Automatic rollback is unavailable; review partial results and create an explicit new change.");
    }

    private String currentValue(UUID clusterId, int brokerId, String key) {
        Map<String, Object> configs = kafka.getBrokerConfigs(clusterId).get(brokerId);
        if (configs == null || !(configs.get(key) instanceof Map<?, ?> entry)) {
            throw new IllegalStateException("Broker " + brokerId + " or property " + key + " is unavailable.");
        }
        Object value = entry.get("value");
        return value == null ? null : String.valueOf(value);
    }

    private Map<String, Object> read(String json) {
        try { return mapper.readValue(json, new TypeReference<>() {}); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid configuration job payload", e); }
    }
}
