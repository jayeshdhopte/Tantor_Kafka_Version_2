package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class RollingRestartJobHandler implements JobHandler {
    private final JobService jobService;
    private final DeploymentService deploymentService;
    private final AgentTaskAwaiter taskAwaiter;
    private final ClusterRepository clusterRepository;
    private final HostRepository hostRepository;
    private final KafkaAdminService kafkaAdminService;
    private final RollingRestartHealthService healthService;
    private final ObjectMapper objectMapper;
    private final RollingRestartTaskDispatcher restartDispatcher;
    @Value("${tantor.rolling-restart.health-timeout-seconds:300}") private long healthTimeoutSeconds;
    @Value("${tantor.rolling-restart.health-poll-seconds:10}") private long healthPollSeconds;
    @Value("${tantor.rolling-restart.stable-samples:2}") private int stableSamples;

    @Override public boolean supports(JobType type) { return type == JobType.ROLLING_RESTART; }

    @Override public void execute(Job job) {
        var request = readMap(job.getPayload());
        UUID clusterId = UUID.fromString(String.valueOf(request.get("clusterId")));
        Cluster cluster = clusterRepository.findWithServicesById(clusterId).orElseThrow(() -> new IllegalArgumentException("Cluster not found."));
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) throw new IllegalArgumentException("Managed rolling restart requires internal node agents.");
        var topology = RollingRestartPlan.topology(cluster);
        int brokers = (int) topology.stream().filter(RollingRestartPlan::broker).count();
        var hosts = topology.stream().map(ClusterServiceAssignment::getHostId).collect(Collectors.toSet());
        boolean singleOverride = hosts.size() == 1 && Boolean.TRUE.equals(request.get("confirmSingleNode"));
        var steps = new ArrayList<>(jobService.getSteps(job.getId()));
        Set<String> allowed = topology.stream().map(RollingRestartPlan::key).collect(Collectors.toSet());
        boolean controllers = false;
        for (var step : steps) {
            var payload = readMap(step.getPayload());
            String role = String.valueOf(payload.get("role"));
            String host = String.valueOf(payload.get("hostId"));
            if (!allowed.contains(host + ":" + RollingRestartPlan.unit(role)) || topology.stream().noneMatch(service -> host.equals(service.getHostId()) && role.equals(service.getRole()) && Objects.equals(service.getNodeId(), ((Number) payload.get("nodeId")).intValue()))) throw new IllegalStateException("Restart topology changed. Create a new restart plan.");
            controllers |= role.contains("controller");
            if (step.getStatus() != JobStepStatus.SUCCESS) {
                var agent = hostRepository.findById(host).orElseThrow(() -> new IllegalStateException("Agent host missing: " + host));
                if (agent.getAgentVersion() == null || !agent.getAgentVersion().contains("rollingrestart."))
                    throw new IllegalStateException("Update the internal agent on " + host + " to the rolling-restart release before starting.");
            }
        }
        if (!singleOverride && brokers < 2) throw new IllegalStateException("At least two brokers are required; single-host interruption needs confirmation.");
        if (controllers && !singleOverride && topology.stream().filter(RollingRestartPlan::controller).count() < 3)
            throw new IllegalStateException("Restarting controllers requires at least three metadata quorum members.");
        Integer leader = controllers ? kafkaAdminService.getControllerId(clusterId) : null;
        steps.sort(Comparator.comparingInt((JobStep step) -> step.getStatus() != JobStepStatus.SUCCESS && step.getAgentTaskId() != null ? -1 : 0)
                .thenComparingInt((JobStep step) -> {
            var payload = readMap(step.getPayload());
            if (!String.valueOf(payload.get("role")).contains("controller")) return 0;
            return leader != null && leader.toString().equals(String.valueOf(payload.get("nodeId"))) ? 2 : 1;
        }).thenComparing(JobStep::getStepOrder));
        boolean recovery = steps.stream().anyMatch(step -> step.getStatus() != JobStepStatus.SUCCESS && step.getAgentTaskId() != null);
        if (!recovery) waitForHealth(clusterId, brokers, hosts, controllers, singleOverride);
        for (JobStep step : steps) {
            if (step.getStatus() == JobStepStatus.SUCCESS) continue;
            var payload = readMap(step.getPayload());
            String host = String.valueOf(payload.get("hostId"));
            String unit = RollingRestartPlan.unit(String.valueOf(payload.get("role")));
            jobService.startStep(step.getId());
            try {
                boolean alreadyRestarted = false;
                if (step.getAgentTaskId() != null) {
                    String prior = taskAwaiter.status(step.getAgentTaskId());
                    if (!List.of("FAILED", "CANCELLED").contains(prior)) {
                        taskAwaiter.await(step.getAgentTaskId());
                        alreadyRestarted = true;
                    } else {
                        // A failed task response can still follow a recovered process. Probe it before repeating the restart.
                        UUID probe = deploymentService.verifyKafkaService(clusterId, host, unit);
                        var observed = readMap(taskAwaiter.await(probe).getLogOutput());
                        if ("active".equals(observed.get("state"))) {
                            if (payload.get("baselineServiceState") instanceof Map<?, ?> before) {
                                alreadyRestarted = !Objects.equals(before.get("pid"), observed.get("pid"))
                                        || !Objects.equals(before.get("started"), observed.get("started"));
                            }
                            if (!alreadyRestarted) waitForHealth(clusterId, brokers, hosts, controllers, singleOverride);
                        } else if (!List.of("inactive", "failed").contains(String.valueOf(observed.get("state"))))
                            throw new IllegalStateException("Kafka process has not settled: " + observed.get("state"));
                    }
                }
                if (!alreadyRestarted) {
                    if (step.getAgentTaskId() == null) waitForHealth(clusterId, brokers, hosts, controllers, singleOverride);
                    UUID baselineTask = deploymentService.verifyKafkaService(clusterId, host, unit);
                    var baseline = readMap(taskAwaiter.await(baselineTask).getLogOutput());
                    boolean active = "active".equals(baseline.get("state"));
                    if (step.getAgentTaskId() == null && !active) throw new IllegalStateException("Kafka process must be active before starting a new restart.");
                    if (active && !singleOverride && String.valueOf(payload.get("role")).contains("broker"))
                        healthService.assertBrokerCanStop(clusterId, ((Number) payload.get("nodeId")).intValue());
                    jobService.recordRollingRestartBaseline(step.getId(), baselineTask, baseline);
                    jobService.appendLog(job.getId(), "Restarting " + unit + " on " + host + "; waiting for service and cluster health before continuing.");
                    UUID task = restartDispatcher.dispatch(clusterId, host, unit, step.getId());
                    taskAwaiter.await(task);
                } else jobService.appendLog(job.getId(), "Resuming health verification on " + host + "; no repeated restart.");
                Task verified = taskAwaiter.await(deploymentService.verifyKafkaService(clusterId, host, unit));
                if (!"active".equals(readMap(verified.getLogOutput()).get("state"))) throw new IllegalStateException("Kafka service is not active: " + verified.getLogOutput());
                waitForHealth(clusterId, brokers, hosts, controllers, singleOverride);
                jobService.completeStep(step.getId(), "Service active; brokers, controller, replicas, ISR, lag and host health verified.");
            } catch (RuntimeException e) {
                jobService.failStep(step.getId(), e.getMessage());
                jobService.appendLog(job.getId(), "Stopped at " + host + ". Completed nodes are preserved; use Retry / Resume after resolving the failure.");
                throw e;
            }
        }
    }

    private void waitForHealth(UUID clusterId, int brokers, Set<String> hosts, boolean controllers, boolean singleOverride) {
        Instant deadline = Instant.now().plusSeconds(Math.max(1, healthTimeoutSeconds));
        int stable = 0;
        String reason = "Cluster health is not stable.";
        while (Instant.now().isBefore(deadline)) {
            var snapshot = healthService.inspect(clusterId, brokers, hosts);
            boolean healthy = snapshot.healthy();
            reason = snapshot.failureReason();
            if (healthy && controllers) {
                try {
                    var quorum = kafkaAdminService.getAdminClient(clusterId).describeMetadataQuorum().quorumInfo().get(15, TimeUnit.SECONDS);
                    long fresh = quorum.voters().stream().filter(v -> v.replicaId() == quorum.leaderId()
                            || v.lastCaughtUpTimestamp().orElse(0L) >= System.currentTimeMillis() - 30000).count();
                    healthy = quorum.leaderId() >= 0 && (singleOverride || quorum.voters().size() >= 3)
                            && fresh == quorum.voters().size();
                    if (!healthy) reason = "Metadata quorum voters have not all caught up.";
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); throw new IllegalStateException("Quorum inspection interrupted", e);
                } catch (Exception e) { healthy = false; reason = "Metadata quorum inspection failed: " + e.getMessage(); }
            }
            stable = healthy ? stable + 1 : 0;
            if (stable >= Math.max(1, stableSamples)) return;
            try { Thread.sleep(Math.max(1, healthPollSeconds) * 1000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Health verification interrupted", e); }
        }
        throw new IllegalStateException("Timed out waiting for stable cluster health: " + reason);
    }

    @Override public void rollback(Job job) { throw new IllegalStateException("Standalone rolling restart changes no configuration. Use Retry / Resume."); }
    private Map<String, Object> readMap(String json) {
        try { return objectMapper.readValue(json, new TypeReference<>() {}); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid rolling restart payload", e); }
    }
}
