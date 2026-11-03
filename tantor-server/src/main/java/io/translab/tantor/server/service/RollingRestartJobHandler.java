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
    @org.springframework.beans.factory.annotation.Autowired private ExternalKafkaManagement externalKafka;
    private final JobService jobService;
    private final DeploymentService deploymentService;
    private final AgentTaskAwaiter taskAwaiter;
    private final ClusterRepository clusterRepository;
    private final HostRepository hostRepository;
    private final KafkaAdminService kafkaAdminService;
    private final RollingRestartHealthService healthService;
    private final RollingRestartEvidenceService evidence;
    private final ObjectMapper objectMapper;
    private final RollingRestartTaskDispatcher restartDispatcher;
    private final RestartNodeDisplayService restartDisplay;
    @Value("${tantor.rolling-restart.health-timeout-seconds:300}") private long healthTimeoutSeconds;
    @Value("${tantor.rolling-restart.health-poll-seconds:10}") private long healthPollSeconds;
    @Value("${tantor.rolling-restart.stable-samples:2}") private int stableSamples;

    @Override public boolean supports(JobType type) { return type == JobType.ROLLING_RESTART; }

    @Override public void execute(Job job) {
        var request = readMap(job.getPayload());
        UUID clusterId = UUID.fromString(String.valueOf(request.get("clusterId")));
        Cluster cluster = externalKafka != null && externalKafka.external(clusterId) ? externalKafka.resolve(clusterId) : clusterRepository.findWithServicesById(clusterId).orElseThrow(() -> new IllegalArgumentException("Cluster not found."));
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) throw new IllegalArgumentException("Managed rolling restart requires internal node agents.");
        var topology = RollingRestartPlan.topology(cluster);
        int brokers = (int) topology.stream().filter(RollingRestartPlan::broker).count();
        var hosts = topology.stream().map(ClusterServiceAssignment::getHostId).collect(Collectors.toSet());
        boolean singleOverride = hosts.size() == 1 && Boolean.TRUE.equals(request.get("confirmSingleNode"));
        var settings=new LinkedHashMap<String,Object>(healthService.policySettings());
        settings.put("healthTimeoutSeconds",Math.max(1,healthTimeoutSeconds));
        settings.put("healthPollSeconds",Math.max(1,healthPollSeconds));
        settings.put("stableSamples",Math.max(1,stableSamples));
        settings.put("singleHostOverride",singleOverride);
        settings.put("expectedBrokerCount",brokers);
        settings.put("requiredHostIds",hosts.stream().sorted().toList());
        var context=evidence.begin(job,settings);
        var steps = new ArrayList<>(jobService.getSteps(job.getId()));
        Set<String> allowed = topology.stream().map(RollingRestartPlan::key).collect(Collectors.toSet());
        boolean controllers = false;
        try {
        for (var step : steps) {
            var payload = readMap(step.getPayload());
            String role = String.valueOf(payload.get("role"));
            String host = String.valueOf(payload.get("hostId"));
            if (!allowed.contains(host + ":" + String.valueOf(payload.getOrDefault("serviceName",RollingRestartPlan.unit(role)))) || topology.stream().noneMatch(service -> host.equals(service.getHostId()) && role.equals(service.getRole()) && Objects.equals(service.getNodeId(), ((Number) payload.get("nodeId")).intValue()))) throw new IllegalStateException("Restart topology changed. Create a new restart plan.");
            controllers |= role.contains("controller");
            if (step.getStatus() != JobStepStatus.SUCCESS && externalKafka != null && externalKafka.target(host)) externalKafka.require(host,false,true);
            else if (step.getStatus() != JobStepStatus.SUCCESS) {
                var agent = hostRepository.findById(host).orElseThrow(() -> new IllegalStateException("Agent host missing: " + host));
                if (agent.getAgentVersion() == null || !agent.getAgentVersion().contains("rollingrestart."))
                    throw new IllegalStateException("Update the internal agent on " + host + " to the rolling-restart release before starting.");
            }
        }
        if (!singleOverride && brokers < 2) throw new IllegalStateException("At least two brokers are required; single-host interruption needs confirmation.");
        if (controllers && !singleOverride && topology.stream().filter(RollingRestartPlan::controller).count() < 3)
            throw new IllegalStateException("Restarting controllers requires at least three metadata quorum members.");
        } catch(RuntimeException failure) {
            evidence.check(context,null,"PLAN","topology","FAIL",Map.of(),failure.getMessage());throw failure;
        }
        evidence.check(context,null,"PLAN","topology","PASS",Map.of("brokers",brokers,"hosts",hosts,"singleHostOverride",singleOverride),null);
        Integer leader = controllers ? kafkaAdminService.getControllerId(clusterId) : null;
        steps.sort(Comparator.comparingInt((JobStep step) -> step.getStatus() != JobStepStatus.SUCCESS && step.getAgentTaskId() != null ? -1 : 0)
                .thenComparingInt((JobStep step) -> {
            var payload = readMap(step.getPayload());
            if (!String.valueOf(payload.get("role")).contains("controller")) return 0;
            return leader != null && leader.toString().equals(String.valueOf(payload.get("nodeId"))) ? 2 : 1;
        }).thenComparing(JobStep::getStepOrder));
        boolean recovery = steps.stream().anyMatch(step -> step.getStatus() != JobStepStatus.SUCCESS && step.getAgentTaskId() != null);
        if (!recovery) waitForHealth(clusterId, brokers, hosts, controllers, singleOverride, context, null, "PREFLIGHT");
        for (JobStep step : steps) {
            if (step.getStatus() == JobStepStatus.SUCCESS) continue;
            var payload = readMap(step.getPayload());
            String host = String.valueOf(payload.get("hostId"));
            String unit = String.valueOf(payload.getOrDefault("serviceName",RollingRestartPlan.unit(String.valueOf(payload.get("role")))));
            var identity=restartDisplay.identity(host);
            identity.forEach((key,value)->{if(Objects.toString(payload.get(key),"").isBlank())payload.put(key,value);});
            String display=String.valueOf(payload.get("role")).replace('_','/')+" node "+payload.get("nodeId")+" - "+RestartNodeDisplayService.label(payload);
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
                        var observed = probeState(context,step.getId(),"RECOVERY_PROBE",probe);
                        if ("active".equals(observed.get("state"))) {
                            if (payload.get("baselineServiceState") instanceof Map<?, ?> before) {
                                alreadyRestarted = !Objects.equals(before.get("pid"), observed.get("pid"))
                                        || !Objects.equals(before.get("started"), observed.get("started"));
                            }
                            if (!alreadyRestarted) waitForHealth(clusterId, brokers, hosts, controllers, singleOverride, context, step.getId(), "RECOVERY_PREFLIGHT");
                        } else if (!List.of("inactive", "failed").contains(String.valueOf(observed.get("state"))))
                            throw new IllegalStateException("Kafka process has not settled: " + observed.get("state"));
                    }
                }
                if (!alreadyRestarted) {
                    if (step.getAgentTaskId() == null) waitForHealth(clusterId, brokers, hosts, controllers, singleOverride, context, step.getId(), "BEFORE_RESTART");
                    UUID baselineTask = deploymentService.verifyKafkaService(clusterId, host, unit);
                    var baseline = probeState(context,step.getId(),"BASELINE",baselineTask);
                    boolean active = "active".equals(baseline.get("state"));
                    if (step.getAgentTaskId() == null && !active) throw new IllegalStateException("Kafka process must be active before starting a new restart.");
                    if (active && !singleOverride && String.valueOf(payload.get("role")).contains("broker")) {
                        try {
                            healthService.assertBrokerCanStop(clusterId, ((Number) payload.get("nodeId")).intValue());
                            evidence.check(context,step.getId(),"BEFORE_STOP","brokerCanStop","PASS",Map.of("nodeId",payload.get("nodeId")),null);
                        } catch(RuntimeException failure) {
                            evidence.check(context,step.getId(),"BEFORE_STOP","brokerCanStop","FAIL",Map.of("nodeId",payload.get("nodeId")),failure.getMessage());throw failure;
                        }
                    } else evidence.check(context,step.getId(),"BEFORE_STOP","brokerCanStop","SKIPPED",Map.of("singleHostOverride",singleOverride),"Not an active broker stop or single-host interruption confirmed");
                    jobService.recordRollingRestartBaseline(step.getId(), baselineTask, baseline);
                    jobService.appendLog(job.getId(), "Restarting " + unit + " on " + display + "; waiting for service and cluster health before continuing.");
                    UUID task = restartDispatcher.dispatch(clusterId, host, unit, step.getId());
                    taskAwaiter.await(task);
                } else jobService.appendLog(job.getId(), "Resuming health verification on " + display + "; no repeated restart.");
                var verified = waitForService(context,step.getId(),clusterId,host,unit);
                if (!"active".equals(verified.get("state"))) throw new IllegalStateException("Kafka service is not active: " + verified);
                jobService.reportRollingRestartPhase(step.getId(), "Service active; verifying cluster health.");
                jobService.appendLog(job.getId(), "Service active on " + display + "; verifying quorum, replicas, ISR and host health.");
                waitForHealth(clusterId, brokers, hosts, controllers, singleOverride, context, step.getId(), "POST_RESTART");
                jobService.completeStep(step.getId(), "Service active; brokers, controller, replicas, ISR, lag and host health verified.");
            } catch (RuntimeException e) {
                jobService.failStep(step.getId(), e.getMessage());
                jobService.appendLog(job.getId(), "Stopped at " + display + ". Completed nodes are preserved; use Retry / Resume after resolving the failure.");
                throw e;
            }
        }
    }

    /** A successful systemd restart can precede the next process-discovery snapshot. Retry reads only. */
    private Map<String,Object> waitForService(RollingRestartEvidenceService.Context context,UUID step,UUID cluster,String host,String unit) {
        Instant deadline=Instant.now().plusSeconds(Math.max(1,healthTimeoutSeconds));
        String reason="Kafka service has not become active";
        do {
            try {
                long remaining=Math.max(1,(java.time.Duration.between(Instant.now(),deadline).toMillis()+999)/1000);
                var state=probeState(context,step,"POST_RESTART",deploymentService.verifyKafkaService(cluster,host,unit),remaining);
                if("active".equals(state.get("state"))) return state;
                reason="Kafka service state is "+state.get("state");
            } catch(RuntimeException failure) {
                // Discovery may briefly lose the process/unit association during restart. Never weaken target validation.
                if(failure.getMessage()==null || !failure.getMessage().contains("task does not match a locally discovered Kafka process and configuration")) throw failure;
                reason=failure.getMessage();
            }
            if(!Instant.now().isBefore(deadline)) break;
            try {Thread.sleep(Math.min(Math.max(1,healthPollSeconds)*1000,Math.max(1,java.time.Duration.between(Instant.now(),deadline).toMillis())));}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IllegalStateException("Service verification interrupted",interrupted);}
        } while(Instant.now().isBefore(deadline));
        throw new IllegalStateException("Timed out waiting for post-restart service discovery/health; no additional restart was issued: "+reason);
    }

    private Map<String,Object> probeState(RollingRestartEvidenceService.Context context,UUID step,String phase,UUID task) {
        return probeState(context,step,phase,task,null);
    }

    private Map<String,Object> probeState(RollingRestartEvidenceService.Context context,UUID step,String phase,UUID task,Long limitSeconds) {
        try {
            var observed=limitSeconds==null?taskAwaiter.await(task):taskAwaiter.await(task,limitSeconds);
            var state=readMap(observed.getLogOutput());
            evidence.check(context,step,phase,"serviceActive","active".equals(state.get("state"))?"PASS":"FAIL",
                    Map.of("taskId",task.toString(),"serviceState",state),null);
            return state;
        } catch(RuntimeException failure) {
            evidence.check(context,step,phase,"serviceActive","ERROR",Map.of("taskId",task.toString()),failure.getMessage());throw failure;
        }
    }

    private void waitForHealth(UUID clusterId, int brokers, Set<String> hosts, boolean controllers, boolean singleOverride,
            RollingRestartEvidenceService.Context context, UUID stepId, String phaseName) {
        UUID jobId=context.jobId();
        String reported = null;
        Instant deadline = Instant.now().plusSeconds(Math.max(1, healthTimeoutSeconds));
        int stable = 0;
        String reason = "Cluster health is not stable.";
        while (Instant.now().isBefore(deadline)) {
            var snapshot = healthService.inspect(clusterId, brokers, hosts);
            var checks=new LinkedHashMap<>(snapshot.checks());
            boolean healthy = snapshot.healthy();
            reason = snapshot.failureReason();
            if (healthy && controllers) {
                try {
                    var quorum = kafkaAdminService.getAdminClient(clusterId).describeMetadataQuorum().quorumInfo().get(15, TimeUnit.SECONDS);
                    healthy = RollingRestartQuorumHealth.healthy(quorum, singleOverride);
                    if (!healthy) reason = "Metadata quorum voters have not all caught up.";
                    checks.put("quorum",new RollingRestartEvidenceService.Check(healthy?"PASS":"FAIL",Map.of(
                            "leaderId",quorum.leaderId(),"highWatermark",quorum.highWatermark(),"voterCount",quorum.voters().size(),
                            "singleHostOverride",singleOverride,"freshnessMillis",30000,
                            "voters",quorum.voters().stream().map(v->Map.of("replicaId",v.replicaId(),"logEndOffset",v.logEndOffset(),
                                    "lastFetchTimestamp",v.lastFetchTimestamp().orElse(-1L),"lastCaughtUpTimestamp",v.lastCaughtUpTimestamp().orElse(-1L))).toList()),healthy?null:reason));
                } catch (InterruptedException e) {
                    checks.put("quorum",new RollingRestartEvidenceService.Check("ERROR",Map.of(),"Quorum inspection interrupted"));
                    evidence.sample(context,stepId,phaseName,checks);
                    Thread.currentThread().interrupt(); throw new IllegalStateException("Quorum inspection interrupted", e);
                } catch (Exception e) {
                    healthy = false; reason = "Metadata quorum inspection failed: " + e.getMessage();
                    checks.put("quorum",new RollingRestartEvidenceService.Check("ERROR",Map.of(),reason));
                }
            } else checks.put("quorum",new RollingRestartEvidenceService.Check(controllers?"UNKNOWN":"SKIPPED",Map.of(),controllers?"Base health failed; quorum not inspected":"No controller roles selected"));
            stable = healthy ? stable + 1 : 0;
            checks.put("stableSamples",new RollingRestartEvidenceService.Check(stable>=Math.max(1,stableSamples)?"PASS":"FAIL",
                    Map.of("consecutiveHealthy",stable,"required",Math.max(1,stableSamples),"aggregateHealthy",healthy),healthy?null:reason));
            evidence.sample(context,stepId,phaseName,checks);
            if (stable >= Math.max(1, stableSamples)) return;
            String phase = healthy ? "Waiting for the next stable cluster health sample." : "Waiting for cluster health: " + reason;
            if (jobId != null && !phase.equals(reported)) {
                jobService.appendLog(jobId, phase);
                if(stepId!=null) jobService.reportRollingRestartPhase(stepId, phaseName.equals("POST_RESTART")?"Service active; "+phase:phase);
                reported = phase;
            }
            try { Thread.sleep(Math.max(1, healthPollSeconds) * 1000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Health verification interrupted", e); }
        }
        evidence.check(context,stepId,phaseName+"_TIMEOUT","stableSamples","FAIL",Map.of("required",Math.max(1,stableSamples)),reason);
        throw new IllegalStateException("Timed out waiting for stable cluster health: " + reason);
    }

    @Override public void rollback(Job job) { throw new IllegalStateException("Standalone rolling restart changes no configuration. Use Retry / Resume."); }
    private Map<String, Object> readMap(String json) {
        try { return objectMapper.readValue(json, new TypeReference<>() {}); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid rolling restart payload", e); }
    }
}
