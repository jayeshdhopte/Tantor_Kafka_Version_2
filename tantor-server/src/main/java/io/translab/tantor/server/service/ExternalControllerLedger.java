package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * One external parent owns the candidate, all process IDs, task generations and atomic publication.
 */
@Service
@RequiredArgsConstructor
public class ExternalControllerLedger {
  private final ExternalControllerPlanning planning;
  private final ExternalControllerTasks tasks;
  private final ExternalControllerRelease release;
  private final JobService jobs;
  private final JdbcTemplate jdbc;
  private final KafkaExpansionNativeEvidence evidence;
  private final ObjectMapper mapper;
  private final ExternalClusterRepository clusters;

  public record Operation(
      @com.fasterxml.jackson.annotation.JsonProperty("operationId") UUID id,
      UUID clusterId,
      String agentId,
      int nodeId,
      UUID jobId,
      String state,
      int generation,
      String error,
      boolean inventoryPublished,
      Integer brokerNodeId) {
    public Operation(
        UUID id,
        UUID clusterId,
        String agentId,
        int nodeId,
        UUID jobId,
        String state,
        int generation,
        String error,
        boolean inventoryPublished) {
      this(
          id,
          clusterId,
          agentId,
          nodeId,
          jobId,
          state,
          generation,
          error,
          inventoryPublished,
          null);
    }
  }

  public record Accepted(UUID operationId, UUID jobId, String state) {}

  public record Sample(
      UUID inspectionId,
      KafkaExpansionLiveVerifier.ControllerProof controller,
      String health,
      Instant observedAt,
      UUID brokerInspectionId) {
    public Sample(
        UUID inspectionId,
        KafkaExpansionLiveVerifier.ControllerProof controller,
        String health,
        Instant observedAt) {
      this(inspectionId, controller, health, observedAt, null);
    }
  }

  public record Verified(String planDigest, List<Sample> samples, Instant verifiedAt) {}

  @Transactional
  public Accepted accept(UUID cluster, UUID preview, String context, String digest, String key) {
    release.require();
    if (preview == null
        || key == null
        || !key.matches("[A-Za-z0-9_-]{16,128}")
        || context == null
        || !context.matches("[a-f0-9]{64}")
        || digest == null
        || !digest.matches("[a-f0-9]{64}"))
      throw new IllegalArgumentException(
          "Reviewed controller context, digest and stable request key are required.");
    String requestDigest = BrokerAdditionLedger.hash(preview + ":" + context + ":" + digest);
    var previous = existing(cluster, key, requestDigest);
    if (previous != null) return previous;
    var f = planning.frozen(cluster, preview, true);
    var p = f.nativePlan().plan();
    if (!context.equals(p.contextHash()) || !digest.equals(f.nativePlan().digest()))
      throw new IllegalStateException("Controller review changed.");
    clusters.findForServiceUpdate(cluster).orElseThrow();
    jdbc.queryForObject(
        "select id from kf_discovery_agents where id=? for update",
        String.class,
        p.candidateHostId());
    previous = existing(cluster, key, requestDigest);
    if (previous != null) return previous;
    jdbc.queryForList(
        "select id from kf_discovery_agents where id=? or id in(select agent_id from"
            + " kf_external_cluster_nodes where cluster_id=?) order by id for update",
        p.candidateHostId(),
        cluster);
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_kafka_tasks t join kf_external_cluster_nodes n"
                + " on n.agent_id=t.agent_id where n.cluster_id=? and t.status in"
                + " ('PENDING','RUNNING','IN_PROGRESS') and t.command not in"
                + " ('READ_KAFKA_PROPERTY','READ_KAFKA_PROPERTIES','VERIFY_KAFKA_SERVICE') union"
                + " all select 1 from kf_external_service_tasks t join kf_external_cluster_nodes n"
                + " on n.agent_id=t.agent_id where n.cluster_id=? and t.status in"
                + " ('PENDING','RUNNING','IN_PROGRESS') and t.command<>'PRECHECK')",
            Boolean.class,
            cluster,
            cluster)))
      throw new IllegalStateException(
          "An existing participant mutation must settle before controller addition.");
    planning.current(f);
    if (!f.expiresAt().isAfter(Instant.now()))
      throw new IllegalStateException("Controller review expired.");
    Boolean busy =
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_controller_tasks where agent_id=? and status"
                + " in ('PENDING','RUNNING') and deadline>clock_timestamp() union all select 1 from"
                + " kf_external_add_node_tasks t join kf_external_add_node_previews p on"
                + " p.id=t.preview_id where p.agent_id=? and t.status in ('PENDING','RUNNING') and"
                + " t.deadline>clock_timestamp() union all select 1 from kf_external_service_tasks"
                + " where agent_id=? and status in ('PENDING','RUNNING','IN_PROGRESS') and"
                + " command<>'PRECHECK' union all select 1 from kf_external_kafka_tasks where"
                + " agent_id=? and status in ('PENDING','RUNNING','IN_PROGRESS') and command not in"
                + " ('READ_KAFKA_PROPERTY','READ_KAFKA_PROPERTIES','VERIFY_KAFKA_SERVICE'))",
            Boolean.class,
            p.candidateHostId(),
            p.candidateHostId(),
            p.candidateHostId(),
            p.candidateHostId());
    if (Boolean.TRUE.equals(busy))
      throw new IllegalStateException(
          "Candidate has an unfinished observation or other task. Wait for it to settle.");
    var job = new Job();
    job.setType(JobType.EXTERNAL_CONTROLLER_ADDITION);
    job.setResourceKey("cluster:" + cluster);
    job.setRollbackSupported(false);
    job.setPayload(
        tasks.json(
            Map.of(
                "clusterId",
                cluster,
                "operationId",
                preview,
                "external",
                true,
                "layout",
                p.layout().name())));
    var steps = new ArrayList<JobStep>();
    for (String name :
        List.of(
            p.layout() == KafkaExpansionWorkflow.Layout.SEPARATE
                ? "Repeat final checks for both processes"
                : "Repeat final external controller checks",
            p.layout() == KafkaExpansionWorkflow.Layout.SEPARATE
                ? "Prepare two isolated process layouts"
                : "Prepare controller observer",
            "Format empty controller storage",
            "Start controller observer",
            "Verify catch-up and admit controller",
            p.layout() == KafkaExpansionWorkflow.Layout.SEPARATE
                ? "Start and verify broker; publish both processes"
                : "Verify controller and publish membership")) {
      var step = new JobStep();
      step.setStepOrder(steps.size() + 1);
      step.setName(name);
      step.setTargetId(p.candidateIp());
      steps.add(step);
    }
    job = jobs.createJob(job, steps);
    jdbc.update(
        "insert into"
            + " kf_external_controller_operations(id,cluster_id,agent_id,node_id,job_id,request_key,request_digest,broker_node_id)"
            + " values(?,?,?,?,?,?,?,?)",
        preview,
        cluster,
        p.candidateHostId(),
        p.process("controller").nodeId(),
        job.getId().toString(),
        key,
        requestDigest,
        p.layout() == KafkaExpansionWorkflow.Layout.SEPARATE ? p.process("broker").nodeId() : null);
    return accepted(get(preview));
  }

  public Operation get(UUID id) {
    var r = jdbc.queryForMap("select * from kf_external_controller_operations where id=?", id);
    return operation(r);
  }

  public Operation forJob(UUID id) {
    return operation(
        jdbc.queryForMap(
            "select * from kf_external_controller_operations where job_id=?", id.toString()));
  }

  public Operation view(UUID cluster, UUID id) {
    var op = get(id);
    if (!cluster.equals(op.clusterId()))
      throw new IllegalStateException("Foreign controller operation.");
    return op;
  }

  public Accepted lookup(UUID cluster, String key) {
    if (key == null || !key.matches("[A-Za-z0-9_-]{16,128}"))
      throw new IllegalArgumentException("Invalid request key.");
    var r =
        jdbc.queryForMap(
            "select * from kf_external_controller_operations where cluster_id=? and request_key=?",
            cluster,
            key);
    return accepted(operation(r));
  }

  @Transactional
  public Accepted resume(UUID cluster, UUID id) {
    var op = view(cluster, id);
    jdbc.queryForObject(
        "select id from kf_jobs where id=? for update", String.class, op.jobId().toString());
    op = get(id);
    if ("SUCCEEDED".equals(op.state())) return accepted(op);
    if ("CANCELLED_SAFE".equals(op.state()))
      throw new IllegalStateException("Cancelled controller operation cannot resume.");
    planning.current(planning.owned(id));
    var job = jobs.getJob(op.jobId());
    if (Set.of(JobStatus.PENDING, JobStatus.IN_PROGRESS).contains(job.getStatus()))
      return accepted(op);
    settle(id);
    jobs.retryExternalBrokerJob(op.jobId());
    jdbc.update(
        "update kf_external_controller_operations set"
            + " state='RECONCILING',resume_generation=resume_generation+1,verification=null,last_error=null,updated_at=clock_timestamp()"
            + " where id=?",
        id);
    return accepted(get(id));
  }

  @Transactional
  public void reconcile(UUID id, int generation) {
    var op = lock(id, generation);
    settle(id);
    jdbc.update(
        "update kf_external_controller_operations set"
            + " state='RECONCILING',verification=null,updated_at=clock_timestamp() where id=?",
        id);
  }

  @Transactional
  public void state(UUID id, int generation, String state) {
    lock(id, generation);
    if (!Set.of(
            "VALIDATING",
            "PREPARING",
            "PREPARED",
            "FORMATTING",
            "FORMATTED",
            "STARTING",
            "OBSERVER_STARTED",
            "CATCHING_UP",
            "ADMITTING",
            "VOTER_VERIFIED",
            "VERIFYING")
        .contains(state)) throw new IllegalArgumentException("Invalid controller checkpoint.");
    jdbc.update(
        "update kf_external_controller_operations set state=?,updated_at=clock_timestamp() where"
            + " id=?",
        state,
        id);
  }

  @Transactional
  public void step(UUID id, int generation, UUID step, JobStepStatus state, String message) {
    var op = lock(id, generation);
    if (jobs.getSteps(op.jobId()).stream().noneMatch(s -> step.equals(s.getId())))
      throw new IllegalStateException("Foreign controller step.");
    switch (state) {
      case IN_PROGRESS -> jobs.startStep(step);
      case SUCCESS -> jobs.completeStep(step, message);
      case FAILED -> jobs.failStep(step, message);
      default -> throw new IllegalArgumentException("Unsupported step state.");
    }
  }

  @Transactional
  public void failure(UUID id, int generation) {
    jdbc.update(
        "update kf_external_controller_operations set"
            + " state='RECOVERY_REQUIRED',last_error='External Add Node execution interrupted."
            + " Retain storage, process identities and all reservations; resume this operation to"
            + " reconcile actual state.',updated_at=clock_timestamp() where id=? and"
            + " resume_generation=? and state not in ('SUCCEEDED','CANCELLED_SAFE')",
        id,
        generation);
  }

  @Transactional
  public Accepted cancel(UUID cluster, UUID id) {
    var initial = view(cluster, id);
    jdbc.queryForObject(
        "select id from kf_jobs where id=? for update", String.class, initial.jobId().toString());
    var op = get(id);
    if ("CANCELLED_SAFE".equals(op.state())) return accepted(op);
    if ("SUCCEEDED".equals(op.state()))
      throw new IllegalStateException("Published membership cannot be cancelled.");
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_controller_tasks where operation_id=? and"
                + " command in ('PREPARE','FORMAT','START','ADMIT'))",
            Boolean.class,
            id)))
      throw new IllegalStateException(
          "Candidate changes were dispatched; preserve ownership and resume.");
    settle(id);
    jdbc.update(
        "update kf_external_controller_operations set state='CANCELLED_SAFE' where id=?", id);
    jobs.updateJobStatus(op.jobId(), JobStatus.FAILED);
    return accepted(get(id));
  }

  @Transactional
  public void verified(UUID id, int generation, Verified proof) {
    var op = lock(id, generation);
    if (!"VERIFYING".equals(op.state()))
      throw new IllegalStateException("Controller verification stage required.");
    validate(op, proof);
    jdbc.update(
        "update kf_external_controller_operations set state='PUBLISHING',verification=? where id=?",
        tasks.json(proof),
        id);
  }

  @Transactional
  public Accepted publish(UUID id, int generation, UUID stepId) {
    var initial = get(id);
    if ("SUCCEEDED".equals(initial.state())) return accepted(initial);
    var op = lock(id, generation);
    if (!"PUBLISHING".equals(op.state()))
      throw new IllegalStateException("Verified controller membership required.");
    var proof =
        tasks.parse(
            jdbc.queryForObject(
                "select verification from kf_external_controller_operations where id=?",
                String.class,
                id),
            Verified.class);
    validate(op, proof);
    var step =
        jobs.getSteps(op.jobId()).stream()
            .filter(s -> stepId.equals(s.getId()) && s.getStepOrder() == 6)
            .findFirst()
            .orElseThrow();
    var frozen = planning.owned(id);
    planning.current(frozen);
    var p = frozen.nativePlan().plan();
    var c = p.process("controller");
    var cluster = clusters.findForServiceUpdate(op.clusterId()).orElseThrow();
    if (!p.kafkaClusterId().equals(cluster.getKafkaClusterId())
        || !p.kafkaVersion().equals(cluster.getKafkaVersion())
        || "DELETED".equalsIgnoreCase(cluster.getStatus()))
      throw new IllegalStateException("External cluster identity changed before publication.");
    String owner =
        jdbc.queryForObject(
            "select coalesce(cluster_id::text,'') from kf_discovery_agents where id=? for update",
            String.class,
            op.agentId());
    if (!owner.isEmpty() && !owner.equals(op.clusterId().toString()))
      throw new IllegalStateException("Controller candidate belongs to another cluster.");
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_cluster_nodes where agent_id=? or"
                + " (cluster_id=? and node_id=?))",
            Boolean.class,
            op.agentId(),
            op.clusterId(),
            op.nodeId())))
      throw new IllegalStateException("Controller inventory conflicts with another publication.");
    jdbc.queryForObject(
        "select set_config('tantor.external_broker_operation',?,true)",
        String.class,
        id.toString());
    UUID brokerNode = null;
    if (p.layout() == KafkaExpansionWorkflow.Layout.SEPARATE) {
      var b = p.process("broker");
      brokerNode = UUID.nameUUIDFromBytes((id + ":broker").getBytes(StandardCharsets.UTF_8));
      if (Boolean.TRUE.equals(
          jdbc.queryForObject(
              "select exists(select 1 from kf_external_cluster_nodes where cluster_id=? and"
                  + " node_id=?)",
              Boolean.class,
              op.clusterId(),
              b.nodeId())))
        throw new IllegalStateException("Broker inventory conflicts with another publication.");
      jdbc.update(
          "insert into"
              + " kf_external_cluster_nodes(id,cluster_id,host,agent_id,node_id,is_broker,is_controller,port,systemd_service,config_file,install_dir,data_dirs,log_dirs,jmx_exporter_port,can_manage_kafka,can_write_config,can_restart,last_seen)"
              + " values(?,?,?,?,?,true,false,?,?,?,?,?,?,?,true,false,false,?)",
          brokerNode,
          op.clusterId(),
          p.candidateIp(),
          op.agentId(),
          b.nodeId(),
          b.port(),
          "broker",
          p.configPath("broker"),
          p.deploymentSettings().installPath(),
          String.join(",", b.dataPaths()),
          b.logPath(),
          b.runtime().jmxPort() > 0 ? b.runtime().jmxPort() : null,
          Timestamp.from(proof.verifiedAt()));
      jdbc.update(
          "update kf_external_clusters set broker_count=(select count(*) from"
              + " kf_external_cluster_nodes where cluster_id=? and is_broker=true) where id=?",
          op.clusterId(),
          op.clusterId());
    }
    UUID node = UUID.nameUUIDFromBytes((id + ":controller").getBytes(StandardCharsets.UTF_8));
    jdbc.update(
        "insert into"
            + " kf_external_cluster_nodes(id,cluster_id,host,agent_id,node_id,is_broker,is_controller,port,systemd_service,config_file,install_dir,data_dirs,log_dirs,jmx_exporter_port,can_manage_kafka,can_write_config,can_restart,last_seen)"
            + " values(?,?,?,?,?,false,true,?,?,?,?,?,?,?,true,false,false,?)",
        node,
        op.clusterId(),
        p.candidateIp(),
        op.agentId(),
        c.nodeId(),
        c.port(),
        "controller",
        p.configPath("controller"),
        p.deploymentSettings().installPath(),
        String.join(",", c.dataPaths()),
        c.logPath(),
        c.runtime().jmxPort() > 0 ? c.runtime().jmxPort() : null,
        Timestamp.from(proof.verifiedAt()));
    jdbc.update(
        "update kf_discovery_agents set cluster_id=? where id=?", op.clusterId(), op.agentId());
    jdbc.update(
        "update kf_external_controller_operations set"
            + " state='SUCCEEDED',published_node_id=?,published_broker_node_id=?,last_error=null,updated_at=clock_timestamp()"
            + " where id=?",
        node,
        brokerNode,
        id);
    // Broker count is unchanged by controller-only publication.
    jobs.completeStep(
        step.getId(),
        "Exact voter/directory identity, stable quorum/cluster health and runtime verified;"
            + " controller inventory published atomically.");
    jobs.updateJobStatus(op.jobId(), JobStatus.SUCCESS);
    return accepted(get(id));
  }

  public void validate(Operation op, Verified proof) {
    var frozen = planning.owned(op.id());
    var p = frozen.nativePlan().plan();
    if (proof == null
        || !frozen.nativePlan().digest().equals(proof.planDigest())
        || proof.samples() == null
        || proof.samples().size() != 3
        || proof.verifiedAt() == null
        || proof.verifiedAt().isBefore(Instant.now().minusSeconds(45))
        || proof.verifiedAt().isAfter(Instant.now().plusSeconds(5)))
      throw new IllegalStateException("Fresh stable controller evidence required.");
    Instant previous = null;
    String directory = null;
    for (var sample : proof.samples()) {
      if (sample == null
          || sample.observedAt() == null
          || sample.observedAt().isAfter(proof.verifiedAt())
          || sample.observedAt().isBefore(proof.verifiedAt().minusSeconds(60))
          || previous != null && sample.observedAt().isBefore(previous.plusSeconds(5))
          || sample.health() == null
          || sample.health().isBlank())
        throw new IllegalStateException(
            "Three complete separated controller health samples required.");
      var task =
          jdbc.queryForMap(
              "select * from kf_external_controller_tasks where id=? and operation_id=? and"
                  + " resume_generation=? and command='INSPECT' and status='SUCCESS'",
              sample.inspectionId(),
              op.id(),
              op.generation());
      var nativeProof =
          tasks.candidate(
              frozen.nativePlan(),
              sample.inspectionId(),
              op.agentId(),
              (String) task.get("parameters"),
              tasks.report(sample.inspectionId()));
      evidence.runningCandidate(nativeProof);
      if (!"OBSERVER_STARTED".equals(ExternalControllerRecovery.checkpoint(nativeProof, true)))
        throw new IllegalStateException(
            "Controller tools or physical state are not stable for publication.");
      if (!"candidate:controller".equals(nativeProof.target()))
        throw new IllegalStateException("Controller publication needs a controller inspection.");
      if (p.layout() == KafkaExpansionWorkflow.Layout.SEPARATE) {
        var brokerTask =
            jdbc.queryForMap(
                "select * from kf_external_controller_tasks where id=? and operation_id=? and"
                    + " resume_generation=? and command='INSPECT' and status='SUCCESS'",
                sample.brokerInspectionId(),
                op.id(),
                op.generation());
        var brokerProof =
            tasks.candidate(
                frozen.nativePlan(),
                sample.brokerInspectionId(),
                op.agentId(),
                (String) brokerTask.get("parameters"),
                tasks.report(sample.brokerInspectionId()));
        if (!"candidate:broker".equals(brokerProof.target()))
          throw new IllegalStateException("Broker publication needs a broker inspection.");
        evidence.runningCandidate(brokerProof);
        if (!"OBSERVER_STARTED".equals(ExternalControllerRecovery.checkpoint(brokerProof, true))
            || brokerProof.observedAt().isBefore(sample.observedAt().minusSeconds(30)))
          throw new IllegalStateException("Fresh owned broker runtime and mounts are required.");
        var brokerDirectories = brokerProof.state().path("directoryIds").elements();
        var controllerDirectories = new HashSet<String>();
        nativeProof
            .state()
            .path("directoryIds")
            .elements()
            .forEachRemaining(v -> controllerDirectories.add(v.asText()));
        while (brokerDirectories.hasNext())
          if (controllerDirectories.contains(brokerDirectories.next().asText()))
            throw new IllegalStateException("Candidate directory identities overlap.");
      }
      var e = sample.controller();
      if (e == null
          || !e.voterVerified()
          || e.observerCaughtUp()
          || !op.id().equals(e.operationId())
          || !frozen.nativePlan().digest().equals(e.planDigest())
          || e.nodeId() != op.nodeId()
          || !p.voterDigest().equals(e.baselineDigest())
          || !nativeProof.metadataDirectoryId().equals(e.directoryId())
          || e.observedAt() == null
          || e.observedAt().isBefore(sample.observedAt().minusSeconds(30))
          || e.observedAt().isAfter(sample.observedAt().plusSeconds(5)))
        throw new IllegalStateException(
            "Live voter proof does not match owned controller storage.");
      if (directory != null && !directory.equals(e.directoryId()))
        throw new IllegalStateException("Candidate directory changed during verification.");
      directory = e.directoryId();
      previous = sample.observedAt();
    }
    if (previous.isBefore(proof.verifiedAt().minusSeconds(15)))
      throw new IllegalStateException("Final controller health sample is stale.");
  }

  public boolean anyClaimedMutation(UUID id) {
    return Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_controller_tasks where operation_id=? and"
                + " command in ('PREPARE','FORMAT','START','ADMIT') and claimed_at is not null)",
            Boolean.class,
            id));
  }

  public boolean claimedFormat(UUID id) {
    return Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_controller_tasks where operation_id=? and"
                + " command in ('FORMAT','START','ADMIT') and claimed_at is not null)",
            Boolean.class,
            id));
  }

  public boolean claimedFormat(UUID id, String role) {
    return Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_controller_tasks where operation_id=? and"
                + " command in ('FORMAT','RECONCILE_FORMAT','START','ADMIT') and claimed_at is not"
                + " null and parameters::jsonb->>'role'=?)",
            Boolean.class,
            id,
            role));
  }

  private Operation lock(UUID id, int generation) {
    var op = get(id);
    jdbc.queryForObject(
        "select id from kf_jobs where id=? for update", String.class, op.jobId().toString());
    jobs.assertExternalBrokerAdditionOwnsCluster(op.clusterId(), op.jobId());
    jdbc.queryForObject(
        "select id from kf_external_controller_operations where id=? for update", UUID.class, id);
    op = get(id);
    if (op.generation() != generation || Set.of("SUCCEEDED", "CANCELLED_SAFE").contains(op.state()))
      throw new IllegalStateException(
          "A newer attempt or terminal state owns this controller operation.");
    return op;
  }

  private void settle(UUID id) {
    jdbc.update(
        "update kf_external_controller_tasks set"
            + " status='FAILED',retired_at=clock_timestamp(),claim_token=null,result=? where"
            + " operation_id=? and (status='PENDING' or status='RUNNING' and deadline+interval '190"
            + " seconds'<clock_timestamp())",
        tasks.json(
            new ExternalControllerTasks.Result(
                null,
                "FAILED",
                null,
                "Retired execution lease; inspect actual controller state before continuing.")),
        id);
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_controller_tasks where operation_id=? and"
                + " status='RUNNING')",
            Boolean.class,
            id)))
      throw new IllegalStateException(
          "An earlier controller action may still be running. Wait for its result or execution"
              + " lease before resuming.");
  }

  private Accepted existing(UUID cluster, String key, String digest) {
    var rows =
        jdbc.queryForList(
            "select * from kf_external_controller_operations where cluster_id=? and request_key=?",
            cluster,
            key);
    if (rows.isEmpty()) return null;
    if (!digest.equals(rows.getFirst().get("request_digest")))
      throw new IllegalStateException("Request key belongs to different controller inputs.");
    return accepted(operation(rows.getFirst()));
  }

  private Operation operation(Map<String, Object> r) {
    return new Operation(
        (UUID) r.get("id"),
        (UUID) r.get("cluster_id"),
        (String) r.get("agent_id"),
        ((Number) r.get("node_id")).intValue(),
        UUID.fromString(r.get("job_id").toString()),
        (String) r.get("state"),
        ((Number) r.get("resume_generation")).intValue(),
        (String) r.get("last_error"),
        "SUCCEEDED".equals(r.get("state")),
        r.get("broker_node_id") == null ? null : ((Number) r.get("broker_node_id")).intValue());
  }

  private Accepted accepted(Operation op) {
    return new Accepted(op.id(), op.jobId(), op.state());
  }
}
