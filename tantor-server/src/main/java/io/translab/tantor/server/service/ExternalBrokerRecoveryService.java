package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/** Recovery/publication transactions never wait for Kafka or an agent. */
@Service
@RequiredArgsConstructor
public class ExternalBrokerRecoveryService {
  private final ExternalBrokerAdditionLedger ledger;
  private final ExternalBrokerAdditionPlanning planning;
  private final JobService jobs;
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final ExternalClusterRepository clusters;

  @Transactional
  public ExternalBrokerAdditionLedger.Accepted resume(UUID clusterId, UUID id) {
    var op = ledger.get(id);
    if (!op.clusterId().equals(clusterId)) throw conflict("Foreign external broker operation.");
    jdbc.queryForObject(
        "select id from kf_jobs where id=? for update", String.class, op.jobId().toString());
    if ("SUCCEEDED".equals(op.state())) return accepted(op);
    if ("CANCELLED_SAFE".equals(op.state()))
      throw conflict("This operation was safely cancelled. Run a new readiness review.");
    planning.requireRelease();
    planning.assertCurrent(clusterId, op.payload());
    var job = jobs.getJob(op.jobId());
    if (Set.of(JobStatus.PENDING, JobStatus.IN_PROGRESS).contains(job.getStatus()))
      return accepted(op);
    settle(id);
    jobs.retryExternalBrokerJob(op.jobId());
    jdbc.queryForObject(
        "select id from kf_external_broker_additions where id=? for update", UUID.class, id);
    jdbc.update(
        "update kf_external_broker_additions set"
            + " resume_generation=resume_generation+1,state='RECONCILING',verification=null,verified_at=null,resume_inspection_id=null,last_error=null,updated_at=now()"
            + " where id=?",
        id);
    return accepted(ledger.get(id));
  }

  @Transactional
  public void beginReconciliation(UUID id, int generation) {
    var op = lock(id, generation);
    settle(id);
    jdbc.update(
        "update kf_external_broker_additions set"
            + " state='RECONCILING',verification=null,verified_at=null,updated_at=now() where id=?",
        id);
  }

  @Transactional
  public String reconciled(UUID id, int generation, UUID inspectionId) {
    var op = lock(id, generation);
    if (!"RECONCILING".equals(op.state())) throw conflict("Reconciliation ownership is required.");
    JsonNode proof = inspection(op, inspectionId);
    String checkpoint = ExternalBrokerRecovery.checkpoint(proof);
    int rank = ExternalBrokerRecovery.storageRank(checkpoint);
    Integer successful =
        jdbc.queryForObject(
            "select coalesce(max(case command when 'PREPARE_ADD_BROKER' then 1 when"
                + " 'FORMAT_ADD_BROKER' then 2 when 'START_ADD_BROKER' then 2 else 0 end),0) from"
                + " kf_external_broker_tasks where operation_id=? and status='SUCCESS'",
            Integer.class,
            id);
    if (successful != null && rank < successful)
      throw conflict(
          "Owned storage/receipt regressed behind a completed action; preserve it for"
              + " investigation.");
    Boolean claimedFormat =
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_broker_tasks where operation_id=? and command"
                + " in ('FORMAT_ADD_BROKER','START_ADD_BROKER') and claimed_at is not null)",
            Boolean.class,
            id);
    if (rank == 0 && Boolean.TRUE.equals(claimedFormat))
      throw conflict(
          "A prior formatter/start action was claimed but its durable storage/receipt is missing."
              + " Formatting cannot be repeated.");
    jdbc.update(
        "update kf_external_broker_additions set"
            + " state=?,resume_inspection_id=?,last_evidence=?,last_error=null,updated_at=now()"
            + " where id=?",
        checkpoint,
        inspectionId,
        proof.toString(),
        id);
    return checkpoint;
  }

  @Transactional
  public void beginVerification(UUID id, int generation) {
    var op = lock(id, generation);
    if (!Set.of("STARTED_PENDING_VERIFICATION", "VERIFYING", "PUBLISHING").contains(op.state()))
      throw conflict("An owned started broker is required for verification.");
    jdbc.update(
        "update kf_external_broker_additions set"
            + " state='VERIFYING',verification=null,verified_at=null,updated_at=now() where id=?",
        id);
  }

  @Transactional
  public void verified(UUID id, int generation, ExternalBrokerVerification.Evidence evidence) {
    var op = lock(id, generation);
    if (!"VERIFYING".equals(op.state())) throw conflict("Verification checkpoint is required.");
    ExternalBrokerVerification.validate(op, evidence);
    String raw = json(evidence);
    if (raw.getBytes(StandardCharsets.UTF_8).length > 131072)
      throw conflict("Verification evidence exceeds bound.");
    jdbc.update(
        "update kf_external_broker_additions set"
            + " state='PUBLISHING',verification=?,verified_at=?,updated_at=now() where id=?",
        raw,
        Timestamp.from(evidence.verifiedAt()),
        id);
  }

  @Transactional
  public ExternalBrokerAdditionLedger.Accepted publish(UUID id, int generation, UUID stepId) {
    var initial = ledger.get(id);
    if ("SUCCEEDED".equals(initial.state())) return accepted(initial);
    var op = lock(id, generation);
    if (!"PUBLISHING".equals(op.state()))
      throw conflict("Stable broker verification is required before publication.");
    var raw =
        jdbc.queryForObject(
            "select verification from kf_external_broker_additions where id=?", String.class, id);
    var evidence = parse(raw, ExternalBrokerVerification.Evidence.class);
    ExternalBrokerVerification.validate(op, evidence);
    var step =
        jobs.getSteps(op.jobId()).stream()
            .filter(s -> s.getId().equals(stepId))
            .findFirst()
            .orElseThrow(() -> conflict("Foreign publication step."));
    if (step.getStepOrder() == null || step.getStepOrder() != 5)
      throw conflict("Only the membership publication step may publish inventory.");
    planning.assertBinding(op.clusterId(), op.payload());
    var p = op.payload().plan();
    var cluster = clusters.findForServiceUpdate(op.clusterId()).orElseThrow();
    if (!Objects.equals(cluster.getKafkaClusterId(), p.kafkaClusterId())
        || !Objects.equals(cluster.getKafkaVersion(), p.kafkaVersion())
        || "DELETED".equalsIgnoreCase(cluster.getStatus()))
      throw conflict("External cluster identity/version changed before publication.");
    String owner =
        jdbc.queryForObject(
            "select coalesce(cluster_id::text,'') from kf_discovery_agents where id=? for update",
            String.class,
            op.agentId());
    if (!owner.isEmpty() && !owner.equals(op.clusterId().toString()))
      throw conflict("Candidate belongs to another cluster.");
    Integer conflicts =
        jdbc.queryForObject(
            "select count(*) from kf_external_cluster_nodes where (cluster_id=? and node_id=?) or"
                + " agent_id=?",
            Integer.class,
            op.clusterId(),
            op.nodeId(),
            op.agentId());
    if (conflicts == null || conflicts > 0)
      throw conflict("Candidate inventory already exists outside this publication transaction.");
    UUID node = UUID.nameUUIDFromBytes((id + ":broker").getBytes(StandardCharsets.UTF_8));
    jdbc.queryForObject(
        "select set_config('tantor.external_broker_operation',?,true)",
        String.class,
        id.toString());
    jdbc.update(
        "insert into"
            + " kf_external_cluster_nodes(id,cluster_id,host,agent_id,node_id,is_broker,is_controller,port,systemd_service,config_file,install_dir,data_dirs,log_dirs,jmx_exporter_port,can_manage_kafka,can_write_config,can_restart,last_seen)"
            + " values(?,?,?,?,?,true,false,?,?,?,?,?,?,?,?,false,false,?)",
        node,
        op.clusterId(),
        p.hostIp(),
        op.agentId(),
        op.nodeId(),
        p.brokerPort(),
        p.serviceName(),
        p.configPath(),
        p.installPath(),
        String.join(",", p.dataPaths()),
        p.logPath(),
        op.payload().runtime().jmxPort() > 0 ? op.payload().runtime().jmxPort() : null,
        true,
        Timestamp.from(evidence.verifiedAt()));
    jdbc.update(
        "update kf_discovery_agents set cluster_id=? where id=?", op.clusterId(), op.agentId());
    jdbc.update(
        "update kf_external_clusters set broker_count=(select count(*) from"
            + " kf_external_cluster_nodes where cluster_id=? and"
            + " is_broker=true),is_running=true,status='SUCCESS' where id=?",
        op.clusterId(),
        op.clusterId());
    jdbc.update(
        "update kf_external_broker_additions set"
            + " state='SUCCEEDED',published_node_id=?,last_error=null,updated_at=now() where id=?",
        node,
        id);
    // Job completion is in the same transaction: no publication/step callback gap.
    jobs.completeStep(
        stepId,
        "Verified unfenced broker, unchanged voters and three stable health samples; inventory"
            + " published atomically.");
    jobs.updateJobStatus(op.jobId(), JobStatus.SUCCESS);
    return accepted(ledger.get(id));
  }

  @Transactional
  public void step(UUID id, int generation, UUID stepId, JobStepStatus state, String output) {
    var op = lock(id, generation);
    if (jobs.getSteps(op.jobId()).stream().noneMatch(s -> s.getId().equals(stepId)))
      throw conflict("Foreign external broker step.");
    switch (state) {
      case IN_PROGRESS -> jobs.startStep(stepId);
      case SUCCESS -> jobs.completeStep(stepId, output);
      case FAILED -> jobs.failStep(stepId, output);
      default -> throw conflict("Unsupported external broker step transition.");
    }
  }

  public boolean hasPreparationInspection(UUID id) {
    var op = ledger.get(id);
    var rows =
        jdbc.queryForList(
            "select resume_inspection_id from kf_external_broker_additions where id=?", id);
    if (rows.isEmpty() || rows.getFirst().get("resume_inspection_id") == null) return false;
    return "VALIDATING"
        .equals(
            ExternalBrokerRecovery.checkpoint(
                inspection(op, (UUID) rows.getFirst().get("resume_inspection_id"))));
  }

  private void settle(UUID id) {
    // Agent v3 binds execution to the absolute lease. The extra grace also covers
    // an older parent context while native operation locks/formatter observation fence recovery.
    jdbc.update(
        "update kf_external_broker_tasks set"
            + " status='FAILED',claim_token=null,retired_at=clock_timestamp(),result=? where"
            + " operation_id=? and status='PENDING'",
        json(
            new ExternalBrokerAdditionLedger.Result(
                null, "FAILED", null, "Superseded unclaimed task; inspect before retry.")),
        id);
    jdbc.update(
        "update kf_external_broker_tasks set"
            + " status='FAILED',claim_token=null,retired_at=clock_timestamp(),result=? where"
            + " operation_id=? and status='RUNNING' and deadline+interval '190"
            + " seconds'<clock_timestamp()",
        json(
            new ExternalBrokerAdditionLedger.Result(
                null, "FAILED", null, "Execution lease expired; fresh inspection required.")),
        id);
    Boolean active =
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_broker_tasks where operation_id=? and"
                + " status='RUNNING')",
            Boolean.class,
            id);
    if (Boolean.TRUE.equals(active))
      throw conflict(
          "A previous candidate action may still be running. Wait for its result or execution lease"
              + " before resuming.");
  }

  private ExternalBrokerAdditionLedger.Operation lock(UUID id, int generation) {
    var op = ledger.get(id);
    jdbc.queryForObject(
        "select id from kf_jobs where id=? for update", String.class, op.jobId().toString());
    jobs.assertExternalBrokerAdditionOwnsCluster(op.clusterId(), op.jobId());
    jdbc.queryForObject(
        "select id from kf_external_broker_additions where id=? for update", UUID.class, id);
    op = ledger.get(id);
    ExternalBrokerAdditionLedger.fence(op, generation);
    if (Set.of("SUCCEEDED", "CANCELLED_SAFE").contains(op.state()))
      throw conflict("Operation is already complete/cancelled.");
    return op;
  }

  private JsonNode inspection(ExternalBrokerAdditionLedger.Operation op, UUID taskId) {
    var rows =
        jdbc.queryForList(
            "select result from kf_external_broker_tasks where id=? and operation_id=? and"
                + " resume_generation=? and command='INSPECT_ADD_BROKER' and status='SUCCESS'",
            taskId,
            op.id(),
            op.generation());
    if (rows.size() != 1) throw conflict("Fresh owned inspection is required.");
    var proof =
        parse(rows.getFirst().get("result"), ExternalBrokerAdditionLedger.Result.class).report();
    ExternalBrokerAdditionLedger.validateProof(op, "INSPECT_ADD_BROKER", proof);
    return proof;
  }

  private ExternalBrokerAdditionLedger.Accepted accepted(
      ExternalBrokerAdditionLedger.Operation op) {
    return new ExternalBrokerAdditionLedger.Accepted(op.id(), op.jobId(), op.state());
  }

  private String json(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      throw conflict("Invalid recovery evidence.");
    }
  }

  private <T> T parse(Object value, Class<T> type) {
    try {
      return mapper.readValue(String.valueOf(value), type);
    } catch (Exception e) {
      throw conflict("Invalid saved recovery evidence.");
    }
  }

  private static ResponseStatusException conflict(String message) {
    return new ResponseStatusException(HttpStatus.CONFLICT, message);
  }
}
