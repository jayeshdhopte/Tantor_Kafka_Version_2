package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/** Transactional external ownership boundary; never waits for a task in a transaction. */
@Service
@RequiredArgsConstructor
public class ExternalBrokerAdditionLedger {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final JobService jobs;
  private final ExternalClusterRepository clusters;
  private final ExternalBrokerAdditionPlanning planning;
  private final ExternalAddNodeReadinessService readiness;

  public record Operation(
      UUID id,
      UUID clusterId,
      String agentId,
      UUID jobId,
      int nodeId,
      String state,
      UUID finalPreviewId,
      String error,
      ExternalBrokerAdditionPlanning.Payload payload) {}

  public record Accepted(UUID operationId, UUID jobId, String state) {}

  public record Result(UUID claimToken, String status, JsonNode report, String message) {}

  public record TaskView(UUID id, String command, String status, String result) {}

  public record View(
      UUID operationId,
      UUID clusterId,
      UUID jobId,
      String agentId,
      int nodeId,
      String state,
      String error,
      boolean inventoryPublished) {}

  static final Set<String> COMMANDS =
      Set.of(
          "VALIDATE_ADD_BROKER_PLAN",
          "PREPARE_ADD_BROKER",
          "FORMAT_ADD_BROKER",
          "START_ADD_BROKER",
          "INSPECT_ADD_BROKER");

  @Transactional
  public Accepted accept(
      UUID clusterId, UUID previewId, String contextHash, String planDigest, String key) {
    planning.requireRelease();
    if (previewId == null
        || key == null
        || !key.matches("[A-Za-z0-9_-]{16,128}")
        || contextHash == null
        || !contextHash.matches("[a-f0-9]{64}")
        || planDigest == null
        || !planDigest.matches("[a-f0-9]{64}"))
      throw new IllegalArgumentException(
          "Valid idempotency key and reviewed context/plan digest are required.");
    String requestDigest =
        BrokerAdditionLedger.hash(previewId + ":" + contextHash + ":" + planDigest);
    var prior =
        jdbc.queryForList(
            "select * from kf_external_broker_additions where cluster_id=? and request_key=?",
            clusterId,
            key);
    if (!prior.isEmpty()) {
      var row = prior.getFirst();
      if (!requestDigest.equals(row.get("request_digest")))
        throw conflict("Idempotency key belongs to different reviewed inputs.");
      return accepted(operation(row));
    }
    String agent =
        jdbc.queryForObject(
            "select agent_id from kf_external_add_node_previews where id=? and cluster_id=?",
            String.class,
            previewId,
            clusterId);
    jdbc.queryForObject(
        "select id from kf_discovery_agents where id=? for update", String.class, agent);
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_service_tasks where agent_id=? and status in"
                + " ('PENDING','RUNNING','IN_PROGRESS') and command<>'PRECHECK' union all select 1"
                + " from kf_external_kafka_tasks where agent_id=? and status in"
                + " ('PENDING','RUNNING','IN_PROGRESS') and command not in"
                + " ('READ_KAFKA_PROPERTY','READ_KAFKA_PROPERTIES','VERIFY_KAFKA_SERVICE'))",
            Boolean.class,
            agent,
            agent)))
      throw conflict(
          "An existing external task retains this candidate. Reconcile it before broker addition.");
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_add_node_tasks t join"
                + " kf_external_add_node_previews p on p.id=t.preview_id where p.agent_id=? and"
                + " t.status in ('PENDING','RUNNING') and t.deadline>now())",
            Boolean.class,
            agent)))
      throw conflict(
          "A read-only candidate probe is still active. Wait for completion before broker"
              + " submission.");
    var frozen = planning.freeze(clusterId, previewId, null, null);
    var payload = frozen.payload();
    if (!contextHash.equals(frozen.preview().contextHash()) || !planDigest.equals(payload.digest()))
      throw conflict(
          "Reviewed inputs or inherited configuration changed; rerun checks and review.");
    readiness.requireExecutionAgent(agent);
    clusters
        .findForServiceUpdate(clusterId)
        .orElseThrow(() -> conflict("External cluster not found."));
    // Another request with the same key may have committed while this one waited.
    prior =
        jdbc.queryForList(
            "select * from kf_external_broker_additions where cluster_id=? and request_key=?",
            clusterId,
            key);
    if (!prior.isEmpty()) {
      var row = prior.getFirst();
      if (!requestDigest.equals(row.get("request_digest")))
        throw conflict("Idempotency key conflict.");
      return accepted(operation(row));
    }
    readiness.assertContext(clusterId, payload.request(), contextHash);
    if (!frozen.preview().expiresAt().isAfter(Instant.now()))
      throw conflict("Readiness expired during acceptance.");
    Job job = new Job();
    job.setType(JobType.EXTERNAL_BROKER_ADDITION);
    job.setResourceKey("cluster:" + clusterId);
    job.setRollbackSupported(false);
    job.setPayload(
        json(
            Map.of(
                "operationId",
                payload.plan().operationId(),
                "clusterId",
                clusterId,
                "external",
                true)));
    var steps = new ArrayList<JobStep>();
    for (String name :
        List.of(
            "Repeat final external broker readiness",
            "Prepare candidate broker",
            "Format candidate broker storage",
            "Start candidate broker",
            "Verify broker and publish membership")) {
      var step = new JobStep();
      step.setStepOrder(steps.size() + 1);
      step.setName(name);
      step.setTargetId(payload.plan().hostIp());
      steps.add(step);
    }
    job = jobs.createJob(job, steps);
    jdbc.update(
        "insert into"
            + " kf_external_broker_additions(id,cluster_id,agent_id,job_id,request_key,request_digest,preview_id,node_id,plan)"
            + " values(?,?,?,?,?,?,?,?,?)",
        payload.plan().operationId(),
        clusterId,
        agent,
        job.getId().toString(),
        key,
        requestDigest,
        previewId,
        payload.plan().nodeId(),
        json(payload));
    return new Accepted(payload.plan().operationId(), job.getId(), "ACCEPTED");
  }

  public Accepted lookup(UUID clusterId, String key) {
    if (key == null || !key.matches("[A-Za-z0-9_-]{16,128}"))
      throw new IllegalArgumentException("Invalid submission key.");
    var rows =
        jdbc.queryForList(
            "select * from kf_external_broker_additions where cluster_id=? and request_key=?",
            clusterId,
            key);
    if (rows.isEmpty())
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND,
          "Broker submission is not recorded yet. Check again before submitting a new operation.");
    return accepted(operation(rows.getFirst()));
  }

  public Operation forJob(UUID id) {
    var rows =
        jdbc.queryForList(
            "select * from kf_external_broker_additions where job_id=?", id.toString());
    if (rows.isEmpty()) throw conflict("External broker operation not found.");
    return operation(rows.getFirst());
  }

  public View view(UUID clusterId, UUID id) {
    var op = get(id);
    if (!clusterId.equals(op.clusterId()))
      throw conflict("Operation does not belong to this external cluster.");
    return new View(
        op.id(),
        op.clusterId(),
        op.jobId(),
        op.agentId(),
        op.nodeId(),
        op.state(),
        op.error(),
        "SUCCEEDED".equals(op.state()));
  }

  public Operation get(UUID id) {
    var rows = jdbc.queryForList("select * from kf_external_broker_additions where id=?", id);
    if (rows.isEmpty()) throw conflict("External broker operation not found.");
    return operation(rows.getFirst());
  }

  @Transactional
  public void validating(UUID id) {
    var op = owned(id);
    if (!Set.of("ACCEPTED", "VALIDATING").contains(op.state()))
      throw conflict("Operation cannot restart final checks without reconciliation.");
    jdbc.update(
        "update kf_external_broker_additions set state='VALIDATING',updated_at=now() where id=?",
        id);
  }

  @Transactional
  public void finalPreview(UUID id, UUID preview) {
    var op = owned(id);
    if (!"VALIDATING".equals(op.state())
        || !Boolean.TRUE.equals(
            jdbc.queryForObject(
                "select exists(select 1 from kf_external_add_node_previews where id=? and"
                    + " cluster_id=? and agent_id=? and context_hash=?)",
                Boolean.class,
                preview,
                op.clusterId(),
                op.agentId(),
                op.payload().plan().contextHash())))
      throw conflict("Final readiness does not belong to this frozen operation.");
    jdbc.update(
        "update kf_external_broker_additions set final_preview_id=?,updated_at=now() where id=?",
        preview,
        id);
  }

  @Transactional
  public TaskView dispatch(UUID id, UUID step, String command, String intent) {
    if (!COMMANDS.contains(command))
      throw new IllegalArgumentException("Unsupported broker-only action.");
    var op = owned(id);
    String requiredIntent =
        switch (command) {
          case "VALIDATE_ADD_BROKER_PLAN" -> "VALIDATING";
          case "PREPARE_ADD_BROKER" -> "PREPARING";
          case "FORMAT_ADD_BROKER" -> "FORMATTING";
          case "START_ADD_BROKER" -> "STARTING";
          default -> op.state();
        };
    if (!requiredIntent.equals(intent))
      throw new IllegalArgumentException("Broker action intent mismatch.");
    planning.assertCurrent(op.clusterId(), op.payload());
    if ("PREPARE_ADD_BROKER".equals(command)) assertFinalFresh(op);
    var ownedStep =
        jobs.getSteps(op.jobId()).stream()
            .filter(s -> step.equals(s.getId()))
            .findFirst()
            .orElseThrow(() -> conflict("Foreign execution step."));
    if (!op.jobId().equals(ownedStep.getJob().getId())) throw conflict("Foreign execution step.");
    var existing =
        jdbc.queryForList(
            "select * from kf_external_broker_tasks where operation_id=? and step_id=? order by"
                + " created_at desc limit 1",
            id,
            step);
    if (!existing.isEmpty()) {
      var previous = task(existing.getFirst());
      if (!previous.command().equals(command) || "FAILED".equals(previous.status()))
        throw conflict("Reconcile the previous broker action before retrying.");
      return previous;
    }
    String expected =
        switch (command) {
          case "VALIDATE_ADD_BROKER_PLAN", "PREPARE_ADD_BROKER" -> "VALIDATING";
          case "FORMAT_ADD_BROKER" -> "PREPARED";
          case "START_ADD_BROKER" -> "FORMATTED";
          default -> op.state();
        };
    if (!expected.equals(op.state()))
      throw conflict("Broker checkpoint does not allow this action.");
    UUID task = UUID.randomUUID();
    jdbc.update(
        "insert into kf_external_broker_tasks(id,operation_id,step_id,command,parameters,deadline)"
            + " values(?,?,?,?,?,?)",
        task,
        id,
        step,
        command,
        json(planning.parameters(op.payload())),
        Timestamp.from(Instant.now().plusSeconds(300)));
    jdbc.update(
        "update kf_external_broker_additions set state=?,updated_at=now() where id=?", intent, id);
    jobs.appendLog(op.jobId(), "External broker task " + task + ": " + command);
    return new TaskView(task, command, "PENDING", null);
  }

  public TaskView task(UUID id) {
    var rows = jdbc.queryForList("select * from kf_external_broker_tasks where id=?", id);
    if (rows.isEmpty()) throw conflict("Broker task not found.");
    return task(rows.getFirst());
  }

  @Transactional
  public void checkpoint(UUID id, String state, String evidence) {
    var op = owned(id);
    String expected =
        switch (state) {
          case "PREPARED" -> "PREPARING";
          case "FORMATTED" -> "FORMATTING";
          case "STARTED_PENDING_VERIFICATION" -> "STARTING";
          default -> throw new IllegalArgumentException("Unsupported broker checkpoint");
        };
    if (!expected.equals(op.state())) throw conflict("Unexpected broker checkpoint transition.");
    jdbc.update(
        "update kf_external_broker_additions set state=?,last_evidence=?,updated_at=now() where"
            + " id=?",
        state,
        evidence,
        id);
  }

  @Transactional
  public void recovery(UUID id) {
    jdbc.update(
        "update kf_external_broker_additions set state='RECOVERY_REQUIRED',last_error='Execution"
            + " interrupted; preserve receipts and reservations for"
            + " reconciliation.',updated_at=now() where id=? and state not in"
            + " ('SUCCEEDED','CANCELLED_SAFE','STARTED_PENDING_VERIFICATION')",
        id);
  }

  @Transactional
  public Accepted cancelSafe(UUID clusterId, UUID id) {
    var op = get(id);
    if (!clusterId.equals(op.clusterId())) throw conflict("Foreign operation.");
    clusters.findForServiceUpdate(clusterId).orElseThrow();
    jdbc.queryForObject(
        "select id from kf_external_broker_additions where id=? for update", UUID.class, id);
    if ("CANCELLED_SAFE".equals(get(id).state())) return accepted(get(id));
    if (!Set.of("ACCEPTED", "VALIDATING", "RECOVERY_REQUIRED").contains(get(id).state()))
      throw conflict("This operation cannot be cancelled without reconciliation.");
    Integer mutations =
        jdbc.queryForObject(
            "select count(*) from kf_external_broker_tasks where operation_id=? and command in"
                + " ('PREPARE_ADD_BROKER','FORMAT_ADD_BROKER','START_ADD_BROKER')",
            Integer.class,
            id);
    if (mutations == null || mutations > 0)
      throw conflict(
          "Candidate mutation was dispatched; reservation must be retained for reconciliation.");
    jdbc.update(
        "update kf_external_broker_additions set state='CANCELLED_SAFE',updated_at=now() where"
            + " id=?",
        id);
    jobs.updateJobStatus(op.jobId(), JobStatus.FAILED);
    return accepted(get(id));
  }

  @Transactional
  public Map<String, Object> claim(String agentId) {
    planning.requireRelease();
    readiness.requireExecutionAgent(agentId);
    var rows =
        jdbc.queryForList(
            "select t.* from kf_external_broker_tasks t join kf_external_broker_additions o on"
                + " o.id=t.operation_id join kf_jobs j on j.id=o.job_id where o.agent_id=? and"
                + " o.state not in"
                + " ('RECOVERY_REQUIRED','SUCCEEDED','CANCELLED_SAFE','STARTED_PENDING_VERIFICATION')"
                + " and j.status='IN_PROGRESS' and t.status='PENDING' and t.deadline>now() order by"
                + " t.created_at limit 1 for update of t skip locked",
            agentId);
    if (rows.isEmpty()) return Map.of();
    var row = rows.getFirst();
    var op = get((UUID) row.get("operation_id"));
    planning.assertCurrent(op.clusterId(), op.payload());
    if ("PREPARE_ADD_BROKER".equals(row.get("command"))) assertFinalFresh(op);
    UUID token = UUID.randomUUID();
    jdbc.update(
        "update kf_external_broker_tasks set status='RUNNING',claim_token=? where id=?",
        token,
        row.get("id"));
    return Map.of(
        "id",
        row.get("id"),
        "command",
        row.get("command"),
        "claimToken",
        token,
        "parameters",
        parse(row.get("parameters"), Map.class));
  }

  @Transactional
  public void complete(String agentId, UUID taskId, Result result) {
    if (result == null
        || result.claimToken() == null
        || !Set.of("SUCCESS", "FAILED").contains(Objects.toString(result.status(), ""))
        || json(result).getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 131072)
      throw conflict("Invalid bounded broker result.");
    var rows =
        jdbc.queryForList(
            "select t.* from kf_external_broker_tasks t join kf_external_broker_additions o on"
                + " o.id=t.operation_id where t.id=? and o.agent_id=? and t.claim_token=? and"
                + " t.deadline>now() for update of t",
            taskId,
            agentId,
            result.claimToken());
    if (rows.isEmpty()) throw conflict("Unknown or expired broker claim.");
    var row = rows.getFirst();
    var op = get((UUID) row.get("operation_id"));
    if ("SUCCESS".equals(result.status()))
      validateProof(op, String.valueOf(row.get("command")), result.report());
    if (Set.of("SUCCESS", "FAILED").contains(row.get("status"))) {
      if (!json(result).equals(row.get("result")))
        throw conflict("Completed broker result cannot be replaced.");
      return;
    }
    if (!"RUNNING".equals(row.get("status"))) throw conflict("Broker task was not claimed.");
    jdbc.update(
        "update kf_external_broker_tasks set status=?,result=? where id=?",
        result.status(),
        json(result),
        taskId);
  }

  static void validateProof(Operation op, String command, JsonNode proof) {
    var p = op.payload();
    if (proof == null
        || !"add-broker/v1".equals(proof.path("schemaVersion").asText())
        || !op.id().toString().equals(proof.path("operationId").asText())
        || !op.agentId().equals(proof.path("hostId").asText())
        || !p.digest().equals(proof.path("planDigest").asText()))
      throw conflict("Broker receipt is not bound to the operation and frozen plan.");
    if ("VALIDATE_ADD_BROKER_PLAN".equals(command)) {
      Set<String> actions = new HashSet<>();
      proof.path("supportedActions").forEach(a -> actions.add(a.asText()));
      if (!proof.path("payloadVerified").asBoolean()
          || !proof.path("rootMutationSupported").asBoolean()
          || !p.plan().contextHash().equals(proof.path("contextHash").asText())
          || proof.path("supportedActions").size() != 4
          || !actions.equals(
              Set.of(
                  "PREPARE_ADD_BROKER",
                  "FORMAT_ADD_BROKER",
                  "START_ADD_BROKER",
                  "INSPECT_ADD_BROKER")))
        throw conflict("External broker capabilities are incomplete.");
      return;
    }
    if (proof.path("nodeId").asInt() != op.nodeId()
        || !p.plan().kafkaClusterId().equals(proof.path("kafkaClusterId").asText())
        || !p.plan().serviceName().equals(proof.path("serviceName").asText())
        || !p.plan().configPath().equals(proof.path("configPath").asText()))
      throw conflict("Broker identity, service or configuration receipt mismatch.");
    Instant at;
    try {
      at = Instant.parse(proof.path("observedAt").asText());
    } catch (Exception e) {
      throw conflict("Broker receipt time is invalid.");
    }
    if (at.isBefore(Instant.now().minusSeconds(300)) || at.isAfter(Instant.now().plusSeconds(5)))
      throw conflict("Broker receipt is stale.");
    String phase =
        switch (command) {
          case "PREPARE_ADD_BROKER" -> "PREPARED";
          case "FORMAT_ADD_BROKER" -> "FORMATTED";
          case "START_ADD_BROKER" -> "STARTED";
          default -> proof.path("phase").asText();
        };
    if (!phase.equals(proof.path("phase").asText()))
      throw conflict("Broker action checkpoint is not proven.");
    if (Set.of("FORMAT_ADD_BROKER", "START_ADD_BROKER").contains(command)
        && !"MATCHING".equals(proof.path("storageState").asText()))
      throw conflict("Broker storage identity is not proven.");
    if ("START_ADD_BROKER".equals(command) && !proof.path("serviceActive").asBoolean())
      throw conflict("Candidate broker is not active.");
  }

  public Result result(TaskView task) {
    return parse(task.result(), Result.class);
  }

  private void assertFinalFresh(Operation op) {
    if (op.finalPreviewId() == null
        || !Boolean.TRUE.equals(
            jdbc.queryForObject(
                "select exists(select 1 from kf_external_add_node_previews where id=? and"
                    + " cluster_id=? and agent_id=? and context_hash=? and expires_at>now())",
                Boolean.class,
                op.finalPreviewId(),
                op.clusterId(),
                op.agentId(),
                op.payload().plan().contextHash())))
      throw conflict(
          "Final readiness expired before candidate preparation; preserve the operation for"
              + " reconciliation.");
  }

  private Operation owned(UUID id) {
    var op = get(id);
    jobs.assertExternalBrokerAdditionOwnsCluster(op.clusterId(), op.jobId());
    jdbc.queryForObject(
        "select id from kf_external_broker_additions where id=? for update", UUID.class, id);
    op = get(id);
    if (Set.of("SUCCEEDED", "CANCELLED_SAFE", "RECOVERY_REQUIRED", "STARTED_PENDING_VERIFICATION")
        .contains(op.state()))
      throw conflict("Operation requires verification/reconciliation instead of another mutation.");
    return op;
  }

  private Operation operation(Map<String, Object> row) {
    return new Operation(
        (UUID) row.get("id"),
        (UUID) row.get("cluster_id"),
        String.valueOf(row.get("agent_id")),
        UUID.fromString(String.valueOf(row.get("job_id"))),
        ((Number) row.get("node_id")).intValue(),
        String.valueOf(row.get("state")),
        (UUID) row.get("final_preview_id"),
        (String) row.get("last_error"),
        parse(row.get("plan"), ExternalBrokerAdditionPlanning.Payload.class));
  }

  private TaskView task(Map<String, Object> row) {
    return new TaskView(
        (UUID) row.get("id"),
        String.valueOf(row.get("command")),
        String.valueOf(row.get("status")),
        (String) row.get("result"));
  }

  private Accepted accepted(Operation op) {
    return new Accepted(op.id(), op.jobId(), op.state());
  }

  private <T> T parse(Object raw, Class<T> type) {
    try {
      return mapper.readValue(String.valueOf(raw), type);
    } catch (Exception e) {
      throw conflict("Invalid saved external broker payload.");
    }
  }

  private String json(Object value) {
    return planning.json(value);
  }

  private static ResponseStatusException conflict(String message) {
    return new ResponseStatusException(HttpStatus.CONFLICT, message);
  }
}
