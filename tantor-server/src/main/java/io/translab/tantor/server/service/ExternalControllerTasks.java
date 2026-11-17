package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** External-only queue. RUNTIME/VALIDATE are observations; other actions require an owned job. */
@Service
@RequiredArgsConstructor
public class ExternalControllerTasks {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final ExternalAddNodeReadinessService readiness;
  private final ExternalBrokerAdditionGuard guard;
  private final JobService jobs;
  private final ExternalControllerRelease release;
  private final KafkaExpansionRuntimeInspection runtime;
  private final KafkaExpansionNativeEvidence evidence;
  private final AddNodeArtifactCatalog artifacts;

  public record Result(UUID claimToken, String status, JsonNode report, String message) {}

  public record View(UUID id, String command, String status, String result) {}

  @Transactional
  public UUID enqueue(
      UUID preview,
      UUID operation,
      UUID step,
      String agent,
      String command,
      Map<String, String> parameters,
      int generation) {
    if (!Set.of(
            "RUNTIME",
            "VALIDATE",
            "INSPECT",
            "PREPARE",
            "FORMAT",
            "START",
            "ADMIT",
            "RECONCILE_FORMAT")
        .contains(command)) throw new IllegalArgumentException("Unsupported controller action.");
    if (operation == null && !Set.of("RUNTIME", "VALIDATE").contains(command))
      throw new IllegalStateException("Controller mutation requires parent ownership.");
    if (operation != null) {
      var op =
          jdbc.queryForMap("select * from kf_external_controller_operations where id=?", operation);
      jdbc.queryForObject(
          "select id from kf_jobs where id=? for update",
          String.class,
          op.get("job_id").toString());
      jobs.assertExternalBrokerAdditionOwnsCluster(
          (UUID) op.get("cluster_id"), UUID.fromString(op.get("job_id").toString()));
      op =
          jdbc.queryForMap(
              "select * from kf_external_controller_operations where id=? for update", operation);
      if (((Number) op.get("resume_generation")).intValue() != generation
          || Set.of("RECOVERY_REQUIRED", "SUCCEEDED", "CANCELLED_SAFE").contains(op.get("state")))
        throw new IllegalStateException(
            "A newer attempt or terminal state owns this controller operation.");
    }
    UUID id = UUID.randomUUID();
    jdbc.update(
        "insert into"
            + " kf_external_controller_tasks(id,preview_id,operation_id,step_id,agent_id,command,parameters,deadline,resume_generation)"
            + " values(?,?,?,?,?,?,?,?,?)",
        id,
        preview,
        operation,
        step,
        agent,
        command,
        json(parameters),
        Timestamp.from(Instant.now().plusSeconds(180)),
        generation);
    return id;
  }

  public View get(UUID id) {
    var r = jdbc.queryForMap("select * from kf_external_controller_tasks where id=?", id);
    return new View(
        id, r.get("command").toString(), r.get("status").toString(), (String) r.get("result"));
  }

  public JsonNode report(UUID id) {
    var v = get(id);
    if (!"SUCCESS".equals(v.status()))
      throw new IllegalStateException("Successful controller observation required.");
    return parse(v.result(), Result.class).report();
  }

  @Transactional
  public Map<String, Object> claim(String agent, boolean execution) {
    return claim(agent, execution, false);
  }

  @Transactional
  public Map<String, Object> claim(String agent, boolean execution, boolean separateExecution) {
    readiness.requireControllerAgent(agent, false);
    var rows =
        jdbc.queryForList(
            "select t.*,p.cluster_id,p.context_hash as"
                + " preview_context_hash,p.binding_hash,p.common_request,p.plan,p.plan_digest,p.expires_at"
                + " from kf_external_controller_tasks t join kf_external_controller_previews p on"
                + " p.id=t.preview_id where t.agent_id=? and t.status='PENDING' and"
                + " t.deadline>clock_timestamp() and t.retired_at is null and (t.operation_id is"
                + " not null or p.expires_at>clock_timestamp()) order by t.created_at limit 1 for"
                + " update of t skip locked",
            agent);
    if (rows.isEmpty()) return Map.of();
    var r = rows.getFirst();
    UUID cluster = (UUID) r.get("cluster_id"), operation = (UUID) r.get("operation_id");
    String action = r.get("command").toString();
    boolean separate =
        r.get("plan") != null
            && KafkaExpansionPlan.read(
                        mapper, r.get("plan").toString(), r.get("plan_digest").toString())
                    .plan()
                    .layout()
                == KafkaExpansionWorkflow.Layout.SEPARATE;
    if (separate) readiness.requireSeparateAgent(agent, false);
    if (operation == null) {
      if (!instant(r.get("expires_at")).isAfter(Instant.now())) return Map.of();
      guard.assertMutable(cluster, null);
    } else {
      release.require(separate);
      var op =
          jdbc.queryForMap("select * from kf_external_controller_operations where id=?", operation);
      if (((Number) op.get("resume_generation")).intValue()
              != ((Number) r.get("resume_generation")).intValue()
          || Set.of("RECOVERY_REQUIRED", "SUCCEEDED", "CANCELLED_SAFE").contains(op.get("state")))
        return Map.of();
      jobs.assertExternalBrokerAdditionOwnsCluster(
          cluster, UUID.fromString(op.get("job_id").toString()));
      String expected =
          switch (action) {
            case "VALIDATE" -> "VALIDATING";
            case "PREPARE" -> "PREPARING";
            case "FORMAT", "RECONCILE_FORMAT" -> "FORMATTING";
            case "START" -> "STARTING";
            case "ADMIT" -> "ADMITTING";
            default -> op.get("state").toString();
          };
      if (!expected.equals(op.get("state"))) return Map.of();
    }
    boolean mutation = !Set.of("RUNTIME", "VALIDATE").contains(action);
    if (mutation && (!(separate ? separateExecution : execution) || operation == null))
      return Map.of();
    var common = parse(r.get("common_request"), ExternalAddNodeReadinessService.Request.class);
    readiness.assertControllerScope(cluster, common.agentId(), mutation && !separate);
    if (separate) readiness.requireSeparateAgent(common.agentId(), mutation);
    if (!Objects.equals(r.get("binding_hash"), readiness.executionBinding(cluster, common)))
      throw new IllegalStateException("Controller preview identity/connection binding changed.");
    Map<String, String> params = new TreeMap<>(parse(r.get("parameters"), Map.class));
    if ("RUNTIME".equals(action)) {
      var request =
          parse(
              params.get("runtime_inspection_request"),
              KafkaExpansionRuntimeInspection.Request.class);
      if (!agent.equals(request.identity().hostId())
          || !r.get("preview_context_hash").equals(request.identity().contextHash()))
        throw new IllegalStateException(
            "Runtime inspection is outside its frozen controller scope.");
      if (operation != null) {
        var frozen =
            KafkaExpansionPlan.read(mapper, (String) r.get("plan"), (String) r.get("plan_digest"));
        if (frozen.plan().participants().stream()
            .noneMatch(
                member ->
                    member.hostId().equals(agent)
                        && member.nodeId() == request.identity().nodeId()
                        && member.configPath().equals(request.identity().configPath())
                        && member.unit().equals(request.identity().serviceName())))
          throw new IllegalStateException("Runtime target is not a frozen participant.");
      }
    } else if (!agent.equals(common.agentId())
        || !Objects.equals(params.get("plan_digest"), r.get("plan_digest"))
        || !Objects.equals(params.get("expansion_plan"), r.get("plan")))
      throw new IllegalStateException(
          "Controller task parameters differ from the frozen server plan.");

    if (!"RUNTIME".equals(action)) {
      String role = params.get("role");
      if (!("controller".equals(role) || separate && "broker".equals(role))
          || "ADMIT".equals(action) && !"controller".equals(role))
        throw new IllegalStateException("Unreviewed external process role.");
      var frozen =
          KafkaExpansionPlan.read(mapper, (String) r.get("plan"), (String) r.get("plan_digest"));
      if (!frozen
          .plan()
          .artifactSha256()
          .equals(
              artifacts
                  .resolve(frozen.plan().kafkaVersion(), frozen.plan().artifactId().toString())
                  .checksum()))
        throw new IllegalStateException("Approved controller artifact changed.");
    }
    Instant expiry = Instant.now().plusSeconds(120);
    if (operation == null && instant(r.get("expires_at")).isBefore(expiry))
      expiry = instant(r.get("expires_at"));
    params.put("dispatch_expires_at", expiry.toString());
    UUID token = UUID.randomUUID();
    int updated =
        jdbc.update(
            "update kf_external_controller_tasks t set"
                + " status='RUNNING',claim_token=?,claimed_at=clock_timestamp(),deadline=?,parameters=?"
                + " where id=? and status='PENDING' and retired_at is null and (operation_id is"
                + " null or exists(select 1 from kf_external_controller_operations o join kf_jobs j"
                + " on j.id=o.job_id where o.id=t.operation_id and"
                + " o.resume_generation=t.resume_generation and o.state not in"
                + " ('RECOVERY_REQUIRED','SUCCEEDED','CANCELLED_SAFE') and"
                + " j.status='IN_PROGRESS'))",
            token,
            Timestamp.from(expiry),
            json(params),
            r.get("id"));
    if (updated != 1) return Map.of();
    return Map.of(
        "id",
        r.get("id"),
        "command",
        action,
        "parameters",
        params,
        "claimToken",
        token,
        "expiresAt",
        expiry.toString());
  }

  @Transactional
  public void complete(String agent, UUID id, Result result) {
    if (result == null
        || result.claimToken() == null
        || !Set.of("SUCCESS", "FAILED").contains(Objects.toString(result.status(), ""))
        || result.message() != null && result.message().length() > 2048
        || json(result).length() > 2097152)
      throw new IllegalArgumentException("Bounded controller result required.");
    var r =
        jdbc.queryForMap(
            "select * from kf_external_controller_tasks where id=? and agent_id=? and claim_token=?"
                + " and retired_at is null for update",
            id,
            agent,
            result.claimToken());
    if (Set.of("SUCCESS", "FAILED").contains(r.get("status"))) {
      if (!json(result).equals(r.get("result")))
        throw new IllegalStateException("Completed controller result cannot change.");
      return;
    }
    if (!"RUNNING".equals(r.get("status")))
      throw new IllegalStateException("Controller action was not claimed.");
    if ("SUCCESS".equals(result.status())) {
      var params = parse(r.get("parameters"), Map.class);
      String command = r.get("command").toString();
      var proof = result.report();
      Instant at = Instant.parse(proof.path("observedAt").asText());
      if (at.isBefore(instant(r.get("claimed_at")).minusSeconds(5))
          || at.isBefore(Instant.now().minusSeconds(180))
          || at.isAfter(Instant.now().plusSeconds(5))
          || at.isAfter(instant(r.get("deadline")).plusSeconds(5)))
        throw new IllegalStateException("Controller result lease/freshness mismatch.");
      if ("RUNTIME".equals(command))
        runtime.validate(
            parse(
                params.get("runtime_inspection_request"),
                KafkaExpansionRuntimeInspection.Request.class),
            mapper.convertValue(proof, KafkaExpansionRuntimeInspection.Report.class));
      else {
        var frozen =
            KafkaExpansionPlan.read(
                mapper,
                params.get("expansion_plan").toString(),
                params.get("plan_digest").toString());
        if ("VALIDATE".equals(command)) validateReady(frozen, proof);
        else candidate(frozen, id, agent, json(params), proof);
      }
    }
    jdbc.update(
        "update kf_external_controller_tasks set status=?,result=? where id=?",
        result.status(),
        json(result),
        id);
  }

  public KafkaExpansionNativeEvidence.Observation candidate(
      KafkaExpansionPlan.Frozen frozen,
      UUID taskId,
      String agent,
      String parameters,
      JsonNode report) {
    Task issued = new Task();
    issued.setId(taskId);
    issued.setHostId(agent);
    issued.setCommand(KafkaExpansionLedger.CANDIDATE);
    issued.setParameters(parameters);
    Task done = new Task();
    done.setId(taskId);
    done.setStatus("SUCCESS");
    done.setLogOutput(report.toString());
    String role = parse(parameters, Map.class).get("role").toString();
    return evidence.read(frozen, "candidate:" + role, issued, done);
  }

  public static void validateReady(KafkaExpansionPlan.Frozen frozen, JsonNode proof) {
    var p = frozen.plan();
    if (proof == null
        || !"external-dynamic-controller-ready/v1".equals(proof.path("schemaVersion").asText())
        || !p.operationId().toString().equals(proof.path("operationId").asText())
        || !p.candidateHostId().equals(proof.path("hostId").asText())
        || !frozen.digest().equals(proof.path("planDigest").asText())
        || !p.contextHash().equals(proof.path("contextHash").asText())
        || !proof.path("candidateReady").asBoolean()
        || !proof.path("payloadVerified").asBoolean())
      throw new IllegalStateException("Controller readiness does not match its frozen plan.");
  }

  public KafkaExpansionNativeEvidence.Observation observation(
      KafkaExpansionPlan.Frozen frozen, UUID id) {
    var r =
        jdbc.queryForMap(
            "select * from kf_external_controller_tasks where id=? and operation_id=? and"
                + " status='SUCCESS'",
            id,
            frozen.plan().operationId());
    return candidate(
        frozen, id, r.get("agent_id").toString(), r.get("parameters").toString(), report(id));
  }

  public Map<String, String> parameters(
      KafkaExpansionPlan.Frozen frozen, String action, String proof) {
    var params = new TreeMap<String, String>();
    params.put("expansion_plan", frozen.raw());
    params.put("plan_digest", frozen.digest());
    params.put("role", "controller");
    params.put("action", action);
    if (proof != null) params.put("controller_proof", proof);
    return params;
  }

  public Map<String, String> parameters(
      KafkaExpansionPlan.Frozen frozen, String action, String proof, String role) {
    var params = new TreeMap<>(parameters(frozen, action, proof));
    frozen.plan().process(role);
    params.put("role", role);
    return params;
  }

  public String json(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid controller evidence.", e);
    }
  }

  public <T> T parse(Object raw, Class<T> type) {
    try {
      return mapper.readValue(String.valueOf(raw), type);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid saved controller contract.", e);
    }
  }

  static Instant instant(Object v) {
    return v instanceof Timestamp t
        ? t.toInstant()
        : v instanceof OffsetDateTime t ? t.toInstant() : Instant.parse(v.toString());
  }
}
