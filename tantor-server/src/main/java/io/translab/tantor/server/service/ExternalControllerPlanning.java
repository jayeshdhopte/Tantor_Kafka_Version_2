package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Server-collected runtime evidence; callers provide references, never a native plan. */
@Service
@RequiredArgsConstructor
public class ExternalControllerPlanning {
  private final ExternalAddNodeReadinessService readiness;
  private final ExternalControllerTasks tasks;
  private final DiscoveryAgentRepository agents;
  private final ExternalClusterRepository clusters;
  private final ExternalClusterNodeRepository nodes;
  private final ExternalKafkaManagement external;
  private final AddNodeArtifactCatalog catalog;
  private final KafkaExpansionQuorum quorum;
  private final ExternalControllerRelease release;
  private final KafkaExpansionRuntimeInspection runtime;
  private final AddNodeReadinessProperties commonPolicy;
  private final KafkaExpansionProperties policy;
  private final KafkaDeploymentProperties deployment;
  private final ArtifactRepositoryProperties artifactPolicy;
  private final AddNodeInspectionProperties scope;
  private final JobService jobs;
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;

  public record Request(
      UUID commonPreviewId, int controllerPort, Map<String, String> participantParcelDirs) {
    public Request {
      participantParcelDirs =
          participantParcelDirs == null ? Map.of() : Map.copyOf(participantParcelDirs);
    }
  }

  public record Participant(
      UUID serviceId, String agentId, String role, int nodeId, String status) {}

  public record View(
      UUID previewId,
      String contextHash,
      Instant expiresAt,
      String status,
      Integer proposedControllerId,
      String kafkaClusterId,
      String kafkaVersion,
      String serviceName,
      String configPath,
      boolean executionEnabled,
      List<String> blockers,
      List<Participant> participants,
      String planDigest) {}

  public record Frozen(
      KafkaExpansionPlan.Frozen nativePlan,
      ExternalAddNodeReadinessService.Request common,
      String binding,
      Instant expiresAt) {}

  @Transactional
  public View create(UUID clusterId, Request request) {
    if (request == null
        || request.commonPreviewId() == null
        || request.controllerPort() < 1
        || request.controllerPort() > 65535
        || request.participantParcelDirs().size() > 128)
      throw new IllegalArgumentException(
          "Current common checks and a controller listener are required.");
    var common = readiness.controllerEvidence(clusterId, request.commonPreviewId());
    var r = common.request();
    readiness.assertControllerScope(clusterId, r.agentId(), false);
    var members = nodes.findByClusterId(clusterId);
    if (members.isEmpty() || members.size() > 128)
      throw new IllegalStateException("Complete external participant discovery is required.");
    var memberAgents =
        members.stream()
            .map(ExternalClusterNode::getAgentId)
            .collect(java.util.stream.Collectors.toSet());
    if (!memberAgents.containsAll(request.participantParcelDirs().keySet()))
      throw new IllegalArgumentException(
          "Parcel mappings must refer to current participant agent IDs.");
    for (var member : members) readiness.requireControllerAgent(member.getAgentId(), false);
    var artifact =
        catalog.resolve(
            clusters.findById(clusterId).orElseThrow().getKafkaVersion(), r.artifactId());
    var endpoints = common.inspectionParameters().get("controllerEndpoints");
    if (!(endpoints instanceof List<?> list))
      throw new IllegalStateException("Observed controller routes are required.");
    var live = quorum.observe(list.stream().map(Object::toString).toList());
    var cluster = clusters.findById(clusterId).orElseThrow();
    if (live.features().getOrDefault("kraft.version", (short) 0) < 1
        || !cluster.getKafkaClusterId().equals(live.clusterId()))
      throw new IllegalStateException(
          "Only a proven finalized dynamic KRaft quorum is supported. Static clusters require a"
              + " separate maintenance workflow.");
    var known = new HashSet<Integer>();
    members.forEach(m -> known.add(Objects.requireNonNull(m.getNodeId())));
    var voterIds = new TreeMap<Integer, String>();
    live.voters().forEach((id, v) -> voterIds.put(id, v.directoryId()));
    quorum.healthyBaseline(
        new KafkaExpansionQuorum.Baseline(live.clusterId(), live.features(), voterIds, known, true),
        live,
        commonPolicy.getMaxQuorumLag(),
        Instant.now());
    UUID id = UUID.randomUUID();
    String binding = readiness.executionBinding(clusterId, r);
    Instant expires =
        Instant.now().plusSeconds(Math.min(300, Math.max(120, commonPolicy.getTtlSeconds())));
    String context =
        BrokerAdditionLedger.hash(
            tasks.json(List.of(id, request, binding, live.features(), voterIds, artifact)));
    // Common readiness observes conflicts in a separate transaction; do not
    // hold this cluster lock while asking that independent observer to acquire it.
    jobs.assertClusterMutable(clusterId);
    jdbc.update(
        "insert into"
            + " kf_external_controller_previews(id,cluster_id,agent_id,request,context_hash,binding_hash,common_request,expires_at)"
            + " values(?,?,?,?,?,?,?,?)",
        id,
        clusterId,
        r.agentId(),
        tasks.json(request),
        context,
        binding,
        tasks.json(r),
        Timestamp.from(expires));
    var approved = new ArrayList<String>();
    if (members.stream()
        .anyMatch(m -> m.getJmxExporterPort() != null && m.getJmxExporterPort() > 0))
      approved.add(
          catalog
              .resolveType("JMX_EXPORTER", null, artifactPolicy.getJmxExporterArtifactId())
              .checksum());
    for (var member : members) {
      String role = role(member), unit = Objects.toString(member.getSystemdService(), "");
      if (!unit.endsWith(".service")) unit += ".service";
      var identity =
          new AddNodeProcessInspectionService.Request(
              "add-node-process/v1",
              UUID.randomUUID(),
              member.getAgentId(),
              context,
              cluster.getKafkaClusterId(),
              cluster.getKafkaVersion(),
              member.getNodeId(),
              role,
              unit,
              member.getConfigFile(),
              Instant.now(),
              expires);
      String parcel =
          request.participantParcelDirs().getOrDefault(member.getAgentId(), r.parcelDir());
      AddNodeDeploymentSettings.approvedPath(parcel);
      var nativeRequest =
          new KafkaExpansionRuntimeInspection.Request(
              "add-node-runtime-inspection/v1",
              identity,
              parcel,
              "",
              artifact.checksum(),
              approved);
      tasks.enqueue(
          id,
          null,
          null,
          member.getAgentId(),
          "RUNTIME",
          Map.of(
              "runtime_inspection_request",
              tasks.json(nativeRequest),
              "service_id",
              member.getId().toString()),
          0);
    }
    return get(clusterId, id);
  }

  @Transactional
  public View get(UUID clusterId, UUID id) {
    jdbc.queryForObject(
        "select id from kf_external_controller_previews where id=? and cluster_id=? for update",
        UUID.class,
        id,
        clusterId);
    var row = row(clusterId, id);
    var common =
        tasks.parse(row.get("common_request"), ExternalAddNodeReadinessService.Request.class);
    Instant expires = ExternalControllerTasks.instant(row.get("expires_at"));
    if (!expires.isAfter(Instant.now()))
      return view(
          row,
          "EXPIRED",
          List.of(
              "Controller observations expired. Refresh common checks and request a new controller"
                  + " review."),
          List.of(),
          null);
    if (!row.get("binding_hash").equals(readiness.executionBinding(clusterId, common)))
      return view(
          row,
          "STALE",
          List.of("Cluster, participant, candidate or connection settings changed."),
          List.of(),
          null);
    var observations =
        jdbc.queryForList(
            "select * from kf_external_controller_tasks where preview_id=? and operation_id is null"
                + " order by created_at",
            id);
    var participants = new ArrayList<Participant>();
    boolean pending = false, failed = false;
    for (var task : observations)
      if ("RUNTIME".equals(task.get("command"))) {
        var parameters = tasks.parse(task.get("parameters"), Map.class);
        var request =
            tasks.parse(
                parameters.get("runtime_inspection_request"),
                KafkaExpansionRuntimeInspection.Request.class);
        participants.add(
            new Participant(
                UUID.fromString(parameters.get("service_id").toString()),
                task.get("agent_id").toString(),
                request.identity().role(),
                request.identity().nodeId(),
                task.get("status").toString()));
        pending |= Set.of("PENDING", "RUNNING").contains(task.get("status"));
        failed |= "FAILED".equals(task.get("status"));
      }
    if (failed)
      return view(
          row,
          "BLOCKED",
          List.of(
              "A participant runtime could not be verified against the approved package. Inspect"
                  + " its configuration, storage, unit and cached parcel."),
          participants,
          null);
    if (pending)
      return view(
          row,
          "CHECKING",
          List.of("Waiting for actual participant process and approved runtime observations."),
          participants,
          null);
    if (row.get("plan") == null) {
      var frozen = collect(clusterId, id, row, observations);
      jdbc.update(
          "update kf_external_controller_previews set plan=?,plan_digest=? where id=? and plan is"
              + " null",
          frozen.raw(),
          frozen.digest(),
          id);
      row = row(clusterId, id);
      tasks.enqueue(
          id,
          null,
          null,
          common.agentId(),
          "VALIDATE",
          tasks.parameters(frozen, "VALIDATE", null),
          0);
      return view(
          row,
          "CHECKING",
          List.of(
              "Checking the controller's ports, storage, Java, heap, resources and approved Kafka"
                  + " payload."),
          participants,
          frozen.plan());
    }
    var frozen =
        KafkaExpansionPlan.read(
            mapper, row.get("plan").toString(), row.get("plan_digest").toString());
    var checks = observations.stream().filter(t -> "VALIDATE".equals(t.get("command"))).toList();
    if (checks.size() != 1
        || Set.of("PENDING", "RUNNING").contains(checks.getFirst().get("status")))
      return view(
          row,
          "CHECKING",
          List.of("Waiting for selected controller profile checks."),
          participants,
          frozen.plan());
    if (!"SUCCESS".equals(checks.getFirst().get("status")))
      return view(
          row,
          "BLOCKED",
          List.of(
              "Controller profile readiness failed. Resolve the candidate checks and request fresh"
                  + " review."),
          participants,
          frozen.plan());
    var blockers = new ArrayList<String>();
    if (!release.enabled())
      blockers.add(
          "External controller release gate is held pending live broker/readiness acceptance.");
    try {
      readiness.requireControllerAgent(common.agentId(), true);
    } catch (RuntimeException disabled) {
      blockers.add("The candidate needs the separate controller execution opt-in.");
    }
    return view(row, "PASS", blockers, participants, frozen.plan());
  }

  public Frozen frozen(UUID clusterId, UUID id, boolean requirePass) {
    var view = get(clusterId, id);
    if (requirePass && (!"PASS".equals(view.status()) || !view.executionEnabled()))
      throw new IllegalStateException(
          "Fresh passing controller review and release approval are required.");
    var row = row(clusterId, id);
    return new Frozen(
        KafkaExpansionPlan.read(mapper, (String) row.get("plan"), (String) row.get("plan_digest")),
        tasks.parse(row.get("common_request"), ExternalAddNodeReadinessService.Request.class),
        (String) row.get("binding_hash"),
        ExternalControllerTasks.instant(row.get("expires_at")));
  }

  public String requestJson(UUID id) {
    return jdbc.queryForObject(
        "select request from kf_external_controller_previews where id=?", String.class, id);
  }

  public Frozen owned(UUID id) {
    var row = jdbc.queryForMap("select * from kf_external_controller_previews where id=?", id);
    return new Frozen(
        KafkaExpansionPlan.read(mapper, (String) row.get("plan"), (String) row.get("plan_digest")),
        tasks.parse(row.get("common_request"), ExternalAddNodeReadinessService.Request.class),
        (String) row.get("binding_hash"),
        ExternalControllerTasks.instant(row.get("expires_at")));
  }

  public void current(Frozen f) {
    var p = f.nativePlan().plan();
    release.require();
    readiness.assertControllerScope(p.clusterId(), p.candidateHostId(), true);
    if (!f.binding().equals(readiness.executionBinding(p.clusterId(), f.common())))
      throw new IllegalStateException("Frozen external controller scope changed.");
    if (!p.artifactSha256()
        .equals(catalog.resolve(p.kafkaVersion(), p.artifactId().toString()).checksum()))
      throw new IllegalStateException("Approved Kafka package changed.");
  }

  private KafkaExpansionPlan.Frozen collect(
      UUID clusterId, UUID id, Map<String, Object> row, List<Map<String, Object>> observations) {
    var common =
        tasks.parse(row.get("common_request"), ExternalAddNodeReadinessService.Request.class);
    var settings = common.deploymentSettings();
    var request = tasks.parse(row.get("request"), Request.class);
    var cluster = clusters.findById(clusterId).orElseThrow();
    var participants = new ArrayList<KafkaExpansionPlan.Participant>();
    Map<String, String> inherited = null;
    var members = nodes.findByClusterId(clusterId);
    var grouped = new TreeMap<String, List<Map<String, Object>>>();
    for (var task : observations)
      if ("RUNTIME".equals(task.get("command"))) {
        var report =
            mapper.convertValue(
                tasks.report((UUID) task.get("id")), KafkaExpansionRuntimeInspection.Report.class);
        grouped
            .computeIfAbsent(
                report.identity().hostId() + ":" + report.identity().serviceName(),
                key -> new ArrayList<>())
            .add(task);
      }
    var covered = new HashSet<UUID>();
    for (var group : grouped.values()) {
      var task = group.getFirst();
      var report =
          mapper.convertValue(
              tasks.report((UUID) task.get("id")), KafkaExpansionRuntimeInspection.Report.class);
      var identity = report.identity();
      if (!report
              .runtime()
              .archiveSha256()
              .equals(catalog.resolve(cluster.getKafkaVersion(), common.artifactId()).checksum())
          || !report.runtime().kafkaVersion().equals(cluster.getKafkaVersion()))
        throw new IllegalStateException(
            "Participant runtime no longer matches the approved cluster package.");
      var serviceIds = new ArrayList<UUID>();
      for (var entry : group) {
        var params = tasks.parse(entry.get("parameters"), Map.class);
        var nativeRequest =
            tasks.parse(
                params.get("runtime_inspection_request"),
                KafkaExpansionRuntimeInspection.Request.class);
        var other =
            mapper.convertValue(
                tasks.report((UUID) entry.get("id")), KafkaExpansionRuntimeInspection.Report.class);
        runtime.validate(nativeRequest, other);
        if (other.identity().pid() != identity.pid()
            || !other.identity().configSha256().equals(identity.configSha256())
            || !other.identity().metadata().equals(identity.metadata())
            || !other.runtime().proofSha256().equals(report.runtime().proofSha256()))
          throw new IllegalStateException("Participant process observations disagree.");
        UUID service = UUID.fromString(params.get("service_id").toString());
        if (!covered.add(service))
          throw new IllegalStateException("Duplicate participant evidence.");
        serviceIds.add(service);
      }
      var member =
          members.stream().filter(m -> serviceIds.contains(m.getId())).findFirst().orElseThrow();
      String host = readiness.participantAddress(clusterId, member.getAgentId(), member.getHost());
      participants.add(
          new KafkaExpansionPlan.Participant(
              serviceIds,
              identity.hostId(),
              host,
              identity.role(),
              identity.nodeId(),
              identity.serviceName(),
              identity.configPath(),
              identity.configSha256(),
              identity.unitSha256(),
              report.runtime().proofSha256(),
              report.runtime().runtimeHome(),
              report.javaAgents(),
              report.resolvedPaths(),
              identity.metadata().stream()
                  .map(m -> new KafkaExpansionPlan.Storage(m.path(), m.directoryId()))
                  .toList(),
              identity.pid(),
              identity.processStartTicks(),
              report.controllerPort()));
      if (identity.role().contains("controller")) {
        if (inherited != null && !inherited.equals(report.inherited()))
          throw new IllegalStateException(
              "Controller custom settings differ; resolve inheritance before addition.");
        inherited = report.inherited();
      }
    }
    if (!covered.equals(
            members.stream()
                .map(ExternalClusterNode::getId)
                .collect(java.util.stream.Collectors.toSet()))
        || inherited == null)
      throw new IllegalStateException(
          "Complete controller and broker process evidence is required.");
    var endpoints =
        participants.stream()
            .filter(p -> p.role().contains("controller"))
            .map(p -> endpoint(p.hostIp(), p.controllerPort()))
            .toList();
    var live = quorum.observe(endpoints);
    if (live.features().getOrDefault("kraft.version", (short) 0) < 1
        || !live.clusterId().equals(cluster.getKafkaClusterId()))
      throw new IllegalStateException("Dynamic quorum identity changed.");
    var voters = new ArrayList<KafkaExpansionPlan.Voter>();
    for (var voter : live.voters().values()) {
      var participant =
          participants.stream()
              .filter(p -> p.nodeId() == voter.id() && p.role().contains("controller"))
              .findFirst()
              .orElseThrow();
      voters.add(
          new KafkaExpansionPlan.Voter(
              voter.id(), voter.directoryId(), participant.hostIp(), participant.controllerPort()));
    }
    voters.sort(Comparator.comparingInt(KafkaExpansionPlan.Voter::nodeId));
    long proposed =
        Math.max(
                100,
                participants.stream()
                    .mapToInt(KafkaExpansionPlan.Participant::nodeId)
                    .max()
                    .orElse(100))
            + 1L;
    if (proposed > Integer.MAX_VALUE)
      throw new IllegalStateException("Controller identity space exhausted.");
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_add_node_owners where cluster_id=? and"
                + " node_id=? and state<>'CANCELLED_SAFE')",
            Boolean.class,
            clusterId,
            (int) proposed)))
      throw new IllegalStateException(
          "Proposed sequential controller ID is retained by another operation.");
    var artifact = catalog.resolve(cluster.getKafkaVersion(), common.artifactId());
    var observedJmx =
        members.stream()
            .filter(m -> Boolean.TRUE.equals(m.getIsController()))
            .map(m -> Objects.requireNonNullElse(m.getJmxExporterPort(), 0))
            .collect(java.util.stream.Collectors.toSet());
    if (observedJmx.size() != 1)
      throw new IllegalStateException(
          "Existing controller monitoring settings differ; resolve the inheritance policy first.");
    int jmxPort =
        policy.getControllerJmxPort() > 0
            ? policy.getControllerJmxPort()
            : observedJmx.iterator().next();
    KafkaExpansionPlan.Runtime monitoring;
    if (jmxPort == 0)
      monitoring = new KafkaExpansionPlan.Runtime(null, "", 0, deployment.getLimitNofile());
    else {
      var jmx =
          catalog.resolveType("JMX_EXPORTER", null, artifactPolicy.getJmxExporterArtifactId());
      monitoring =
          new KafkaExpansionPlan.Runtime(
              UUID.fromString(jmx.id()), jmx.checksum(), jmxPort, deployment.getLimitNofile());
    }
    var process =
        new KafkaExpansionPlan.Process(
            "controller",
            (int) proposed,
            request.controllerPort(),
            settings.controllerDataPaths(),
            settings.controllerMetadataPath(),
            settings.controllerLogPath(),
            settings.controllerHeapXmx(),
            "",
            inherited,
            monitoring);
    var plan =
        new KafkaExpansionPlan(
            "add-quorum-expansion/v2",
            id,
            clusterId,
            row.get("context_hash").toString(),
            common.agentId(),
            common.hostIp(),
            cluster.getKafkaClusterId(),
            cluster.getKafkaVersion(),
            KafkaExpansionWorkflow.Mode.DYNAMIC,
            KafkaExpansionWorkflow.Layout.CONTROLLER,
            live.features(),
            UUID.fromString(artifact.id()),
            artifact.checksum(),
            common.parcelDir(),
            settings.javaHome(),
            settings.runtimeUser(),
            List.of(process),
            participants,
            voters,
            scope.getExcludedHostIds(),
            scope.getExcludedHostIps(),
            false,
            new KafkaExpansionPlan.ResourcePolicy(
                commonPolicy.getMinAvailableMemoryMb(),
                policy.getOsReserveMb(),
                commonPolicy.getMinDiskGb(),
                commonPolicy.getMinInodes(),
                commonPolicy.getAllowedJavaMajors()),
            settings);
    quorum.healthyBaseline(baseline(plan), live, commonPolicy.getMaxQuorumLag(), Instant.now());
    return plan.freeze(mapper);
  }

  static KafkaExpansionQuorum.Baseline baseline(KafkaExpansionPlan p) {
    var voters = new TreeMap<Integer, String>();
    p.voters().forEach(v -> voters.put(v.nodeId(), v.directoryId()));
    return new KafkaExpansionQuorum.Baseline(
        p.kafkaClusterId(),
        p.features(),
        voters,
        p.participants().stream()
            .map(KafkaExpansionPlan.Participant::nodeId)
            .collect(java.util.stream.Collectors.toSet()),
        true);
  }

  private Map<String, Object> row(UUID cluster, UUID id) {
    return jdbc.queryForMap(
        "select * from kf_external_controller_previews where id=? and cluster_id=?", id, cluster);
  }

  private String failureMessage(Map<String, Object> task, String fallback) {
    try {
      String message =
          tasks.parse(task.get("result"), ExternalControllerTasks.Result.class).message();
      return message == null || message.isBlank() ? fallback : message;
    } catch (RuntimeException invalid) {
      return fallback;
    }
  }

  private View view(
      Map<String, Object> r,
      String status,
      List<String> blockers,
      List<Participant> participants,
      KafkaExpansionPlan p) {
    return new View(
        (UUID) r.get("id"),
        (String) r.get("context_hash"),
        ExternalControllerTasks.instant(r.get("expires_at")),
        status,
        p == null ? null : p.process("controller").nodeId(),
        p == null ? "" : p.kafkaClusterId(),
        p == null ? "" : p.kafkaVersion(),
        "controller.service",
        p == null ? "" : p.configPath("controller"),
        "PASS".equals(status) && blockers.isEmpty(),
        List.copyOf(blockers),
        List.copyOf(participants),
        (String) r.get("plan_digest"));
  }

  static String role(ExternalClusterNode n) {
    if (Boolean.TRUE.equals(n.getIsBroker()) && Boolean.TRUE.equals(n.getIsController()))
      return "broker,controller";
    if (Boolean.TRUE.equals(n.getIsBroker())) return "broker";
    if (Boolean.TRUE.equals(n.getIsController())) return "controller";
    throw new IllegalStateException("Unknown participant role.");
  }

  static String endpoint(String host, int port) {
    return (host.contains(":") ? "[" + host + "]" : host) + ":" + port;
  }
}
