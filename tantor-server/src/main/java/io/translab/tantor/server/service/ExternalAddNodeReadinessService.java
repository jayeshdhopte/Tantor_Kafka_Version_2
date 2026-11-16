package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import io.translab.tantor.server.repository.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/** Phase 2 only: durable observation tasks, no deployment jobs or reservations. */
@Service
@RequiredArgsConstructor
public class ExternalAddNodeReadinessService {
  private final ExternalAddNodeInspectionService inspection;
  private final ExternalClusterRepository clusters;
  private final ExternalClusterNodeRepository nodes;
  private final DiscoveryAgentRepository agents;
  private final ExternalKafkaManagement external;
  private final AddNodeKafkaReadiness kafkaReadiness;
  private final KafkaAdminService kafka;
  private final AddNodeArtifactCatalog artifacts;
  private final AddNodeReadinessProperties policy;
  private final AddNodeInspectionProperties scope;
  private final ExternalAddNodeConflictObservation conflicts;
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  static final Set<String> AGENT_CODES =
      Set.of(
          "C09", "C10", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "B02", "B03",
          "B04", "B05", "B06", "B07", "B08", "B10", "B11");
  static final Set<String> BACKEND_CODES =
      Set.of(
          "HOST",
          "ADDRESS",
          "C04",
          "C01",
          "C02",
          "C03",
          "C05",
          "C06",
          "C07",
          "C08",
          "B09",
          "RACK",
          "ARTIFACT");

  static final Set<String> CONTROLLER_COMMON_CODES =
      Set.of(
          "HOST",
          "ADDRESS",
          "C04",
          "C01",
          "C02",
          "C03",
          "C05",
          "C06",
          "C07",
          "C08",
          "B09",
          "ARTIFACT",
          "C09",
          "C10",
          "C12",
          "C14",
          "C15",
          "C16",
          "C17",
          "C20");

  public record Request(
      String agentId,
      String hostIp,
      String parcelDir,
      String artifactId,
      int brokerPort,
      String brokerRack,
      AddNodeDeploymentSettings deploymentSettings) {}

  public record Preview(
      UUID id,
      UUID clusterId,
      String agentId,
      String contextHash,
      Instant checkedAt,
      Instant expiresAt,
      String status,
      List<Check> checks,
      boolean deploymentEnabled) {
    @com.fasterxml.jackson.annotation.JsonProperty("controllerCommonPassed")
    public boolean controllerCommonPassed() {
      return Set.of("PASS", "BLOCKED").contains(status)
          && expiresAt != null
          && expiresAt.isAfter(Instant.now())
          && checks != null
          && CONTROLLER_COMMON_CODES.stream()
              .allMatch(
                  code -> {
                    var values = checks.stream().filter(c -> code.equals(c.code())).toList();
                    return values.size() == 1 && "PASS".equals(values.getFirst().status());
                  });
    }
  }

  public record Result(
      UUID claimToken,
      String status,
      com.fasterxml.jackson.databind.JsonNode report,
      String message) {}

  @Transactional
  public Preview create(UUID clusterId, Request request) {
    return create(clusterId, request, null);
  }

  @Transactional
  public Preview create(UUID clusterId, Request request, UUID ownerJob) {
    validate(request);
    if (json(request).getBytes(StandardCharsets.UTF_8).length > 65536)
      throw new IllegalArgumentException("Readiness request exceeds the bounded profile contract.");
    var cluster = cluster(clusterId);
    var candidate = agent(request.agentId());
    // Serializes candidate previews without assigning/reserving the host.
    jdbc.queryForObject(
        "select id from kf_discovery_agents where id=? for update",
        String.class,
        request.agentId());
    if (Boolean.TRUE.equals(
        jdbc.queryForObject(
            "select exists(select 1 from kf_external_add_node_tasks t join"
                + " kf_external_add_node_previews p on p.id=t.preview_id where p.agent_id=? and"
                + " t.status in ('PENDING','RUNNING') and t.deadline>now())",
            Boolean.class,
            request.agentId())))
      throw conflict(
          "A read-only inspection is already active for this candidate. Wait for completion or"
              + " expiry before rerunning.");
    UUID id = UUID.randomUUID();
    Instant started = Instant.now(), expires = started.plusSeconds(policy.getTtlSeconds());
    String context = context(clusterId, request);
    var checks = new ArrayList<Check>();
    var observed =
        (ownerJob == null
                ? inspection.candidates(clusterId)
                : inspection.candidates(clusterId, ownerJob))
            .candidates().stream()
                .filter(c -> c.agentId().equals(request.agentId()))
                .findFirst()
                .orElseThrow(() -> conflict("Candidate is not enrolled."));
    checks.add(
        check(
            "HOST",
            "External host ownership and opt-in",
            observed.blockers().isEmpty(),
            observed.blockers()));
    boolean address = ips(candidate).contains(canonical(request.hostIp()));
    checks.add(
        check(
            "ADDRESS",
            "Registered candidate address",
            address,
            address ? "Matched registered interface" : "Address mismatch"));
    try {
      if (ownerJob == null) conflicts.verify(clusterId);
      else conflicts.verifyOwned(clusterId, ownerJob);
      checks.add(check("C04", "Conflicting cluster operations", true, "No conflicting operation"));
    } catch (RuntimeException e) {
      checks.add(
          check(
              "C04",
              "Conflicting cluster operations",
              false,
              "A conflicting cluster operation exists"));
    }
    String staticVoters = "";
    AddNodeKafkaReadiness.Observation live = null;
    AddNodeArtifactCatalog.Artifact artifact = null;
    if (checks.stream().allMatch(c -> "PASS".equals(c.status()))) {
      try {
        guardCluster(clusterId, cluster);
        var snapshot = external.resolve(clusterId);
        snapshot.setKafkaClusterId(cluster.getKafkaClusterId());
        snapshot.setBootstrapServers(cluster.getBootstrapServers());
        Map<String, Object> config = new HashMap<>();
        // Observe static discovery from effective broker configuration, never invent voters.
        var resources =
            nodes.findByClusterId(clusterId).stream()
                .filter(n -> Boolean.TRUE.equals(n.getIsBroker()) && n.getNodeId() != null)
                .map(n -> new ConfigResource(ConfigResource.Type.BROKER, n.getNodeId().toString()))
                .distinct()
                .toList();
        try {
          var configurations =
              kafka
                  .getAdminClient(clusterId)
                  .describeConfigs(resources)
                  .all()
                  .get(policy.getTimeoutSeconds(), TimeUnit.SECONDS);
          Set<String> voters = new HashSet<>();
          for (var resource : resources) {
            var entry = configurations.get(resource).get("controller.quorum.voters");
            voters.add(entry == null ? "" : Objects.toString(entry.value(), ""));
          }
          if (voters.size() == 1 && !voters.iterator().next().isBlank()) {
            String value = voters.iterator().next();
            for (String voter : value.split(","))
              guardEndpoint(voter.substring(voter.indexOf('@') + 1), clusterId);
            config.put("controller_quorum_voters", value);
            staticVoters = value;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw conflict("Readiness observation interrupted.");
        } catch (ResponseStatusException e) {
          throw e;
        } catch (Exception unavailable) {
          /* Live quorum check remains UNKNOWN/FAIL if static routes cannot be proven. */
        }
        live = kafkaReadiness.observe(snapshot, config);
        checks.addAll(live.checks());
        for (String endpoint : live.brokerEndpoints()) guardEndpoint(endpoint, clusterId);
        for (String endpoint : live.controllerEndpoints()) guardEndpoint(endpoint, clusterId);
        checks.add(
            check(
                "RACK",
                "Live broker rack policy",
                live.rackObserved()
                    && (request.brokerRack().isEmpty()
                        ? live.brokerRacks().equals(Set.of(""))
                        : live.brokerRacks().contains(request.brokerRack())),
                "Requested rack must match the observed policy; a different rack requires explicit"
                    + " policy support"));
      } catch (ResponseStatusException e) {
        throw e;
      } catch (RuntimeException e) {
        checks.add(
            unknown(
                "LIVE",
                "Live cluster observations",
                "Cluster identity, participants or Kafka observations could not be verified."));
      }
      try {
        artifact = artifacts.resolve(cluster.getKafkaVersion(), request.artifactId());
        checks.add(
            check(
                "ARTIFACT",
                "Approved matching Kafka artifact",
                true,
                Map.of(
                    "id",
                    artifact.id(),
                    "version",
                    artifact.version(),
                    "sha256",
                    artifact.checksum())));
      } catch (RuntimeException e) {
        checks.add(
            unknown(
                "ARTIFACT",
                "Approved matching Kafka artifact",
                "A unique available artifact with matching version and SHA-256 is required."));
      }
    }
    if (live == null)
      for (String code : List.of("C01", "C02", "C03", "C05", "C06", "C07", "C08", "B09"))
        checks.add(
            unknown(
                code,
                "Live Kafka observation",
                "Resolve candidate ownership, scope and live-cluster observations, then rerun."));
    jdbc.update(
        "insert into"
            + " kf_external_add_node_previews(id,cluster_id,agent_id,context_hash,request,checks,created_at,expires_at)"
            + " values(?,?,?,?,?,?,?,?)",
        id,
        clusterId,
        request.agentId(),
        context,
        json(request),
        json(checks),
        Timestamp.from(started),
        Timestamp.from(expires));
    if (live != null
        && artifact != null
        && checks.stream().allMatch(c -> "PASS".equals(c.status()))) {
      var p = request.deploymentSettings();
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("schemaVersion", "add-node/v1");
      payload.put("hostId", candidate.getId());
      payload.put("hostIp", request.hostIp());
      payload.put("hostIdentityUnique", true);
      payload.put("contextHash", context);
      payload.put("contextStartedAt", started.toString());
      payload.put("expiresAt", expires.toString());
      payload.put("deploymentSettings", p);
      payload.put("configDirectory", p.configDirectory());
      payload.put("installPath", p.installPath());
      payload.put("javaHome", p.javaHome());
      payload.put("runtimeUser", p.runtimeUser());
      payload.put("dataPaths", p.brokerDataPaths());
      payload.put("metadataPath", p.brokerMetadataPath());
      payload.put("logPath", p.brokerLogPath());
      payload.put("heapXmx", p.brokerHeapXmx());
      payload.put("kafkaVersion", cluster.getKafkaVersion());
      payload.put("kafkaClusterId", cluster.getKafkaClusterId());
      payload.put("brokerPort", String.valueOf(request.brokerPort()));
      var ports = new TreeSet<String>();
      ports.add(String.valueOf(request.brokerPort()));
      nodes.findByClusterId(clusterId).stream()
          .filter(
              n ->
                  Boolean.TRUE.equals(n.getIsBroker())
                      && n.getJmxExporterPort() != null
                      && n.getJmxExporterPort() > 0)
          .forEach(n -> ports.add(n.getJmxExporterPort().toString()));
      payload.put("ports", List.copyOf(ports));
      payload.put("parcelDir", request.parcelDir());
      payload.put("artifactChecksum", artifact.checksum());
      payload.put("securityMode", "PLAINTEXT");
      payload.put("quorumMode", live.quorumMode());
      payload.put("quorumVoters", staticVoters);
      payload.put("controllerEndpoints", live.controllerEndpoints());
      payload.put("brokerEndpoints", live.brokerEndpoints());
      payload.put("limitNofile", 100000);
      payload.put("minAvailableMemoryMb", policy.getMinAvailableMemoryMb());
      payload.put("minDiskGb", policy.getMinDiskGb());
      payload.put("minInodes", policy.getMinInodes());
      payload.put("allowedJavaMajors", policy.getAllowedJavaMajors());
      var peers = new ArrayList<Map<String, Object>>();
      Set<String> clientIds = new HashSet<>();
      int brokerCount = 0, controllerCount = 0;
      Map<String, Set<String>> roles = new TreeMap<>();
      for (var n : nodes.findByClusterId(clusterId)) {
        var rs = roles.computeIfAbsent(n.getAgentId(), key -> new TreeSet<>());
        if (Boolean.TRUE.equals(n.getIsBroker())) rs.add("broker");
        if (Boolean.TRUE.equals(n.getIsController())) rs.add("controller");
      }
      for (String host : policy.getClientProbeHostIds()) {
        var matches =
            agents.findAll().stream()
                .filter(a -> host.equals(a.getId()) || host.equals(a.getHostname()))
                .toList();
        if (matches.size() != 1)
          throw conflict("Resolve each configured client probe host to one external agent.");
        roles.computeIfAbsent(matches.getFirst().getId(), key -> new TreeSet<>()).add("client");
        clientIds.add(matches.getFirst().getId());
      }
      if (roles.size() > 128) throw conflict("Readiness probe scope exceeds 128 enrolled hosts.");
      for (var entry : roles.entrySet()) {
        var peer = agent(entry.getKey());
        requireAgent(peer);
        guardAgent(peer);
        List<String> peerIps = ips(peer);
        if (peerIps.isEmpty()) throw conflict("Peer has no registered numeric IP.");
        String token = UUID.randomUUID().toString();
        String role = String.join(",", entry.getValue()).toUpperCase(Locale.ROOT);
        peers.add(
            Map.of(
                "hostId",
                peer.getId(),
                "sourceIp",
                peerIps.getFirst(),
                "roles",
                role,
                "token",
                token));
        if (entry.getValue().contains("broker")) brokerCount++;
        if (entry.getValue().contains("controller")) controllerCount++;
        enqueue(
            id,
            peer.getId(),
            "PROBE",
            Map.of(
                "hostId",
                peer.getId(),
                "targetIp",
                request.hostIp(),
                "targetPort",
                String.valueOf(request.brokerPort()),
                "token",
                token,
                "expiresAt",
                expires.toString()),
            expires);
      }
      payload.put("peerProbes", peers);
      payload.put("expectedBrokerProbeCount", brokerCount);
      payload.put("expectedControllerProbeCount", controllerCount);
      payload.put("expectedClientProbeCount", Math.max(1, clientIds.size()));
      enqueue(id, candidate.getId(), "INSPECT", payload, expires);
    }
    return get(clusterId, id, ownerJob);
  }

  @Transactional
  public Preview get(UUID clusterId, UUID id) {
    return get(clusterId, id, null);
  }

  @Transactional
  public Preview get(UUID clusterId, UUID id, UUID ownerJob) {
    var rows =
        jdbc.queryForList(
            "select * from kf_external_add_node_previews where id=? and cluster_id=?",
            id,
            clusterId);
    if (rows.isEmpty())
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Readiness preview not found.");
    var row = rows.getFirst();
    Request request = parse(row.get("request"), Request.class);
    Instant started = instant(row.get("created_at")), expires = instant(row.get("expires_at"));
    String hash = String.valueOf(row.get("context_hash"));
    List<Check> checks = checks(String.valueOf(row.get("checks")));
    String state = "BLOCKED";
    if (!expires.isAfter(Instant.now())) state = "EXPIRED";
    else if (!hash.equals(context(clusterId, request))) state = "STALE";
    else {
      var tasks =
          jdbc.queryForList(
              "select agent_id,command,status,result from kf_external_add_node_tasks where"
                  + " preview_id=?",
              id);
      boolean pending =
          tasks.stream().anyMatch(t -> Set.of("PENDING", "RUNNING").contains(t.get("status")));
      for (var task : tasks) {
        try {
          var participant = agent(String.valueOf(task.get("agent_id")));
          requireAgent(participant);
          guardAgent(participant);
        } catch (RuntimeException unavailable) {
          checks.add(
              unknown(
                  "PEER_" + task.get("agent_id"),
                  "Current external agent capability",
                  "A candidate or peer agent is offline, outside scope or missing fresh opt-in"
                      + " observation."));
        }
        if ("INSPECT".equals(task.get("command")) && "SUCCESS".equals(task.get("status"))) {
          var report = parse(task.get("result"), Result.class);
          checks.addAll(checks(report.report().path("checks").toString()));
        } else if ("FAILED".equals(task.get("status")))
          checks.add(
              unknown(
                  "TASK",
                  "Read-only agent task",
                  "Agent inspection did not complete. Rerun after fixing the reported host profile"
                      + " or connectivity."));
      }
      var observed =
          (ownerJob == null
                  ? inspection.candidates(clusterId)
                  : inspection.candidates(clusterId, ownerJob))
              .candidates().stream().filter(c -> c.agentId().equals(request.agentId())).findFirst();
      if (observed.isEmpty() || !observed.get().blockers().isEmpty())
        checks.add(
            unknown(
                "FRESH",
                "Fresh candidate observation",
                "Candidate heartbeat, ownership or opt-in inspection is no longer current."));
      try {
        if (ownerJob == null) conflicts.verify(clusterId);
        else conflicts.verifyOwned(clusterId, ownerJob);
      } catch (RuntimeException e) {
        checks.add(
            check(
                "CONFLICT",
                "Current conflicting operations",
                false,
                "A cluster operation started after these checks."));
      }
      Set<String> reported = new HashSet<>();
      checks.forEach(c -> reported.add(c.code()));
      if (pending) state = "CHECKING";
      else if (reported.containsAll(AGENT_CODES)
          && reported.containsAll(BACKEND_CODES)
          && checks.stream().allMatch(c -> "PASS".equals(c.status()))) state = "PASS";
    }
    Set<String> reported = new HashSet<>();
    checks.forEach(c -> reported.add(c.code()));
    for (String code : new TreeSet<>(AGENT_CODES))
      if (!reported.contains(code))
        checks.add(
            unknown(
                code,
                "Candidate readiness observation",
                "No current agent evidence for this check."));
    for (String code : new TreeSet<>(BACKEND_CODES))
      if (!reported.contains(code))
        checks.add(
            unknown(
                code,
                "Backend readiness observation",
                "Resolve preceding blockers and rerun this check."));
    return new Preview(
        id,
        clusterId,
        request.agentId(),
        hash,
        started,
        expires,
        state,
        List.copyOf(checks),
        false);
  }

  @Transactional
  public Map<String, Object> claim(String agentId) {
    var a = agent(agentId);
    requireAgent(a);
    guardAgent(a);
    var rows =
        jdbc.queryForList(
            "select t.*,p.cluster_id,p.context_hash,p.request from kf_external_add_node_tasks t"
                + " join kf_external_add_node_previews p on p.id=t.preview_id where t.agent_id=?"
                + " and t.status='PENDING' and t.deadline>now() order by t.created_at limit 1 for"
                + " update of t skip locked",
            agentId);
    if (rows.isEmpty()) return Map.of();
    var row = rows.getFirst();
    if (!row.get("context_hash")
        .equals(context((UUID) row.get("cluster_id"), parse(row.get("request"), Request.class))))
      return Map.of();
    UUID token = UUID.randomUUID();
    jdbc.update(
        "update kf_external_add_node_tasks set status='RUNNING',claim_token=? where id=?",
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
  public void complete(String agentId, UUID id, Result result) {
    requireAgent(agent(agentId));
    if (result == null
        || result.claimToken() == null
        || !Set.of("SUCCESS", "FAILED").contains(Objects.toString(result.status(), ""))
        || json(result).getBytes(StandardCharsets.UTF_8).length > 131072)
      throw conflict("Invalid bounded inspection result.");
    var rows =
        jdbc.queryForList(
            "select * from kf_external_add_node_tasks where id=? and agent_id=? and claim_token=?"
                + " and deadline>now() for update",
            id,
            agentId,
            result.claimToken());
    if (rows.isEmpty()) throw conflict("Unknown or expired inspection claim.");
    var row = rows.getFirst();
    if ("SUCCESS".equals(result.status()) && "INSPECT".equals(row.get("command")))
      validateReport(result, agentId, parse(row.get("parameters"), Map.class));
    if (Set.of("SUCCESS", "FAILED").contains(row.get("status"))) {
      if (!json(result).equals(row.get("result")))
        throw conflict("A completed inspection result cannot be replaced.");
      return;
    }
    if (!"RUNNING".equals(row.get("status"))) throw conflict("Inspection was not claimed.");
    jdbc.update(
        "update kf_external_add_node_tasks set status=?,result=? where id=?",
        result.status(),
        json(result),
        id);
  }

  static void validateReport(Result result, String agentId, Map<?, ?> request) {
    var report = result.report();
    if (report == null
        || !"add-node/v1".equals(report.path("schemaVersion").asText())
        || !agentId.equals(report.path("hostId").asText())
        || !Objects.equals(request.get("contextHash"), report.path("contextHash").asText())
        || !report.path("checks").isArray())
      throw conflict("Inspection evidence identity/context mismatch.");
    Set<String> codes = new HashSet<>();
    for (var c : report.path("checks")) {
      String code = c.path("code").asText();
      Instant at;
      try {
        at = Instant.parse(c.path("observedAt").asText());
      } catch (Exception e) {
        throw conflict("Inspection evidence time is invalid.");
      }
      if (!AGENT_CODES.contains(code)
          || !codes.add(code)
          || !Set.of("PASS", "FAIL", "UNKNOWN").contains(c.path("status").asText())
          || at.isBefore(Instant.parse(request.get("contextStartedAt").toString()).minusSeconds(5))
          || at.isAfter(Instant.now().plusSeconds(5)))
        throw conflict("Inspection evidence is incomplete, duplicated or stale.");
    }
    if (!codes.equals(AGENT_CODES)) throw conflict("Required readiness evidence is missing.");
  }

  static void validate(Request r) {
    if (r == null
        || r.agentId() == null
        || !r.agentId().matches("[A-Za-z0-9_.:-]{1,255}")
        || r.hostIp() == null
        || !r.hostIp().matches("[0-9a-fA-F:.]{3,64}")
        || canonical(r.hostIp()).isEmpty()
        || r.brokerPort() < 1024
        || r.brokerPort() > 65535
        || r.brokerRack() == null
        || !r.brokerRack().matches("[A-Za-z0-9_.-]{0,128}")
        || r.deploymentSettings() == null)
      throw new IllegalArgumentException(
          "Provide an explicit registered host, listener and candidate profile.");
    var profile = r.deploymentSettings();
    for (String value :
        new String[] {
          profile.installPath(),
          profile.configDirectory(),
          profile.javaHome(),
          profile.runtimeUser(),
          profile.brokerMetadataPath(),
          profile.brokerLogPath(),
          profile.brokerHeapXmx(),
          profile.controllerMetadataPath(),
          profile.controllerLogPath(),
          profile.controllerHeapXmx()
        })
      if (value == null)
        throw new IllegalArgumentException("Complete all required candidate profile fields.");
    profile.validate();
    AddNodeDeploymentSettings.approvedPath(r.parcelDir());
    if (r.artifactId() != null && !r.artifactId().isBlank()) UUID.fromString(r.artifactId());
  }

  public record ExecutionEvidence(
      Preview preview, Request request, Map<String, Object> inspectionParameters) {}

  @Transactional
  public ExecutionEvidence executionEvidence(UUID clusterId, UUID previewId, UUID ownerJob) {
    var preview = get(clusterId, previewId, ownerJob);
    if (!"PASS".equals(preview.status()))
      throw conflict("A complete current readiness PASS is required.");
    return loadEvidence(clusterId, previewId, preview);
  }

  @Transactional
  public ExecutionEvidence controllerEvidence(UUID clusterId, UUID previewId) {
    var preview = get(clusterId, previewId, null);
    if (!preview.controllerCommonPassed())
      throw conflict(
          "Fresh common identity, package, Java, network and cluster checks must pass before"
              + " controller planning. Broker-specific profile checks do not validate a"
              + " controller.");
    return loadEvidence(clusterId, previewId, preview);
  }

  private ExecutionEvidence loadEvidence(UUID clusterId, UUID previewId, Preview preview) {
    var request =
        jdbc.queryForObject(
            "select request from kf_external_add_node_previews where id=? and cluster_id=?",
            String.class,
            previewId,
            clusterId);
    var parameters =
        jdbc.queryForObject(
            "select parameters from kf_external_add_node_tasks where preview_id=? and"
                + " command='INSPECT' and status='SUCCESS'",
            String.class,
            previewId);
    return new ExecutionEvidence(
        preview, parse(request, Request.class), parse(parameters, Map.class));
  }

  void assertContext(UUID clusterId, Request request, String expected) {
    if (!expected.equals(context(clusterId, request)))
      throw conflict("The frozen external Add Node context changed.");
  }

  void assertExecutionScope(UUID clusterId, String agentId) {
    guardCluster(clusterId, cluster(clusterId));
    requireExecutionAgent(agentId);
  }

  void requireExecutionAgent(String id) {
    var candidate = agent(id);
    requireAgent(candidate);
    guardAgent(candidate);
    var raw =
        jdbc.queryForObject(
            "select report from kf_external_add_node_observations where agent_id=?",
            String.class,
            id);
    var report = parse(raw, ExternalAddNodeInspectionService.Observation.class);
    if (!(candidate.getVersion().endsWith("-an.3") || candidate.getVersion().endsWith("-an.4"))
        || !report.brokerExecutionCapable())
      throw conflict(
          "Install the compatible external broker-add adapter and explicitly enable broker"
              + " execution on the candidate.");
  }

  String participantAddress(UUID clusterId, String agentId, String host) {
    var a = agent(agentId);
    requireControllerAgent(agentId, false);
    guardEndpoint((host.contains(":") ? "[" + host + "]" : host) + ":1", clusterId);
    try {
      var approved = ips(a);
      var addresses =
          Arrays.stream(InetAddress.getAllByName(host))
              .map(InetAddress::getHostAddress)
              .map(ExternalAddNodeReadinessService::canonical)
              .distinct()
              .sorted(Comparator.comparing((String value) -> value.contains(":")))
              .toList();
      if (addresses.isEmpty() || !approved.containsAll(addresses))
        throw conflict("Participant DNS route is not bound to its enrolled agent.");
      return addresses.getFirst();
    } catch (ResponseStatusException e) {
      throw e;
    } catch (Exception e) {
      throw conflict("Participant address cannot be resolved inside the approved scope.");
    }
  }

  void assertControllerScope(UUID clusterId, String candidate, boolean execution) {
    guardCluster(clusterId, cluster(clusterId));
    requireControllerAgent(candidate, execution);
  }

  void requireControllerAgent(String id, boolean execution) {
    var a = agent(id);
    requireAgent(a);
    guardAgent(a);
    if (!a.getVersion().endsWith("-an.4"))
      throw conflict("A compatible external controller adapter (an.4) is required.");
    if (execution) {
      var raw =
          jdbc.queryForObject(
              "select report from kf_external_add_node_observations where agent_id=?",
              String.class,
              id);
      if (!parse(raw, ExternalAddNodeInspectionService.Observation.class)
          .controllerExecutionCapable())
        throw conflict(
            "Enable the separate external controller execution opt-in on this candidate.");
    }
  }

  private ExternalCluster cluster(UUID id) {
    var c =
        clusters
            .findById(id)
            .orElseThrow(
                () ->
                    new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "External cluster not found."));
    if ("DELETED".equals(c.getStatus())
        || !"kraft".equalsIgnoreCase(c.getKafkaMode())
        || !policy.getSupportedKafkaVersions().contains(c.getKafkaVersion())
        || c.getKafkaClusterId() == null
        || c.getKafkaClusterId().isBlank())
      throw conflict("A supported connected KRaft cluster with known cluster ID is required.");
    if (!"PLAINTEXT"
        .equalsIgnoreCase(
            Objects.toString(c.getSecurityProtocol(), Objects.toString(c.getSecurity(), ""))))
      throw conflict(
          "This initial readiness adapter requires explicit PLAINTEXT security; secure profile"
              + " inheritance is not yet supported.");
    return c;
  }

  private DiscoveryAgent agent(String id) {
    return agents.findById(id).orElseThrow(() -> conflict("External agent not found."));
  }

  private void requireAgent(DiscoveryAgent a) {
    if (!Boolean.TRUE.equals(a.getCanExecuteTasks())
        || a.getVersion() == null
        || !a.getVersion().matches("3\\.4\\.0-[A-Za-z0-9_.-]*-an\\.[1-9][0-9]*")
        || !"ONLINE".equals(a.getStatus())
        || a.getLastHeartbeat() == null
        || a.getLastHeartbeat().toInstant().isBefore(Instant.now().minusSeconds(120)))
      throw conflict("Fresh opt-in external readiness agent is required.");
    var rows =
        jdbc.queryForList(
            "select report from kf_external_add_node_observations where agent_id=? and"
                + " observed_at>now()-interval '120 seconds'",
            a.getId());
    if (rows.size() != 1) throw conflict("Fresh local host observation is required.");
    var r =
        parse(rows.getFirst().get("report"), ExternalAddNodeInspectionService.Observation.class);
    ExternalAddNodeInspectionService.validate(r);
    if (!r.root() || !r.inspectionEnabled() || !r.agentVersion().equals(a.getVersion()))
      throw conflict("Agent inspection capability changed.");
  }

  private List<String> ips(DiscoveryAgent a) {
    try {
      return mapper
          .readValue(
              Objects.toString(a.getIpAddresses(), "[]"), new TypeReference<List<String>>() {})
          .stream()
          .map(ExternalAddNodeReadinessService::canonical)
          .filter(s -> !s.isEmpty())
          .distinct()
          .sorted()
          .toList();
    } catch (Exception e) {
      return List.of();
    }
  }

  private void guardAgent(DiscoveryAgent a) {
    if (!scope.getAllowedHostIds().contains(a.getId())
        && !scope.getAllowedHostIds().contains(a.getHostname()))
      throw conflict("External agent is outside approved inspection scope.");
    if (scope.getExcludedHostIds().contains(a.getId())
        || scope.getExcludedHostIds().contains(a.getHostname())
        || java.util.stream.Stream.concat(ips(a).stream(), observedIps(a).stream())
            .anyMatch(
                ip -> scope.getExcludedHostIps().stream().anyMatch(v -> ip.equals(canonical(v)))))
      throw conflict("Excluded candidate, participant or probe host.");
  }

  private void guardCluster(UUID id, ExternalCluster c) {
    var participants = nodes.findByClusterId(id);
    if (participants.size() > 128)
      throw conflict("External readiness supports at most 128 participant records.");
    if (participants.isEmpty()) throw conflict("Refresh external participant discovery first.");
    for (var n : participants) {
      var a = agent(n.getAgentId());
      requireAgent(a);
      guardAgent(a);
    }
    for (String ep : c.getBootstrapServers().split(",")) guardEndpoint(ep.trim(), id);
  }

  private void guardEndpoint(String endpoint, UUID clusterId) {
    try {
      var uri = URI.create("tcp://" + endpoint);
      if (uri.getHost() == null || uri.getPort() < 1) throw conflict("Unresolved Kafka route.");
      String host = uri.getHost().replace("[", "").replace("]", "");
      var participants = nodes.findByClusterId(clusterId);
      var matches =
          participants.stream()
              .map(n -> agent(n.getAgentId()))
              .filter(
                  a -> host.equalsIgnoreCase(a.getHostname()) || ips(a).contains(canonical(host)))
              .toList();
      if (matches.isEmpty()) throw conflict("Kafka route is not bound to an approved participant.");
      for (var a : matches) guardAgent(a);
      for (var ip : InetAddress.getAllByName(host)) {
        String address = canonical(ip.getHostAddress());
        if (matches.stream().noneMatch(a -> ips(a).contains(address)))
          throw conflict("Kafka DNS route differs from enrolled participant addresses.");
      }
    } catch (ResponseStatusException e) {
      throw e;
    } catch (Exception e) {
      throw conflict("Resolve Kafka endpoints before checking protected-host scope.");
    }
  }

  String executionBinding(UUID id, Request request) {
    var c = cluster(id);
    var participants =
        nodes.findByClusterId(id).stream()
            .sorted(Comparator.comparing(n -> n.getId().toString()))
            .map(
                n ->
                    List.of(
                        n.getId(),
                        Objects.toString(n.getAgentId(), ""),
                        Objects.toString(n.getNodeId(), ""),
                        Objects.toString(n.getHost(), ""),
                        Objects.toString(n.getPort(), ""),
                        Objects.toString(n.getConfigFile(), ""),
                        Objects.toString(n.getSystemdService(), ""),
                        Objects.toString(n.getIsBroker(), ""),
                        Objects.toString(n.getIsController(), ""),
                        Objects.toString(n.getJmxExporterPort(), "")))
            .toList();
    var ids = new TreeSet<String>();
    ids.add(request.agentId());
    nodes.findByClusterId(id).forEach(n -> ids.add(n.getAgentId()));
    var identities =
        ids.stream()
            .map(this::agent)
            .map(
                a ->
                    List.of(
                        a.getId(),
                        Objects.toString(a.getHostname(), ""),
                        contextIps(a),
                        Objects.toString(a.getClusterId(), "")))
            .toList();
    if (!ips(agent(request.agentId())).contains(canonical(request.hostIp())))
      throw conflict("Candidate address no longer matches enrollment.");
    return hash(
        json(
            List.of(
                request,
                c.getKafkaClusterId(),
                c.getKafkaVersion(),
                c.getBootstrapServers(),
                Objects.toString(c.getConnectionSettings(), ""),
                Objects.toString(c.getSecurityProtocol(), ""),
                participants,
                identities,
                policy,
                scope)));
  }

  private String context(UUID id, Request request) {
    var c = cluster(id);
    var participants =
        nodes.findByClusterId(id).stream()
            .sorted(Comparator.comparing(n -> n.getId().toString()))
            .map(
                n ->
                    List.of(
                        n.getId(),
                        Objects.toString(n.getAgentId(), ""),
                        Objects.toString(n.getNodeId(), ""),
                        Objects.toString(n.getHost(), ""),
                        Objects.toString(n.getPort(), ""),
                        Objects.toString(n.getConfigFile(), ""),
                        Objects.toString(n.getSystemdService(), ""),
                        Objects.toString(n.getIsBroker(), ""),
                        Objects.toString(n.getIsController(), ""),
                        Objects.toString(n.getJmxExporterPort(), "")))
            .toList();
    var identities =
        agents.findAll().stream()
            .sorted(Comparator.comparing(DiscoveryAgent::getId))
            .map(
                a ->
                    List.of(
                        a.getId(),
                        Objects.toString(a.getHostname(), ""),
                        Objects.toString(a.getVersion(), ""),
                        contextIps(a),
                        Objects.toString(a.getClusterId(), ""),
                        Objects.toString(a.getCanExecuteTasks(), "")))
            .toList();
    return hash(
        json(
            List.of(
                request,
                Objects.toString(c.getKafkaClusterId(), ""),
                Objects.toString(c.getKafkaVersion(), ""),
                c.getBootstrapServers(),
                Objects.toString(c.getConnectionSettings(), ""),
                Objects.toString(c.getSecurityProtocol(), ""),
                participants,
                identities,
                policy,
                scope)));
  }

  private List<String> contextIps(DiscoveryAgent agent) {
    var registered = ips(agent);
    // Normal discovery reports store the transport IP, while opt-in heartbeat
    // reports all interfaces. A subset update is not a different machine.
    var observed = observedIps(agent);
    return !registered.isEmpty() && observed.containsAll(registered) ? observed : registered;
  }

  private List<String> observedIps(DiscoveryAgent agent) {
    var reports =
        jdbc.queryForList(
            "select report from kf_external_add_node_observations where agent_id=?", agent.getId());
    if (reports.size() == 1) {
      try {
        var report =
            parse(
                reports.getFirst().get("report"),
                ExternalAddNodeInspectionService.Observation.class);
        ExternalAddNodeInspectionService.validate(report);
        var observed =
            report.addresses().stream()
                .map(ExternalAddNodeReadinessService::canonical)
                .sorted()
                .toList();
        if (Objects.equals(report.agentVersion(), agent.getVersion())) return observed;
      } catch (RuntimeException incompatible) {
        /* Incompatible observations cannot normalize identity. */
      }
    }
    return List.of();
  }

  private void enqueue(
      UUID preview, String agent, String command, Object payload, Instant deadline) {
    jdbc.update(
        "insert into kf_external_add_node_tasks(id,preview_id,agent_id,command,parameters,deadline)"
            + " values(?,?,?,?,?,?)",
        UUID.randomUUID(),
        preview,
        agent,
        command,
        json(payload),
        Timestamp.from(deadline));
  }

  private static Check check(String code, String name, boolean pass, Object observed) {
    return new Check(
        code,
        name,
        "COMMON",
        pass ? "PASS" : "FAIL",
        "Verified live evidence",
        observed,
        pass ? "" : "Resolve the blocker and run fresh checks.",
        Instant.now(),
        "BACKEND");
  }

  private static Check unknown(String code, String name, String message) {
    return new Check(
        code,
        name,
        code.startsWith("B") ? "BROKER" : "COMMON",
        "UNKNOWN",
        "Current measured evidence",
        message,
        "Resolve missing evidence and rerun; preview applies no remediation.",
        Instant.now(),
        "BACKEND");
  }

  private List<Check> checks(String raw) {
    try {
      return new ArrayList<>(mapper.readValue(raw, new TypeReference<List<Check>>() {}));
    } catch (Exception e) {
      throw conflict("Invalid saved readiness evidence.");
    }
  }

  private <T> T parse(Object raw, Class<T> type) {
    try {
      return mapper.readValue(String.valueOf(raw), type);
    } catch (Exception e) {
      throw conflict("Invalid readiness payload.");
    }
  }

  private String json(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid readiness JSON.");
    }
  }

  private static String hash(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  static String canonical(String value) {
    if (value == null || !value.matches("[0-9a-fA-F:.]{3,64}")) return "";
    try {
      var ip = InetAddress.getByName(value);
      return ip.isAnyLocalAddress() || ip.isLoopbackAddress() || ip.isLinkLocalAddress()
          ? ""
          : ip.getHostAddress();
    } catch (Exception e) {
      return "";
    }
  }

  private static Instant instant(Object value) {
    if (value instanceof Timestamp t) return t.toInstant();
    if (value instanceof OffsetDateTime d) return d.toInstant();
    return Instant.parse(value.toString());
  }

  private static ResponseStatusException conflict(String message) {
    return new ResponseStatusException(HttpStatus.CONFLICT, message);
  }
}
