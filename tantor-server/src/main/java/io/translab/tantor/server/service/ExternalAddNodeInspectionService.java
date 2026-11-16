package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import io.translab.tantor.server.domain.DiscoveryAgent;
import io.translab.tantor.server.repository.*;
import java.time.*;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/** External Phase 2 host inventory foundation, never execution admission. */
@Service
@RequiredArgsConstructor
public class ExternalAddNodeInspectionService {
  @org.springframework.beans.factory.annotation.Autowired
  private ExternalBrokerAdditionGuard externalBrokerGuard;

  private final DiscoveryAgentRepository agents;
  private final ExternalClusterRepository clusters;
  private final ExternalClusterNodeRepository nodes;
  private final HostRepository internalHosts;
  private final AddNodeInspectionProperties scope;
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  public static final String SCHEMA = "external-add-node-host/v1";

  public record Observation(
      String schemaVersion,
      String agentId,
      String agentVersion,
      Instant observedAt,
      List<String> addresses,
      boolean root,
      boolean inspectionEnabled,
      boolean inventoryComplete,
      int kafkaProcessCount,
      int kafkaUnitCount,
      int discoveredProcessCount,
      boolean executionEnabled,
      boolean brokerExecutionCapable) {
    public Observation(
        String schemaVersion,
        String agentId,
        String agentVersion,
        Instant observedAt,
        List<String> addresses,
        boolean root,
        boolean inspectionEnabled,
        boolean inventoryComplete,
        int kafkaProcessCount,
        int kafkaUnitCount,
        int discoveredProcessCount,
        boolean executionEnabled) {
      this(
          schemaVersion,
          agentId,
          agentVersion,
          observedAt,
          addresses,
          root,
          inspectionEnabled,
          inventoryComplete,
          kafkaProcessCount,
          kafkaUnitCount,
          discoveredProcessCount,
          executionEnabled,
          false);
    }

    public Observation {
      addresses = addresses == null ? List.of() : List.copyOf(addresses);
    }
  }

  public record Candidate(
      String agentId,
      String hostname,
      Instant observedAt,
      String status,
      boolean localInventoryClear,
      boolean executionEnabled,
      List<String> blockers) {}

  public record Review(
      String schemaVersion,
      UUID clusterId,
      String phase,
      boolean executionEnabled,
      List<Candidate> candidates,
      List<String> remainingChecks) {}

  @Transactional
  public void record(Observation report) {
    validate(report);
    var agent =
        agents
            .findById(report.agentId())
            .orElseThrow(
                () ->
                    error(
                        HttpStatus.CONFLICT,
                        "Register a fresh external agent heartbeat before reporting host"
                            + " inspection."));
    if (!Objects.equals(agent.getVersion(), report.agentVersion())
        || !Boolean.TRUE.equals(agent.getCanExecuteTasks())
        || !online(agent))
      throw error(
          HttpStatus.CONFLICT,
          "Inspection does not match a current task-enabled external agent heartbeat.");
    String raw = json(report);
    if (raw.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 16384)
      throw error(
          HttpStatus.BAD_REQUEST,
          "Inspection report exceeds the bounded host observation contract.");
    jdbc.update(
        "INSERT INTO kf_external_add_node_observations(agent_id,observed_at,report) VALUES(?,?,?)"
            + " ON CONFLICT(agent_id) DO UPDATE SET"
            + " observed_at=excluded.observed_at,received_at=now(),report=excluded.report WHERE"
            + " kf_external_add_node_observations.observed_at < excluded.observed_at",
        report.agentId(),
        java.sql.Timestamp.from(report.observedAt()),
        raw);
  }

  @Transactional(readOnly = true)
  public Review candidates(UUID clusterId) {
    return candidates(clusterId, null);
  }

  @Transactional(readOnly = true)
  public Review candidates(UUID clusterId, UUID ownerJob) {
    var cluster =
        clusters
            .findById(clusterId)
            .orElseThrow(() -> error(HttpStatus.NOT_FOUND, "External cluster not found."));
    if ("DELETED".equalsIgnoreCase(cluster.getStatus()))
      throw error(HttpStatus.CONFLICT, "The external cluster was deleted.");
    boolean supported =
        "kraft".equalsIgnoreCase(cluster.getKafkaMode())
            && Set.of("4.1.0", "4.1.1", "4.1.2", "4.3.0")
                .contains(Objects.toString(cluster.getKafkaVersion(), ""));
    var rows =
        jdbc.query(
            "SELECT agent_id,report FROM kf_external_add_node_observations",
            (rs, n) -> Map.entry(rs.getString(1), rs.getString(2)));
    var byAgent = new HashMap<String, Observation>();
    for (var row : rows)
      try {
        var report = mapper.readValue(row.getValue(), Observation.class);
        validate(report);
        if (row.getKey().equals(report.agentId())) byAgent.put(row.getKey(), report);
      } catch (Exception ignored) {
        /* Incompatible/stale evidence cannot establish a clear inventory. */
      }
    var internal = internalHosts.findAll();
    var discovered = nodes.findAll();
    var result = new ArrayList<Candidate>();
    var enrolled = agents.findAll();
    for (var agent : enrolled) {
      var report = byAgent.get(agent.getId());
      var blockers = new ArrayList<String>();
      if (externalBrokerGuard != null
          && externalBrokerGuard.reservedAgentExcept(agent.getId(), ownerJob))
        blockers.add(
            "Another external broker operation retains this candidate. Inspect or resume that"
                + " operation before starting another preview.");
      if (!supported)
        blockers.add(
            "Initial external Add Node support requires a supported Kafka 4.x KRaft cluster.");
      if (!online(agent)) blockers.add("External agent heartbeat is stale or offline.");
      if (agent.getClusterId() != null
          || discovered.stream()
              .anyMatch(
                  n ->
                      agent.getId().equals(n.getAgentId())
                          || agent.getHostname() != null
                              && agent.getHostname().equalsIgnoreCase(n.getHost())
                          || report != null
                              && report.addresses().stream()
                                  .anyMatch(ip -> sameAddress(ip, n.getHost()))))
        blockers.add("Agent already owns an external cluster or discovered Kafka node.");
      boolean clear =
          report != null
              && report.inventoryComplete()
              && report.kafkaProcessCount() == 0
              && report.kafkaUnitCount() == 0
              && report.discoveredProcessCount() == 0;
      if (report == null || !Objects.equals(report.agentVersion(), agent.getVersion()))
        blockers.add("Fresh opt-in Add Node host inspection is required.");
      else {
        if (!clear)
          blockers.add("Kafka processes/units were observed or the host inventory is incomplete.");
        boolean excluded =
            scope.getExcludedHostIds().contains(agent.getId())
                || scope.getExcludedHostIds().contains(agent.getHostname())
                || report.addresses().stream()
                    .anyMatch(
                        ip ->
                            scope.getExcludedHostIps().stream()
                                .anyMatch(excludedIp -> sameAddress(ip, excludedIp)));
        boolean allowed =
            scope.getAllowedHostIds().contains(agent.getId())
                || scope.getAllowedHostIds().contains(agent.getHostname());
        if (excluded || !allowed)
          blockers.add("External candidate is outside the approved Add Node inspection scope.");
        if (!addressMatch(agent, report))
          blockers.add("Reported addresses do not match the registered agent identity.");
        if (enrolled.stream()
            .anyMatch(
                other ->
                    !other.getId().equals(agent.getId())
                        && (agent.getHostname() != null
                                && agent.getHostname().equalsIgnoreCase(other.getHostname())
                            || addressMatch(other, report))))
          blockers.add(
              "Another enrolled external identity reports this host or address; resolve duplicate"
                  + " enrollment first.");
        if (internal.stream()
            .anyMatch(
                h ->
                    !Boolean.TRUE.equals(h.getRemoved())
                        && (report.addresses().stream()
                                .anyMatch(ip -> sameAddress(ip, h.getHostIp()))
                            || agent.getId().equals(h.getId())
                            || Objects.equals(agent.getHostname(), h.getId()))))
          blockers.add(
              "Host already has an internal deployment-agent identity; resolve ownership before"
                  + " external addition.");
        if (!report.root()
            || !report.inspectionEnabled()
            || !Boolean.TRUE.equals(agent.getCanExecuteTasks()))
          blockers.add("Explicit root-run, task-enabled external inspection is required.");
      }
      result.add(
          new Candidate(
              agent.getId(),
              agent.getHostname(),
              report == null ? null : report.observedAt(),
              blockers.isEmpty() ? "HOST_OBSERVED" : "BLOCKED",
              clear,
              false,
              List.copyOf(blockers)));
    }
    return new Review(
        "external-add-node-candidates/v1",
        clusterId,
        "HOST_INSPECTION_ONLY",
        false,
        List.copyOf(result),
        List.of(
            "Candidate storage, Java, package checksum, ports and resource inspection",
            "Live cluster identity, version, rack, sequential IDs and quorum verification",
            "Durable external task claims, host/ID reservations and recovery",
            "Broker-first execution/publication acceptance; controller and separate-process"
                + " adapters afterwards"));
  }

  private boolean addressMatch(DiscoveryAgent agent, Observation report) {
    try {
      var addresses = mapper.readTree(Objects.toString(agent.getIpAddresses(), "[]"));
      if (!addresses.isArray()) return false;
      for (var ip : addresses)
        if (report.addresses().stream().anyMatch(v -> sameAddress(v, ip.asText()))) return true;
    } catch (Exception ignored) {
    }
    return false;
  }

  private static boolean online(DiscoveryAgent a) {
    return "ONLINE".equalsIgnoreCase(a.getStatus())
        && a.getLastHeartbeat() != null
        && a.getLastHeartbeat().toInstant().isAfter(Instant.now().minusSeconds(120));
  }

  static void validate(Observation r) {
    if (r == null
        || !SCHEMA.equals(r.schemaVersion())
        || r.agentId() == null
        || !r.agentId().matches("[A-Za-z0-9_.:-]{1,255}")
        || r.agentVersion() == null
        || r.agentVersion().length() > 128
        || !r.agentVersion().matches("3\\.4\\.0-[A-Za-z0-9_.-]*-an\\.[1-9][0-9]*")
        || r.observedAt() == null
        || r.observedAt().isBefore(Instant.now().minusSeconds(120))
        || r.observedAt().isAfter(Instant.now().plusSeconds(5))
        || r.addresses().isEmpty()
        || r.addresses().size() > 64
        || r.addresses().stream()
                .map(ExternalAddNodeInspectionService::canonicalAddress)
                .distinct()
                .count()
            != r.addresses().size()
        || r.addresses().stream().anyMatch(v -> v == null || !validAddress(v))
        || r.kafkaProcessCount() < 0
        || r.kafkaProcessCount() > 65536
        || r.kafkaUnitCount() < 0
        || r.kafkaUnitCount() > 60000
        || r.discoveredProcessCount() < 0
        || r.discoveredProcessCount() > 65536
        || r.executionEnabled())
      throw error(
          HttpStatus.BAD_REQUEST,
          "Invalid, expired or execution-enabled external host inspection.");
  }

  private static boolean validAddress(String value) {
    if (!value.matches("[0-9a-fA-F:.]{3,64}")) return false;
    try {
      var ip = java.net.InetAddress.getByName(value);
      return !ip.isLoopbackAddress()
          && !ip.isAnyLocalAddress()
          && !ip.isLinkLocalAddress()
          && (value.contains(":") || value.equals(ip.getHostAddress()));
    } catch (Exception e) {
      return false;
    }
  }

  private static String canonicalAddress(String value) {
    if (value == null || !validAddress(value)) return "";
    try {
      return java.net.InetAddress.getByName(value).getHostAddress();
    } catch (Exception e) {
      return "";
    }
  }

  private static boolean sameAddress(String a, String b) {
    String canonical = canonicalAddress(a);
    return !canonical.isEmpty() && canonical.equals(canonicalAddress(b));
  }

  private String json(Object o) {
    try {
      return mapper.writeValueAsString(o);
    } catch (Exception e) {
      throw error(HttpStatus.BAD_REQUEST, "Invalid inspection JSON.");
    }
  }

  private static ResponseStatusException error(HttpStatus status, String message) {
    return new ResponseStatusException(status, message);
  }
}
