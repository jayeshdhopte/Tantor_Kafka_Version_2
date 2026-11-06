package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** Durable, read-only participant inspection. No job, host reservation or process mutation. */
@Service @RequiredArgsConstructor
public class AddNodeProcessInspectionService {
    public static final String COMMAND = "INSPECT_ADD_NODE_PROCESS";
    private static final Set<String> BLOCKERS = Set.of("SERVICE_NOT_OBSERVED", "CONFIG_IDENTITY_MISMATCH", "STORAGE_IDENTITY_MISMATCH", "PROCESS_IDENTITY_MISMATCH", "STALE_OBSERVATION", "VERSION_NOT_VERIFIED");
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final TaskRepository tasks;
    private final HostStatusService connectivity;
    private final JobService jobs;
    private final AddNodeInspectionProperties policy;
    private final ObjectMapper mapper;
    private final io.translab.tantor.server.audit.AuditService audit;

    public record Request(String schemaVersion, UUID inspectionId, String hostId, String contextHash,
                          String kafkaClusterId, String kafkaVersion, int nodeId, String role,
                          String serviceName, String configPath, Instant startedAt, Instant expiresAt) {}
    public record Metadata(String path, String clusterId, int nodeId, String directoryId) {}
    public record Evidence(String schemaVersion, UUID inspectionId, String hostId, String contextHash,
                           String kafkaClusterId, String kafkaVersion, int nodeId, String role,
                           String serviceName, String configPath, String configSha256, String unitSha256,
                           int pid, String processStartTicks, boolean serviceActive, List<Metadata> metadata,
                           boolean identityVerified, boolean versionVerified, boolean executionReady,
                           Instant observedAt, List<String> blockers) {}
    public record View(UUID taskId, UUID serviceId, String hostId, String status, Instant expiresAt,
                       Evidence evidence, boolean executionReady, String message) {}
    private record Scope(Cluster cluster, Host host, ClusterServiceAssignment service, String unit, String path, String role) {}

    @org.springframework.transaction.annotation.Transactional
    public View start(UUID clusterId, UUID serviceId) {
        var scope = scope(clusterId, serviceId);
        if (busy(clusterId)) throw conflict("Resolve the active cluster operation before participant inspection.");
        if (!supportedAgent(scope.host().getAgentVersion())) throw conflict("Install the prepared addnode.6 or newer agent before process inspection.");
        Instant now = Instant.now();
        var request = new Request("add-node-process/v1", UUID.randomUUID(), scope.host().getId(), fingerprint(scope),
                scope.cluster().getKafkaClusterId(), scope.cluster().getKafkaVersion(), scope.service().getNodeId(), scope.role(), scope.unit(), scope.path(), now, now.plusSeconds(120));
        var task = new Task(); task.setClusterId(clusterId); task.setHostId(request.hostId()); task.setCommand(COMMAND); task.setStatus("PENDING");
        task.setParameters(json(Map.of("process_inspection_request", json(request), "service_id", serviceId.toString())));
        task = tasks.saveAndFlush(task);
        audit.record("PREREQUISITE", "ADD_NODE_PROCESS_INSPECTION_REQUESTED", "HOST", request.hostId(), clusterId,
                "REQUESTED", null, null, null, Map.of("taskId", task.getId().toString(), "serviceId", serviceId.toString(), "role", scope.role()));
        return new View(task.getId(), serviceId, request.hostId(), "RUNNING", request.expiresAt(), null, false, "Read-only inspection queued; no service changes.");
    }

    public View get(UUID clusterId, UUID taskId) {
        var task = tasks.findById(taskId).filter(t -> COMMAND.equals(t.getCommand()) && clusterId.equals(t.getClusterId()))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Process inspection not found for this cluster."));
        try {
            Map<String,String> parameters = mapper.readValue(task.getParameters(), new TypeReference<>() {});
            var serviceId = UUID.fromString(parameters.get("service_id"));
            var request = mapper.readValue(parameters.get("process_inspection_request"), Request.class);
            var scope = scope(clusterId, serviceId);
            if (!task.getHostId().equals(request.hostId()) || !request.contextHash().equals(fingerprint(scope)) || busy(clusterId))
                return view(task, serviceId, request, "STALE", null, "Participant context changed. Request a fresh inspection.");
            if (!request.expiresAt().isAfter(Instant.now())) return view(task, serviceId, request, "EXPIRED", null, "Inspection expired. Request fresh evidence.");
            if (Set.of("PENDING", "IN_PROGRESS", "RUNNING").contains(task.getStatus())) return view(task, serviceId, request, "RUNNING", null, "Waiting for the enrolled agent.");
            if (!"SUCCESS".equals(task.getStatus()) || task.getLogOutput() == null || task.getLogOutput().length() > 65536)
                return view(task, serviceId, request, "BLOCKED", null, "Agent observation unavailable; existing services were not changed.");
            var evidence = mapper.readValue(task.getLogOutput(), Evidence.class);
            validate(evidence, request);
            return view(task, serviceId, request, evidence.identityVerified() ? "OBSERVED" : "BLOCKED", evidence,
                    "Runtime identity observation only. Artifact version, quorum membership and execution readiness still require independent verification.");
        } catch (ResponseStatusException e) { throw e; }
        catch (Exception e) { throw conflict("Invalid or incomplete process inspection evidence. Request fresh evidence."); }
    }

    private Scope scope(UUID clusterId, UUID serviceId) {
        var cluster = clusters.findWithServicesById(clusterId).orElseThrow(() -> conflict("Cluster not found."));
        if (!"kraft".equalsIgnoreCase(cluster.getMode()) || "EXTERNAL".equalsIgnoreCase(cluster.getOriginType()) || !"SUCCESS".equals(cluster.getStatus()))
            throw conflict("Process inspection requires an available managed KRaft cluster.");
        var service = cluster.getServices().stream().filter(s -> serviceId.equals(s.getId())).findFirst().orElseThrow(() -> conflict("Select a service assignment in this cluster."));
        var host = hosts.findById(service.getHostId()).orElseThrow(() -> conflict("Enrolled participant host not found."));
        if (!policy.allows(host.getId(), host.getHostIp())) throw conflict("This host is outside the explicitly permitted Add Node inspection scope.");
        if (Boolean.TRUE.equals(host.getRemoved()) || host.getClusterId() != null && !clusterId.equals(host.getClusterId())
                || !connectivity.isInfrastructureHost(host) || !"ONLINE".equals(connectivity.agentConnectivityStatus(host))) throw conflict("Participant requires a fresh online enrolled agent assigned to this cluster.");
        var config = configuration(service.getConfigJson());
        String unit = Objects.toString(config.get("service_name"), "");
        if (!unit.endsWith(".service")) unit += ".service";
        String path = Objects.toString(config.get("config_path"), "");
        String role = canonicalRole(service.getRole());
        if (!unit.matches("[A-Za-z0-9][A-Za-z0-9_@.-]{0,180}\\.service") || unit.contains("..") || !validConfig(path) || role.isBlank()
                || service.getNodeId() == null || service.getNodeId() < 0 || cluster.getKafkaClusterId() == null || !cluster.getKafkaClusterId().matches("[A-Za-z0-9_-]{22}")
                || cluster.getKafkaVersion() == null || !cluster.getKafkaVersion().matches("[0-9]+\\.[0-9]+\\.[0-9]+"))
            throw conflict("Recorded service, configuration, role or Kafka identity is incomplete. Runtime paths are never guessed.");
        return new Scope(cluster, host, service, unit, path, role);
    }
    private void validate(Evidence e, Request r) {
        if (!Objects.equals(e.schemaVersion(), r.schemaVersion()) || !Objects.equals(e.inspectionId(), r.inspectionId()) || !Objects.equals(e.hostId(), r.hostId())
                || !Objects.equals(e.contextHash(), r.contextHash()) || !Objects.equals(e.kafkaClusterId(), r.kafkaClusterId()) || !Objects.equals(e.kafkaVersion(), r.kafkaVersion())
                || e.nodeId() != r.nodeId() || !Objects.equals(e.role(), r.role()) || !Objects.equals(e.serviceName(), r.serviceName()) || !Objects.equals(e.configPath(), r.configPath())
                || e.versionVerified() || e.executionReady() || e.observedAt() == null || e.observedAt().isBefore(r.startedAt().minusSeconds(5))
                || e.observedAt().isAfter(Instant.now().plusSeconds(5)) || e.observedAt().isAfter(r.expiresAt()) || e.metadata() == null || e.metadata().size() > 17
                || e.blockers() == null || e.blockers().size() > BLOCKERS.size() || new HashSet<>(e.blockers()).size() != e.blockers().size()
                || !new HashSet<>(e.blockers()).contains("VERSION_NOT_VERIFIED") || e.blockers().stream().anyMatch(b -> !BLOCKERS.contains(b))
                || e.configSha256() == null || !e.configSha256().isEmpty() && !digest(e.configSha256())
                || e.unitSha256() == null || !e.unitSha256().isEmpty() && !digest(e.unitSha256())
                || e.pid() < 0 || e.processStartTicks() == null || !e.processStartTicks().isEmpty() && !e.processStartTicks().matches("[1-9][0-9]*"))
            throw new IllegalArgumentException("Unmatched observation");
        if (e.identityVerified()) {
            if (!e.serviceActive() || e.pid() < 1 || e.processStartTicks() == null || !e.processStartTicks().matches("[1-9][0-9]*")
                    || !digest(e.configSha256()) || !digest(e.unitSha256()) || e.metadata().isEmpty()
                    || !new HashSet<>(e.blockers()).equals(Set.of("VERSION_NOT_VERIFIED"))) throw new IllegalArgumentException("Incomplete verified identity");
        }
        var paths = new HashSet<String>(); var ids = new HashSet<String>();
        for (var m : e.metadata())
            if (!validStorage(m.path()) || !paths.add(m.path()) || !Objects.equals(m.clusterId(), r.kafkaClusterId()) || m.nodeId() != r.nodeId()
                    || m.directoryId() == null || !m.directoryId().matches("[A-Za-z0-9_-]{22}") || !ids.add(m.directoryId())) throw new IllegalArgumentException("Invalid storage observation");
    }
    private View view(Task task, UUID serviceId, Request r, String state, Evidence evidence, String message) { return new View(task.getId(), serviceId, task.getHostId(), state, r.expiresAt(), evidence, false, message); }
    private String fingerprint(Scope s) {
        return sha(json(List.of(s.cluster().getId(), Objects.toString(s.cluster().getKafkaClusterId(), ""), Objects.toString(s.cluster().getKafkaVersion(), ""), Objects.toString(s.cluster().getConfigJson(), ""),
                s.cluster().getServices().stream().map(v -> v.getId()+":"+v.getHostId()+":"+v.getNodeId()+":"+v.getRole()+":"+v.getConfigJson()).sorted().toList(),
                s.host().getId(), Objects.toString(s.host().getHostIp(), ""), Objects.toString(s.host().getAgentVersion(), ""), s.service().getId(), s.unit(), s.path(), mapper.valueToTree(policy))));
    }
    private boolean busy(UUID clusterId) { return jobs.getActiveClusterJobs().stream().anyMatch(j -> clusterId.toString().equals(j.get("clusterId"))); }
    static boolean supportedAgent(String value) { if (value == null) return false; var m = java.util.regex.Pattern.compile(".*-addnode\\.([0-9]+)$").matcher(value); try { return m.matches() && Integer.parseInt(m.group(1)) >= 6; } catch (Exception e) { return false; } }
    private static boolean digest(String value) { return value != null && value.matches("[a-f0-9]{64}"); }
    private static String canonicalRole(String value) { if (value == null) return ""; return switch (value.replace(" ", "")) { case "broker", "controller" -> value.replace(" ", ""); case "broker_controller", "broker,controller", "controller,broker" -> "broker,controller"; default -> ""; }; }
    private static boolean canonicalPath(String value) { return value != null && value.matches("/[A-Za-z0-9_./-]+") && !value.endsWith("/") && Path.of(value).normalize().toString().replace('\\','/').equals(value); }
    private static boolean validStorage(String value) { return canonicalPath(value) && List.of("/opt/", "/var/", "/data/", "/srv/", "/app/").stream().anyMatch(value::startsWith); }
    private static boolean validConfig(String value) { return canonicalPath(value) && (validStorage(value) || List.of("/etc/kafka/", "/etc/tantor-kafka/", "/etc/confluent/").stream().anyMatch(value::startsWith)) && Set.of("server.properties", "broker.properties", "controller.properties").contains(Path.of(value).getFileName().toString()); }
    private Map<String,Object> configuration(String value) { try { Map<String,Object> c = mapper.readValue(value == null ? "{}" : value, new TypeReference<>() {}); if (c == null) throw new IllegalArgumentException(); return c; } catch (Exception e) { throw conflict("Recorded process configuration is unreadable."); } }
    private String json(Object value) { try { return mapper.writeValueAsString(value); } catch (Exception e) { throw new IllegalStateException("Cannot encode process inspection contract", e); } }
    private static String sha(String value) { try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))); } catch (Exception e) { throw new IllegalStateException(e); } }
    private static ResponseStatusException conflict(String message) { return new ResponseStatusException(HttpStatus.CONFLICT, message); }
}
