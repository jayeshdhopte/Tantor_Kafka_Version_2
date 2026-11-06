package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** Durable preview stored in the existing leased task/evidence infrastructure. No deployment mutations. */
@Service @RequiredArgsConstructor
public class AddNodeReadinessService {
    public static final String COMMAND = "INSPECT_ADD_NODE";
    private static final Set<String> AGENT_CODES = Set.of("C09", "C10", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B10", "B11");
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final ClusterServiceAssignmentRepository assignments;
    private final HostParcelRepository parcels;
    private final TaskRepository tasks;
    private final HostStatusService hostStatus;
    private final JobService jobs;
    private final AddNodeKafkaReadiness kafka;
    private final AddNodeArtifactCatalog catalog;
    private final AddNodeReadinessProperties policy;
    private final KafkaDeploymentProperties deployment;
    private final ObjectMapper mapper;
    private final AuditService audit;

    public Preview start(UUID clusterId, String hostId) {
        return startForOperation(clusterId,hostId,null);
    }
    public Preview start(UUID clusterId,String hostId,String brokerRack){return startForOperation(clusterId,hostId,null,brokerRack);}
    public Preview startForOperation(UUID clusterId, String hostId, UUID ownerJob) {
        return startForOperation(clusterId,hostId,ownerJob,"");
    }
    public Preview startForOperation(UUID clusterId,String hostId,UUID ownerJob,String requestedRack) {
        String brokerRack=Objects.toString(requestedRack,"").strip();
        if(!brokerRack.matches("[A-Za-z0-9_.-]{0,128}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Broker rack must use up to 128 letters, numbers, dots, underscores or hyphens.");
        Cluster cluster = requireCluster(clusterId);
        Host host = requireHost(hostId);
        if (!hostStatus.isInfrastructureHost(host) || !hostStatus.isOnline(host))
            throw new ResponseStatusException(HttpStatus.CONFLICT, "An online enrolled deployment agent is required.");
        Instant started = Instant.now();
        Map<String, Object> config = config(cluster);
        var checks = new ArrayList<Check>();
        checks.add(backendCheck("C04", "Cluster operation availability", mutable(clusterId,ownerJob), "No conflicting cluster job", "Cluster operation guard"));
        boolean unassigned = unassigned(host);
        checks.add(backendCheck("C11", "Host deployment reservation", unassigned, "Unassigned fresh host; no pending mutations", host.getId()));
        checks.add(backendCheck("C21", "Cluster and host lock availability", mutable(clusterId,ownerJob) && unassigned, "No conflicting operation", "Observation only; no locks acquired"));
        var observation = kafka.observe(cluster, config);
        checks.addAll(observation.checks());
        boolean rackAware=observation.brokerRacks().stream().anyMatch(r->!r.isBlank());
        boolean mixedRack=rackAware&&observation.brokerRacks().contains("");
        boolean rackValid=observation.rackObserved()&&!mixedRack&&(!rackAware||!brokerRack.isBlank())&&(rackAware||brokerRack.isBlank());
        checks.add(new Check("B_RACK","Candidate rack configuration","BROKER",!observation.rackObserved()?"UNKNOWN":rackValid?"PASS":"FAIL",
                rackAware?"Explicit actual rack/zone for this candidate":"No rack unless the existing cluster is rack-aware",Map.of("existingRacks",observation.brokerRacks(),"candidateRack",brokerRack),
                rackValid?"":"Enter the actual candidate rack when the cluster uses racks; resolve mixed or unavailable existing rack observations, then rerun checks.",started,"KAFKA_API"));
        var allocation=AddNodeIdentityAllocator.broker(cluster,observation.nodeIds());
        checks.add(backendCheck("B01", "Broker role and proposed identity", allocation.available() && !observation.nodeIds().isEmpty(),
                "broker; highest existing broker ID + 1; unused across brokers/controllers/observers", allocation.reason()+" Not reserved."));

        var request = new LinkedHashMap<String, Object>();
        request.put("schemaVersion", "add-node/v1");
        request.put("hostId", hostId);
        request.put("brokerRack",brokerRack);
        request.put("hostIp", Objects.toString(host.getHostIp(), ""));
        request.put("hostIdentityUnique", uniqueHost(host));
        request.put("kafkaVersion", Objects.toString(cluster.getKafkaVersion(), ""));
        request.put("kafkaClusterId", Objects.toString(cluster.getKafkaClusterId(), ""));
        request.put("brokerPort", text(config, "broker_port", "listener_port"));
        request.put("runtimeUser", first(policy.getCandidateRuntimeUser(),text(config, "runtime_user", "runtimeUser"), deployment.getRuntimeUser()));
        request.put("javaHome", first(policy.getCandidateJavaHome(),text(config, "java_home", "javaHome"), deployment.getJavaHome()));
        request.put("dataPaths", policy.getCandidateDataPaths().isEmpty()?split(first(text(config, "data_paths", "kafka_log_dirs", "log_dirs", "kafka_data_dir"), cluster.getDataDirectory())):List.copyOf(policy.getCandidateDataPaths()));
        request.put("metadataPath", text(config, "metadata_path", "kafka_metadata_dir", "metadata_log_dir"));
        request.put("logPath", first(policy.getCandidateLogPath(),text(config, "log_path", "kafka_app_log_dir", "service_log_dir"), cluster.getLogDirectory()));
        request.put("installPath", first(text(config, "install_base_path", "kafka_install_base_dir", "kafka_install_dir"), cluster.getInstallDirectory()));
        request.put("controllerEndpoints", observation.controllerEndpoints());
        request.put("brokerEndpoints", observation.brokerEndpoints());
        request.put("securityMode", first(text(config, "security_mode", "listener_security_protocol"), deployment.getSecurityMode()));
        request.put("quorumMode", observation.quorumMode());
        request.put("heapXmx", brokerHeap(cluster,config));
        request.put("ports", plannedPorts(cluster, config));
        request.put("limitNofile", deployment.getLimitNofile());
        request.put("minAvailableMemoryMb", policy.getMinAvailableMemoryMb());
        request.put("minDiskGb", policy.getMinDiskGb());
        request.put("minInodes", policy.getMinInodes());
        request.put("allowedJavaMajors", policy.getAllowedJavaMajors());
        String artifactId = text(config, "artifact_id");
        String artifactHash = "";
        try {
            var artifact = catalog.resolve(cluster.getKafkaVersion(), artifactId);
            artifactId = artifact.id(); artifactHash = artifact.checksum();
            request.put("artifactId", artifactId);
            request.put("artifactChecksum", artifactHash);
            var parcel = parcels.findFirstByHostIdAndArtifactIdOrderByCreatedAtDescIdDesc(hostId, UUID.fromString(artifactId));
            request.put("parcelDir", parcel.map(HostParcel::getParcelDir).orElse(""));
        } catch (Exception e) {
            request.put("artifactId", ""); request.put("artifactChecksum", ""); request.put("parcelDir", "");
            checks.add(new Check("ARTIFACT_CATALOG", "Approved artifact catalog", "COMMON", "UNKNOWN", "One approved matching artifact and SHA-256", "Unavailable or ambiguous",
                    "Make a matching Kafka artifact AVAILABLE with SHA-256; pin artifact_id if several match.", started, "ARTIFACT_API"));
        }
        String fingerprint = rackFingerprint(fingerprint(cluster, host, artifactId, artifactHash),brokerRack);
        request.put("contextHash", fingerprint);
        request.put("expiresAt", started.plusSeconds(policy.getTtlSeconds()).toString());
        request.put("contextStartedAt", started.toString());
        // Peers perform a nonce-bound handshake against a temporary candidate listener.
        // Client-origin coverage is explicit policy, never inferred from a peer-side dial.
        var probes = new ArrayList<Map<String, String>>();
        var peerIds = new LinkedHashSet<String>();
        cluster.getServices().forEach(s -> peerIds.add(s.getHostId()));
        peerIds.addAll(policy.getClientProbeHostIds());
        if (peerIds.size() > 128) throw new ResponseStatusException(HttpStatus.CONFLICT, "Readiness peer scope exceeds 128 hosts; configure an approved bounded inspection policy.");
        for (String peerId : peerIds) {
            Host peer = hosts.findById(peerId).orElse(null);
            if (peer == null || peerId.equals(hostId) || !hostStatus.isInfrastructureHost(peer) || !"ONLINE".equals(hostStatus.agentConnectivityStatus(peer))) continue;
            var roles = new ArrayList<String>();
            cluster.getServices().stream().filter(s -> peerId.equals(s.getHostId())).forEach(s -> {
                if (s.getRole().contains("broker")) roles.add("BROKER");
                if (s.getRole().contains("controller")) roles.add("CONTROLLER");
            });
            if (policy.getClientProbeHostIds().contains(peerId)) roles.add("CLIENT");
            probes.add(Map.of("hostId", peerId, "sourceIp", Objects.toString(peer.getHostIp(), ""), "roles", String.join(",", roles), "token", UUID.randomUUID().toString()));
        }
        request.put("peerProbes", probes);
        request.put("expectedBrokerProbeCount", cluster.getServices().stream().filter(s -> s.getRole().contains("broker")).map(ClusterServiceAssignment::getHostId).distinct().count());
        request.put("expectedControllerProbeCount", cluster.getServices().stream().filter(s -> s.getRole().contains("controller")).map(ClusterServiceAssignment::getHostId).distinct().count());
        request.put("expectedClientProbeCount", policy.getClientProbeHostIds().size());
        var parameters = new LinkedHashMap<String, String>();
        parameters.put("readiness_request", json(request));
        parameters.put("readiness_backend", json(checks));
        parameters.put("readiness_context", fingerprint);
        parameters.put("readiness_started_at", started.toString());
        parameters.put("readiness_artifact_id", artifactId);
        parameters.put("readiness_artifact_checksum", artifactHash);
        Task task = new Task(); task.setClusterId(clusterId); task.setHostId(hostId); task.setCommand(COMMAND);
        task.setStatus("PENDING"); task.setParameters(json(parameters));
        task = tasks.saveAndFlush(task);
        for (Map<String, String> probe : probes) {
            Task peerTask = new Task(); peerTask.setHostId(probe.get("hostId")); peerTask.setClusterId(clusterId);
            peerTask.setCommand("PROBE_ADD_NODE"); peerTask.setStatus("PENDING");
            peerTask.setParameters(json(Map.of("readiness_request", json(Map.of("hostId", probe.get("hostId"), "targetIp", Objects.toString(host.getHostIp(), ""),
                    "targetPort", text(config, "broker_port", "listener_port"), "token", probe.get("token"), "expiresAt", request.get("expiresAt"))))));
            tasks.save(peerTask);
        }
        audit.record("PREREQUISITE", "ADD_NODE_READINESS_REQUESTED", "HOST", hostId, clusterId, "REQUESTED", null, null, null,
                Map.of("taskId", task.getId().toString(), "contextHash", fingerprint, "role", "broker"));
        return response(task, cluster, host, false, ownerJob);
    }

    public Preview get(UUID clusterId, UUID previewId) {
        return getForOperation(clusterId,previewId,null);
    }
    public Preview getForOperation(UUID clusterId, UUID previewId, UUID ownerJob) {
        Task task = tasks.findById(previewId).filter(t -> COMMAND.equals(t.getCommand()) && clusterId.equals(t.getClusterId()))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Readiness preview not found for this cluster."));
        return response(task, requireCluster(clusterId), requireHost(task.getHostId()), true, ownerJob);
    }

    private Preview response(Task task, Cluster cluster, Host host, boolean validateCurrent, UUID ownerJob) {
        try {
            Map<String, String> parameters = mapper.readValue(task.getParameters(), new TypeReference<>() {});
            Instant started = Instant.parse(parameters.get("readiness_started_at"));
            Instant expires = started.plusSeconds(policy.getTtlSeconds());
            var checks = new ArrayList<>(mapper.readValue(parameters.get("readiness_backend"), new TypeReference<List<Check>>() {}));
            boolean pending = Set.of("PENDING", "IN_PROGRESS", "RUNNING").contains(task.getStatus());
            if (!pending) {
                try {
                    JsonNode result = mapper.readTree(task.getLogOutput());
                    if (!"SUCCESS".equals(task.getStatus()) || !"add-node/v1".equals(result.path("schemaVersion").asText())
                            || !host.getId().equals(result.path("hostId").asText()) || !parameters.get("readiness_context").equals(result.path("contextHash").asText()))
                        throw new IllegalStateException("Unmatched inspection contract");
                    var seen = new HashSet<String>();
                    for (JsonNode row : result.path("checks")) {
                        Check check = mapper.treeToValue(row, Check.class);
                        if (!AGENT_CODES.contains(check.code()) || !seen.add(check.code()) || !Set.of("PASS", "FAIL", "UNKNOWN", "BLOCKED").contains(check.status())
                                || !(check.code().startsWith("B") ? "BROKER" : "COMMON").equals(check.phase())
                                || check.expected() == null || check.observed() == null || !"AGENT".equals(check.source())
                                || check.observedAt() == null || check.observedAt().isBefore(started.minusSeconds(5)) || check.observedAt().isAfter(Instant.now().plusSeconds(5)))
                            throw new IllegalStateException("Invalid inspection evidence");
                        checks.add(check);
                    }
                    if (!seen.equals(AGENT_CODES)) throw new IllegalStateException("Incomplete inspection evidence");
                } catch (Exception e) {
                    checks.removeIf(c -> AGENT_CODES.contains(c.code()));
                    checks.add(new Check("AGENT_CONTRACT", "Agent inspection evidence", "COMMON", "UNKNOWN", "Complete add-node/v1 evidence", "Missing, incompatible, failed or malformed inspection",
                            "Install the compatible agent with the collective release, check agent logs, then rerun readiness.", Instant.now(), "AGENT"));
                }
            }
            boolean stale = false;
            if (validateCurrent) {
                var originalRequest=mapper.readTree(parameters.get("readiness_request"));
                stale = !parameters.get("readiness_context").equals(rackFingerprint(fingerprint(cluster, host, parameters.get("readiness_artifact_id"), parameters.get("readiness_artifact_checksum")),originalRequest.path("brokerRack").asText("")))
                        || !unassigned(host) || !mutable(cluster.getId(),ownerJob) || !hostStatus.isOnline(host);
                if (!pending && !parameters.get("readiness_artifact_id").isBlank()) {
                    try {
                        var approved = catalog.resolve(cluster.getKafkaVersion(), parameters.get("readiness_artifact_id"));
                        stale |= !approved.checksum().equals(parameters.get("readiness_artifact_checksum"));
                    } catch (Exception e) { stale = true; }
                }
                if (stale) checks.add(new Check("CONTEXT", "Preview input freshness", "COMMON", "BLOCKED", "Unchanged cluster/host/policy and no conflicting operation", "Context changed",
                        "Run fresh checks for the current host and cluster settings.", Instant.now(), "BACKEND"));
            }
            var required = new HashSet<String>();
            for (int i = 1; i <= 21; i++) required.add("C%02d".formatted(i));
            for (int i = 1; i <= 11; i++) required.add("B%02d".formatted(i));
            var measured = checks.stream().map(Check::code).collect(java.util.stream.Collectors.toSet());
            String state = Instant.now().isAfter(expires) ? "EXPIRED" : stale ? "STALE" : pending ? "RUNNING"
                    : measured.containsAll(required) && checks.stream().allMatch(c -> "PASS".equals(c.status())) ? "PASS" : "BLOCKED";
            var plan = mapper.readValue(parameters.get("readiness_request"), new TypeReference<Map<String, Object>>() {});
            plan.remove("peerProbes");
            return new Preview(task.getId(), cluster.getId(), host.getId(), parameters.get("readiness_context"), started, expires, state,
                    checks.stream().sorted(Comparator.comparing(Check::phase).thenComparing(Check::code)).toList(), plan, false);
        } catch (java.io.IOException | IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Stored readiness evidence cannot be read. Run fresh checks.");
        }
    }

    private Cluster requireCluster(UUID id) {
        Cluster cluster = clusters.findWithServicesById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Cluster not found."));
        if (!"kraft".equalsIgnoreCase(cluster.getMode()) || "EXTERNAL".equalsIgnoreCase(cluster.getOriginType()) || !"SUCCESS".equals(cluster.getStatus()))
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Readiness requires an available managed KRaft cluster.");
        return cluster;
    }
    private Host requireHost(String id) {
        if (id == null || id.isBlank()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Select a registered host.");
        return hosts.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Host not found."));
    }
    private boolean mutable(UUID id) { try { jobs.assertClusterMutable(id); return true; } catch (ClusterOperationConflictException e) { return false; } }
    private boolean mutable(UUID id,UUID owner) { if(owner==null)return mutable(id);try{jobs.assertBrokerAdditionOwnsCluster(id,owner);return true;}catch(ClusterOperationConflictException e){return false;} }
    private boolean unassigned(Host host) {
        boolean assigned = host.getClusterId() != null || assignments.findReservationsForLiveClusters().stream().anyMatch(s -> host.getId().equals(s.getHostId()));
        boolean busy = java.util.stream.Stream.concat(tasks.findByHostIdAndStatusOrderByCreatedAtAsc(host.getId(), "PENDING").stream(),
                tasks.findByHostIdAndStatusOrderByCreatedAtAsc(host.getId(), "IN_PROGRESS").stream())
                .anyMatch(t -> !Set.of(COMMAND, "PROBE_ADD_NODE", "CHECK_PORTS", "CHECK_PREREQUISITES", "READ_KAFKA_PROPERTIES", "READ_KAFKA_PROPERTY").contains(t.getCommand()));
        return !assigned && !busy;
    }
    private String fingerprint(Cluster cluster, Host host, String artifactId, String checksum) {
        var values = new TreeMap<String, Object>();
        values.put("cluster", List.of(cluster.getId().toString(), Objects.toString(cluster.getKafkaClusterId(), ""), cluster.getKafkaVersion(), cluster.getMode(), cluster.getStatus(), Objects.toString(cluster.getConfigJson(), "")));
        values.put("paths", List.of(Objects.toString(cluster.getDataDirectory(), ""), Objects.toString(cluster.getInstallDirectory(), ""), Objects.toString(cluster.getLogDirectory(), "")));
        values.put("host", List.of(host.getId(), Objects.toString(host.getHostIp(), ""), Objects.toString(host.getAgentVersion(), ""), Objects.toString(host.getClusterId(), "")));
        values.put("hostIdentityUnique", uniqueHost(host));
        values.put("nodeIds", cluster.getNodeIds());
        values.put("members", cluster.getServices().stream().map(s -> s.getHostId() + ":" + s.getNodeId() + ":" + s.getRole() + ":" + s.getConfigJson()).sorted().toList());
        values.put("artifact", Objects.toString(artifactId, "") + ":" + Objects.toString(checksum, ""));
        values.put("policy", mapper.valueToTree(policy));
        values.put("deploymentPolicy", mapper.valueToTree(deployment));
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json(values).getBytes(StandardCharsets.UTF_8))); }
        catch (Exception e) { throw new IllegalStateException("Cannot fingerprint readiness context.", e); }
    }
    private String rackFingerprint(String base,String rack) {
        if(rack.isEmpty())return base;
        try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest((base+":rack:"+rack).getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}
    }
    private Map<String, Object> config(Cluster cluster) {
        try { return mapper.readValue(Objects.toString(cluster.getConfigJson(), "{}"), new TypeReference<>() {}); }
        catch (Exception e) { throw new ResponseStatusException(HttpStatus.CONFLICT, "Cluster configuration is unreadable."); }
    }
    private String brokerHeap(Cluster cluster,Map<String,Object> config) {
        String configured=text(config,"heap_xmx","heap_size");
        if(!configured.isBlank())return configured;
        var heaps=new TreeSet<String>();
        for(var service:cluster.getServices()) {
            if(service.getRole()==null||!service.getRole().contains("broker"))continue;
            try {
                Map<String,Object> profile=mapper.readValue(Objects.toString(service.getConfigJson(),"{}"),new TypeReference<>(){});
                String heap=text(profile,"heap_xmx","heap_size");if(!heap.isBlank())heaps.add(heap);
            } catch(Exception e) { throw new ResponseStatusException(HttpStatus.CONFLICT,"Existing broker runtime profile is unreadable."); }
        }
        if(heaps.size()>1)throw new ResponseStatusException(HttpStatus.CONFLICT,"Existing brokers use different heaps; configure an explicit cluster broker heap before adding a broker.");
        return heaps.isEmpty()?"":heaps.first();
    }
    private boolean uniqueHost(Host host) {
        return host.getHostIp() != null && !host.getHostIp().isBlank() && hosts.findAll().stream()
                .noneMatch(other -> !host.getId().equals(other.getId()) && host.getHostIp().equals(other.getHostIp()) && !Boolean.TRUE.equals(other.getRemoved()));
    }
    private List<String> plannedPorts(Cluster cluster, Map<String, Object> config) {
        var ports = new ArrayList<String>(); ports.add(text(config, "broker_port", "listener_port"));
        if (!Boolean.FALSE.equals(cluster.getJmxEnabled())) ports.add(text(config, "jmx_port", "broker_jmx_port").isBlank() ? Objects.toString(cluster.getJmxExporterPort(), "") : text(config, "jmx_port", "broker_jmx_port"));
        if (Boolean.TRUE.equals(cluster.getNodeExporterEnabled())) ports.add(Objects.toString(cluster.getNodeExporterPort(), ""));
        return ports;
    }
    private Check backendCheck(String code, String name, boolean pass, Object expected, Object actual) {
        return new Check(code, name, code.startsWith("B") ? "BROKER" : "COMMON", pass ? "PASS" : "FAIL", expected, actual,
                pass ? "" : "Resolve the conflict and run fresh checks. No reservation is made by preview.", Instant.now(), "BACKEND");
    }
    private String json(Object value) { try { return mapper.writeValueAsString(value); } catch (Exception e) { throw new IllegalStateException("Cannot encode readiness contract.", e); } }
    private static List<String> split(String value) { return value.isBlank() ? List.of() : Arrays.stream(value.split(",")).map(String::trim).toList(); }
    private static String text(Map<String, Object> config, String... keys) { for (String key : keys) { String value = Objects.toString(config.get(key), ""); if (!value.isBlank()) return value; } return ""; }
    private static String first(String... values) { return Arrays.stream(values).filter(Objects::nonNull).filter(v -> !v.isBlank()).findFirst().orElse(""); }
}
