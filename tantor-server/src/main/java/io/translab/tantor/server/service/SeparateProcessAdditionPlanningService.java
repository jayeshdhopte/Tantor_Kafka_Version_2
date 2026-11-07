package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import io.translab.tantor.server.repository.ClusterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** Phase 6 review. One fresh host, two proposed identities; never dispatches a task or reserves an ID. */
@Service
@RequiredArgsConstructor
public class SeparateProcessAdditionPlanningService {
    private final ControllerAdditionPlanningService controllers;
    private final ClusterRepository clusters;
    private final ObjectMapper mapper;

    public record Process(String role, Integer nodeId, String processRoles, String configurationFile,
                          String serviceName, Integer listenerPort, List<String> requiredDirectories,
                          String evidenceStatus) {}
    public record RecoveryRule(String checkpoint, String requirement) {}
    public record Review(String schemaVersion, UUID clusterId, String hostId, String kafkaClusterId,
                         String kafkaVersion, String quorumMode, String brokerRack, String artifactId,
                         String artifactChecksum, Instant observedAt, Instant expiresAt, String contextHash,
                         String status, boolean executionEnabled, boolean maintenanceRequired,
                         List<Process> processes, List<ControllerAdditionPlanningService.Participant> participants,
                         List<ControllerAdditionPlanningService.Step> steps, List<Check> checks,
                         List<RecoveryRule> recoveryRules, List<String> blockers) {}

    public Review review(UUID clusterId, String hostId, String requestedRack) {
        String rack = Objects.toString(requestedRack, "").strip();
        if (!rack.matches("[A-Za-z0-9_.-]{0,128}"))
            throw error(HttpStatus.BAD_REQUEST, "Broker rack must use up to 128 letters, numbers, dots, underscores or hyphens.");
        // Reuse the permission-protected controller scope, live Kafka observation,
        // artifact policy and fresh-host constraints. No readiness task is created.
        var base = controllers.review(clusterId, hostId);
        var cluster = clusters.findWithServicesById(clusterId)
                .orElseThrow(() -> error(HttpStatus.NOT_FOUND, "Cluster not found."));
        if (!"kraft".equalsIgnoreCase(cluster.getMode()) || "EXTERNAL".equalsIgnoreCase(cluster.getOriginType())
                || !"SUCCESS".equals(cluster.getStatus()) || !Objects.equals(cluster.getKafkaClusterId(), base.kafkaClusterId())
                || !Objects.equals(cluster.getKafkaVersion(), base.kafkaVersion()))
            throw error(HttpStatus.CONFLICT, "Cluster identity or availability changed during planning. Refresh the plan.");
        var config = config(cluster.getConfigJson());
        var blockers = new ArrayList<>(base.blockers());
        var checks = new ArrayList<>(base.checks());
        var broker = AddNodeIdentityAllocator.broker(cluster, new HashSet<>(base.observedNodeIds()));
        Integer brokerId = broker.nodeId() > 0 ? broker.nodeId() : null;
        Integer controllerId = base.proposedControllerId();
        boolean distinct = broker.available() && controllerId != null && !controllerId.equals(brokerId);
        if (!broker.available()) blockers.add(broker.reason());
        if (controllerId != null && controllerId.equals(brokerId))
            blockers.add("The script's broker and controller proposals collide at ID " + controllerId + ". Resolve the identity policy explicitly; do not silently skip a broker ID or renumber an existing process.");
        checks.add(check("S01", "Two distinct proposed process identities", distinct ? "PASS" : "FAIL",
                "Broker maximum + 1; controller maximum (at least 100) + 1; both globally unused",
                "Broker " + brokerId + "; controller " + controllerId));

        Integer brokerPort = port(config.getOrDefault("broker_port", config.get("listener_port")));
        Integer controllerPort = uniformControllerPort(base.controllerEndpoints());
        Object configuredControllerValue = config.getOrDefault("controller_port", config.get("controller_listener_port"));
        Integer configuredControllerPort = port(configuredControllerValue);
        boolean portsConsistent = controllerPort != null && (configuredControllerValue == null || configuredControllerPort != null && configuredControllerPort.equals(controllerPort));
        boolean portsDistinct = brokerPort != null && portsConsistent && !brokerPort.equals(controllerPort);
        checks.add(check("S02", "Inherited listener layout", portsDistinct ? "PASS" : "FAIL",
                "Distinct broker/controller ports matching the existing cluster; binding not yet inspected",
                "Broker " + brokerPort + "; observed controllers " + base.controllerEndpoints()));
        if (!portsDistinct) blockers.add("Resolve missing, inconsistent or overlapping inherited listener ports before freezing a two-process plan.");
        // Relative paths describe the required layout, not approved/inspected absolute paths.
        var processes = List.of(
                new Process("controller", controllerId, "controller", "controller.properties", unit("controller", controllerId),
                        controllerPort, List.of("controller/data", "controller/metadata", "controller/logs"), "NOT_INSPECTED"),
                new Process("broker", brokerId, "broker", "broker.properties", unit("broker", brokerId),
                        brokerPort, List.of("broker/data", "broker/logs"), "NOT_INSPECTED"));
        checks.add(check("S03", "Two-process storage and service ownership", "UNKNOWN",
                "Distinct absolute configurations, units, data/metadata and application logs; no overlap or foreign data", "Not inspected"));
        checks.add(check("S04", "Combined host resources and exporter ports", "UNKNOWN",
                "Two JVM heaps plus OS reserve; filesystem capacity counted per mount; distinct listener/JMX/exporter ports", "Not inspected"));
        checks.add(check("S05", "Broker rack and controller-first dependency", "UNKNOWN",
                "Candidate's actual rack verified against live rack policy; controller membership verified before broker start",
                "Requested rack " + (rack.isBlank() ? "not specified" : rack) + "; final layout validation pending"));
        blockers.add("This layout requires one fresh host for two new processes. Adding a second process to an occupied host is not supported by this flow.");
        blockers.add("Two-process readiness is incomplete: inspect both storage trees, units, listener/JMX/exporter ports, rack and combined memory/disk requirements. A broker-only PASS is insufficient.");
        blockers.add("Use final layout validation below to obtain a server-owned execution preview. Submission also requires the release gate, fresh evidence and atomic host/identity reservations.");
        boolean knownMode = Set.of("STATIC", "DYNAMIC").contains(base.quorumMode());
        var steps = knownMode ? workflow(base.quorumMode()) : List.<ControllerAdditionPlanningService.Step>of();
        var fingerprint = new TreeMap<String, Object>();
        fingerprint.put("schema", "add-separate-review/v1");
        fingerprint.put("controllerContext", base.contextHash());
        fingerprint.put("processes", processes);
        fingerprint.put("rack", rack);
        fingerprint.put("clusterConfig", Objects.toString(cluster.getConfigJson(), ""));
        return new Review("add-separate-review/v1", clusterId, base.hostId(), base.kafkaClusterId(), base.kafkaVersion(),
                base.quorumMode(), rack, base.artifactId(), base.artifactChecksum(), base.observedAt(), base.expiresAt(),
                hash(fingerprint), "PLANNING_ONLY", false, base.maintenanceRequired(), processes, base.participants(),
                steps, List.copyOf(checks), recoveryRules(), List.copyOf(blockers));
    }

    static List<ControllerAdditionPlanningService.Step> workflow(String mode) {
        var steps = new ArrayList<ControllerAdditionPlanningService.Step>();
        steps.add(step("P0", "Final checks and atomic reservation of host and both IDs", "One parent operation; no independent broker job", "add_host.yml; tasks/cluster_lock.yml"));
        steps.add(step("P1", "Verify the shared Kafka package and stage two isolated process layouts", "Same Kafka version/cluster ID; separate role-only configs, units, data and logs", "tasks/storage_format.yml; templates/kafka_server.properties.j2"));
        if ("STATIC".equals(mode)) {
            for (var original : ControllerAdditionPlanningService.workflow(mode)) {
                // Publish neither process until the parent's final joint verification.
                if (original.code().equals("S4")) steps.add(step("S4", "Format only the two candidate storage trees using the existing cluster ID", "Distinct controller/broker IDs and configuration files; never format existing members", "tasks/static_quorum_expand.yml · Phase 4"));
                else if (original.code().equals("S6")) steps.add(step("P_BROKER", "Start and verify the candidate broker after controller quorum verification", "Candidate broker plus all existing brokers stopped by maintenance", "tasks/static_quorum_expand.yml · Phase 6"));
                else if (original.code().equals("S7")) steps.add(step("P_HEALTH", "Verify stable health and complete maintenance", "Both candidate identities, existing services, URP and smoke test", original.script()));
                else steps.add(original);
            }
        } else if ("DYNAMIC".equals(mode)) {
            steps.add(step("P2", "Format the two empty candidate storage trees", "Same existing cluster ID; distinct node IDs; --no-initial-controllers on the controller only", "tasks/storage_format.yml"));
            steps.add(step("P3", "Start the controller observer, verify catch-up and admit its voting identity", "Controller ID, directory ID, endpoints and healthy quorum before any broker start", "tasks/service_deploy.yml; tasks/verify_controller.yml; prototype controller-first gate"));
            steps.add(step("P4", "Start only the candidate broker after controller verification", "Separate broker storage and broker.properties; retain controller admission checkpoint", "tasks/service_deploy.yml"));
            steps.add(step("P5", "Verify unfenced broker registration and stable cluster health", "Both candidate roles; existing brokers/controllers remain running", "tasks/verify_broker.yml; tasks/post_health_gate.yml"));
        }
        steps.add(step("P_PUBLISH", "Publish both verified process identities atomically", "Two service rows on one host; release the parent reservation only after both pass", "Prototype: jobStageDefinitions / finish"));
        return List.copyOf(steps);
    }

    static List<RecoveryRule> recoveryRules() {
        return List.of(
                new RecoveryRule("Before mutation", "Cancel safely and release the host and both IDs together."),
                new RecoveryRule("Uncertain format or start", "Inspect the same process/storage identity; retain receipts and both reservations. Never blindly reformat or create a replacement job."),
                new RecoveryRule("Uncertain controller admission", "Reconcile actual voters, directory ID and endpoints before retry. Do not remove or stop a possibly admitted controller."),
                new RecoveryRule("Controller verified; broker failed", "Keep the controller checkpoint and both reservations. Recheck controller health and resume only incomplete broker work; publish neither process yet."),
                new RecoveryRule("Static maintenance failure", "Preserve all participant reservations and verified backups; resume/recover the applicable script phase using actual files and quorum state."),
                new RecoveryRule("Both verified; publication interrupted", "Reverify both roles and complete the same idempotent publication transaction; never repeat formatting or admission."));
    }
    private static ControllerAdditionPlanningService.Step step(String code, String name, String scope, String script) {
        return new ControllerAdditionPlanningService.Step(code, name, scope, script);
    }
    private Check check(String code, String name, String status, String expected, String observed) {
        return new Check(code, name, "SEPARATE", status, expected, observed,
                status.equals("PASS") ? "" : "Complete two-process readiness before execution. Planning does not reserve identities.", Instant.now(), "BACKEND_PLAN");
    }
    private Map<String, Object> config(String value) {
        try { return mapper.readValue(value == null || value.isBlank() ? "{}" : value, new com.fasterxml.jackson.core.type.TypeReference<>() {}); }
        catch (Exception e) { throw error(HttpStatus.CONFLICT, "Stored cluster configuration is unreadable."); }
    }
    private static Integer port(Object value) {
        try { int number = Integer.parseInt(Objects.toString(value, "")); return number > 0 && number <= 65535 ? number : null; }
        catch (NumberFormatException e) { return null; }
    }
    private static Integer uniformControllerPort(List<String> endpoints) {
        var ports = new HashSet<Integer>();
        for (String endpoint : endpoints) {
            int split = endpoint.lastIndexOf(':');
            Integer value = split < 1 ? null : port(endpoint.substring(split + 1));
            if (value == null) return null;
            ports.add(value);
        }
        return ports.size() == 1 ? ports.iterator().next() : null;
    }
    private static String unit(String role, Integer id) { return id == null ? "" : "tantor-kafka-" + role + "-" + id + ".service"; }
    private String hash(Object value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsString(value).getBytes(StandardCharsets.UTF_8))); }
        catch (Exception e) { throw new IllegalStateException("Cannot fingerprint separate-process review.", e); }
    }
    private static ResponseStatusException error(HttpStatus status, String message) { return new ResponseStatusException(status, message); }
}
