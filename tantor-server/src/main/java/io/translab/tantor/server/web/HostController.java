package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStatus;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.domain.JobType;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.HostStatusService;
import io.translab.tantor.server.service.JobService;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.web.bind.annotation.RequestHeader;

@RestController
@RequestMapping("/api/v1/ui/hosts")
@RequiredArgsConstructor
@Slf4j
public class HostController {

    private final HostRepository hostRepository;
    private final ClusterRepository clusterRepository;
    private final io.translab.tantor.server.repository.ExternalClusterNodeRepository externalClusterNodeRepository;
    private final io.translab.tantor.server.repository.ExternalClusterRepository externalClusterRepository;
    private final io.translab.tantor.server.repository.DiscoveryAgentRepository discoveryAgentRepository;
    private final TaskRepository taskRepository;
    private final io.translab.tantor.server.service.ActivityAlertService activityAlertService;
    private final HostStatusService hostStatusService;
    private final ObjectMapper objectMapper;
    private final JobService jobService;
    private final AuditService auditService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> getAllHosts() {
        Map<String, Map<String, Object>> hostsByIdentity = new LinkedHashMap<>();
        hostRepository.findAll().stream()
                .map(this::hostSummary)
                .forEach(host -> hostsByIdentity.merge(hostIdentity(host), host, this::preferredHostSummary));
        return ResponseEntity.ok(List.copyOf(hostsByIdentity.values()));
    }

    @PostMapping("/{id}/check-prerequisites")
    public ResponseEntity<?> checkPrerequisites(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> options
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_PREREQUISITES)) {
            return unauthorized();
        }
        Host host = hostRepository.findById(id).orElse(null);
        if (host == null) return ResponseEntity.notFound().build();
        String effectiveStatus = hostStatusService.effectiveStatus(host);
        if (!"ONLINE".equalsIgnoreCase(effectiveStatus)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "message",
                    "Host must be ONLINE before prerequisites can be checked. Current status: " + effectiveStatus
            ));
        }

        Task task;
        try {
            task = hostTask(id, "CHECK_PREREQUISITES", options);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("message", "Invalid prerequisite options."));
        }
        taskRepository.save(task);
        auditService.record("PREREQUISITE", "PREREQUISITE_CHECK_REQUESTED", "HOST", id,
                host.getClusterId(), "REQUESTED", null, null, null,
                Map.of("taskId", task.getId().toString(),
                        "mode", options == null ? "" : String.valueOf(options.getOrDefault("mode", "")),
                        "requiredPorts", options == null ? "" : String.valueOf(options.getOrDefault("required_ports", ""))));
        return ResponseEntity.ok(Map.of("taskId", task.getId().toString()));
    }

    @PostMapping("/{id}/check-ports")
    public ResponseEntity<?> checkPorts(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> options
    ) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_PREREQUISITES)) {
            return unauthorized();
        }
        Host host = hostRepository.findById(id).orElse(null);
        if (host == null) return ResponseEntity.notFound().build();
        String effectiveStatus = hostStatusService.effectiveStatus(host);
        if (!"ONLINE".equalsIgnoreCase(effectiveStatus)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "message",
                    "Host must be ONLINE before ports can be checked. Current status: " + effectiveStatus
            ));
        }

        Task task;
        try {
            task = hostTask(id, "CHECK_PORTS", options);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("message", "Invalid port check options."));
        }
        taskRepository.save(task);
        auditService.record("PREREQUISITE", "PORT_CHECK_REQUESTED", "HOST", id,
                host.getClusterId(), "REQUESTED", null, null, null,
                Map.of("taskId", task.getId().toString(),
                        "requiredPorts", options == null ? "" : String.valueOf(options.getOrDefault("required_ports", ""))));
        return ResponseEntity.ok(Map.of("taskId", task.getId().toString()));
    }

    @PostMapping("/{id}/schema-directory-candidates")
    public ResponseEntity<?> schemaDirectoryCandidates(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> options) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_PREREQUISITES)) {
            return unauthorized();
        }
        return createDataServiceHostTask(id, "SCHEMA_DIRECTORY_CANDIDATES", options);
    }

    @PostMapping("/{id}/precheck-schema")
    public ResponseEntity<?> precheckSchema(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id,
            @RequestBody Map<String, Object> options) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_PREREQUISITES)) {
            return unauthorized();
        }
        return createDataServiceHostTask(id, "PRECHECK_SCHEMA", options);
    }

    @PostMapping("/{id}/connect-directory-candidates")
    public ResponseEntity<?> connectDirectoryCandidates(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id,
            @RequestBody(required = false) Map<String, Object> options) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_PREREQUISITES)) {
            return unauthorized();
        }
        return createDataServiceHostTask(id, "CONNECT_DIRECTORY_CANDIDATES", options);
    }

    @PostMapping("/{id}/precheck-connect")
    public ResponseEntity<?> precheckConnect(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id,
            @RequestBody Map<String, Object> options) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_PREREQUISITES)) {
            return unauthorized();
        }
        return createDataServiceHostTask(id, "PRECHECK_CONNECT", options);
    }

    private ResponseEntity<?> createDataServiceHostTask(String id, String command, Map<String, Object> options) {
        Host host = hostRepository.findById(id).orElse(null);
        if (host == null) return ResponseEntity.notFound().build();
        if ("PRECHECK_SCHEMA".equals(command) || "PRECHECK_CONNECT".equals(command)) {
            String blocked = io.translab.tantor.server.service.DataServiceAgentSupport.precheckBlockReason(host.getAgentVersion());
            if (blocked != null) return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", blocked));
        }
        if (!"ONLINE".equalsIgnoreCase(hostStatusService.agentConnectivityStatus(host))) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", "Host agent must be ONLINE."));
        }
        try {
            Task task = dataServiceHostTask(id, command, options);
            taskRepository.save(task);
            return ResponseEntity.ok(Map.of("taskId", task.getId().toString()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("message", e.getMessage()));
        }
    }

    @PostMapping("/{id}/fix-prerequisites")
    public ResponseEntity<?> fixPrerequisites(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_PREREQUISITES)) {
            return unauthorized();
        }
        Host host = hostRepository.findById(id).orElse(null);
        if (host == null) return ResponseEntity.notFound().build();
        if (!"ONLINE".equalsIgnoreCase(hostStatusService.effectiveStatus(host))) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", "Host must be ONLINE before prerequisites can be fixed."));
        }
        Task task = new Task();
        task.setHostId(id);
        task.setCommand("APPLY_PREREQUISITES");
        task.setStatus("PENDING");
        task.setParameters(requesterParameters());
        taskRepository.save(task);
        auditService.record("PREREQUISITE", "PREREQUISITE_FIX_REQUESTED", "HOST", id,
                host.getClusterId(), "REQUESTED", null, null, null,
                Map.of("taskId", task.getId().toString(), "confirmation", true));
        return ResponseEntity.ok(Map.of("taskId", task.getId().toString()));
    }

    @PostMapping("/{id}/reboot")
    public ResponseEntity<?> rebootHost(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id,
            @RequestBody Map<String, Object> request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_REBOOT)) {
            return unauthorized();
        }
        Host host = hostRepository.findById(id).orElse(null);
        if (host == null) return ResponseEntity.notFound().build();
        if (!Boolean.parseBoolean(String.valueOf(request.getOrDefault("confirmed", false)))) {
            return ResponseEntity.badRequest().body(Map.of("message", "Explicit reboot confirmation is required."));
        }
        if (!"ONLINE".equalsIgnoreCase(hostStatusService.effectiveStatus(host))) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", "Host must be ONLINE before reboot can be scheduled."));
        }
        Task task = new Task();
        task.setHostId(id);
        task.setCommand("REBOOT_HOST");
        task.setStatus("PENDING");
        task.setParameters("{\"reason\":\"prerequisite-remediation\"}");
        taskRepository.save(task);
        auditService.record("RESTART", "HOST_REBOOT_REQUESTED", "HOST", id, host.getClusterId(),
                "REQUESTED", null, null, null, Map.of("taskId", task.getId().toString(), "reason", "prerequisite-remediation"));
        return ResponseEntity.ok(Map.of("taskId", task.getId().toString()));
    }

    @GetMapping("/{id}/check-prerequisites/{taskId}")
    public ResponseEntity<?> prerequisiteResult(@PathVariable String id, @PathVariable UUID taskId) {
        return taskRepository.findById(taskId)
                .filter(task -> id.equals(task.getHostId()))
                .map(task -> ResponseEntity.ok(Map.of(
                        "taskId", task.getId().toString(),
                        "status", task.getStatus(),
                        "logOutput", task.getLogOutput() == null ? "" : task.getLogOutput(),
                        "errorMsg", task.getErrorMsg() == null ? "" : task.getErrorMsg()
                )))
                .orElse(ResponseEntity.notFound().build());
    }

    private Task hostTask(String hostId, String command, Map<String, Object> options) throws Exception {
        Task task = new Task();
        task.setHostId(hostId);
        task.setCommand(command);
        task.setStatus("PENDING");

        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("requested_by", io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        if (options != null) {
            copyOption(options, parameters, "mode");
            copyOption(options, parameters, "required_ports");
            copyOption(options, parameters, "java_home");
            copyOption(options, parameters, "javaHome");
        }
        task.setParameters(objectMapper.writeValueAsString(parameters));
        return task;
    }

    private Task dataServiceHostTask(String hostId, String command, Map<String, Object> options) throws Exception {
        Task task = new Task();
        task.setHostId(hostId);
        task.setCommand(command);
        task.setStatus("PENDING");
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("requested_by", io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        if (options != null) {
            options.forEach((key, value) -> {
                if (value != null && !String.valueOf(value).isBlank()
                        && !"artifact_url".equals(key) && !"checksum".equals(key)) {
                    parameters.put(key, String.valueOf(value));
                }
            });
            task.setArtifactUrl(String.valueOf(options.getOrDefault("artifact_url", "")));
            task.setChecksum(String.valueOf(options.getOrDefault("checksum", "")));
        }
        task.setParameters(objectMapper.writeValueAsString(parameters));
        return task;
    }

    private String requesterParameters() {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "requested_by", io.translab.tantor.server.security.SecurityUtils.getCurrentUsername()));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to capture request identity", e);
        }
    }
    private void copyOption(Map<String, Object> source, Map<String, Object> target, String key) {
        Object value = source.get(key);
        if (value != null && !String.valueOf(value).isBlank()) {
            target.put(key, String.valueOf(value));
        }
    }

    @PostMapping("/{id}/mark-unavailable")
    public ResponseEntity<?> markUnavailable(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_AVAILABILITY)) {
            return unauthorized();
        }
        return hostRepository.findById(id).map(host -> {
            String previous = host.getStatus();
            host.setStatus("OCCUPIED");
            hostRepository.save(host);
            activityAlertService.logAudit("WARN", "HOST", "MARK_OCCUPIED", "Host marked occupied", "HOST", id,
                    host.getClusterId(), "ONLINE", "OCCUPIED", "SUCCESS", null, null);
            auditService.record("HOST", "HOST_MARKED_OCCUPIED", "HOST", id, host.getClusterId(), "SUCCESS",
                    Map.of("status", String.valueOf(previous)), Map.of("status", "OCCUPIED"), null, null);
            return ResponseEntity.ok(hostSummary(host));
        }).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/mark-available")
    public ResponseEntity<?> markAvailable(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_AVAILABILITY)) {
            return unauthorized();
        }
        return hostRepository.findById(id).map(host -> {
            String previous = host.getStatus();
            host.setStatus("ONLINE");
            hostRepository.save(host);
            activityAlertService.logAudit("INFO", "HOST", "MARK_AVAILABLE", "Host marked available", "HOST", id,
                    host.getClusterId(), "OCCUPIED", "ONLINE", "SUCCESS", null, null);
            auditService.record("HOST", "HOST_MARKED_AVAILABLE", "HOST", id, host.getClusterId(), "SUCCESS",
                    Map.of("status", String.valueOf(previous)), Map.of("status", "ONLINE"), null, null);
            return ResponseEntity.ok(hostSummary(host));
        }).orElse(ResponseEntity.notFound().build());
    }
    @PostMapping("/{id}/approve")
    public ResponseEntity<?> approveHost(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_ONBOARDING)) {
            return unauthorized();
        }
        Host host = hostRepository.findById(id).orElse(null);
        if (host == null) return ResponseEntity.notFound().build();
        if (!"PENDING".equalsIgnoreCase(host.getStatus())) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", "Host is already connected or occupied."));
        }

        Job job = new Job();
        job.setType(JobType.ONBOARDING);
        job.setStatus(JobStatus.PENDING);
        job.setRollbackSupported(true);
        job.setResourceKey("host:" + id);
        JobStep step = new JobStep();
        step.setStepOrder(1);
        step.setName("Connect host " + host.getHostname());
        step.setTargetId(id);
        try {
            String payload = objectMapper.writeValueAsString(Map.of("hostId", id));
            job.setPayload(payload);
            step.setPayload(payload);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("message", "Unable to create onboarding job."));
        }
        Job saved = jobService.createJob(job, List.of(step));
        activityAlertService.logAudit("INFO", "APPROVAL", "APPROVE", "Host onboarding approved", "HOST", id,
                host.getClusterId(), "PENDING", "ONBOARDING_QUEUED", "SUCCESS", "APPROVED", "jobId=" + saved.getId());
        return ResponseEntity.ok(Map.of("jobId", saved.getId().toString(), "status", saved.getStatus().name()));
    }

    @Transactional
    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteHost(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.HOST_REMOVE)) {
            return unauthorized();
        }
        return hostRepository.findById(id).map(host -> {
            Map<String, Object> oldValue = Map.of("status", String.valueOf(host.getStatus()),
                    "clusterId", String.valueOf(host.getClusterId()), "hostname", String.valueOf(host.getHostname()));
            if (host.getClusterId() != null) {
                boolean assignedToActiveCluster = clusterRepository.findById(host.getClusterId())
                    .filter(cluster -> !"DELETED".equalsIgnoreCase(cluster.getStatus()))
                    .isPresent();
                if (assignedToActiveCluster) {
                    return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                        "message",
                        "This host is assigned to an active cluster. Delete or force-delete the cluster before disconnecting the host."
                    ));
                }
            }

            UUID previousClusterId = host.getClusterId();
            String previousStatus = host.getStatus();
            host.setClusterId(null);
            host.setStatus("PENDING");

            host.setRemoved(true);
            host.setLastHeartbeat(null);
            host.setAction("HOST_REMOVED");
            hostRepository.save(host);
            activityAlertService.logAudit("INFO", "HOST", "DISCONNECT", "Host disconnected from management", "HOST", id,
                    previousClusterId, previousStatus, "PENDING", "SUCCESS", null, null);
            auditService.record("HOST", "HOST_REMOVED", "HOST", id, null, "SUCCESS", oldValue,
                    Map.of("status", "PENDING", "clusterId", ""), null,
                    Map.of("operation", "disconnect", "recordRetained", true));
            return ResponseEntity.ok(Map.of(
                "message",
                "Host disconnected. It is now waiting in discovered nodes and can be connected again."
            ));
        }).orElse(ResponseEntity.notFound().build());
    }

    private ResponseEntity<?> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
    }

    @GetMapping("/{id}/check-port/{port}")
    public ResponseEntity<?> checkPort(@PathVariable String id, @PathVariable int port) {
        Host host = hostRepository.findById(id).orElse(null);
        if (host == null) return ResponseEntity.notFound().build();

        if (port < 1 || port > 65535) {
            return ResponseEntity.badRequest().body(Map.of("message", "Invalid port number"));
        }

        String targetIp = host.getHostname();
        if (host.getIpAddresses() != null && !host.getIpAddresses().isEmpty()) {
            String raw = host.getIpAddresses().replace("[", "").replace("]", "").replace("\"", "").trim();
            if (!raw.isEmpty()) {
                targetIp = raw.split(",")[0].trim();
            }
        }

        boolean portInUse;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(targetIp, port), 2000);
            portInUse = true;
        } catch (Exception e) {
            portInUse = false;
        }

        boolean isFree = !portInUse;
        String message = isFree
            ? "Port " + port + " is free on " + host.getHostname()
            : "Port " + port + " is already in use on " + host.getHostname();

        log.info("Port check: {}:{} -> {}", targetIp, port, isFree ? "FREE" : "IN_USE");

        return ResponseEntity.ok(Map.of(
            "free", isFree,
            "host", host.getHostname(),
            "ip", targetIp,
            "port", port,
            "message", message
        ));
    }

    private Map<String, Object> hostSummary(Host host) {
        String effectiveStatus = hostStatusService.effectiveStatus(host);
        String agentConnectivityStatus = hostStatusService.agentConnectivityStatus(host);
        host.setStatus(effectiveStatus);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("id", host.getId());
        summary.put("agentName", host.getAgentName());
        summary.put("hostname", host.getHostname());
        summary.put("ipAddresses", host.getIpAddresses());
        summary.put("osDetails", host.getOsDetails());
        summary.put("agentVersion", host.getAgentVersion());
        summary.put("agentPath", host.getAgentPath());
        summary.put("agentStatus", agentConnectivityStatus);
        boolean discoveryAgent = hostStatusService.isDiscoveryAgent(host);
        summary.put("agentType", discoveryAgent ? "KAFKA_DISCOVERY" : "HOST");
        summary.put("deployable", !discoveryAgent);
        summary.put("javaVersion", host.getJavaVersion());
        summary.put("status", effectiveStatus);
        summary.put("lastHeartbeat", host.getLastHeartbeat());
        summary.put("cpuUsagePct", host.getCpuUsagePct());
        summary.put("memTotalMb", host.getMemTotalMb());
        summary.put("memUsedMb", host.getMemUsedMb());
        summary.put("diskTotalGb", host.getDiskTotalGb());
        summary.put("diskUsedGb", host.getDiskUsedGb());

        Optional<Cluster> activeCluster = host.getClusterId() == null
                ? Optional.empty()
                : clusterRepository.findById(host.getClusterId()).filter(cluster -> !"DELETED".equalsIgnoreCase(cluster.getStatus()));
                
        boolean hasExternalCluster = false;
        UUID extClusterId = null;
        String extClusterName = "External Cluster";

        String hostIpRaw = host.getIpAddresses() != null ? host.getIpAddresses().replace("[", "").replace("]", "").replace("\"", "").trim() : "";
        String[] hostIps = hostIpRaw.isEmpty() ? new String[0] : hostIpRaw.split(",");

        List<io.translab.tantor.server.domain.ExternalClusterNode> allExternalNodes = externalClusterNodeRepository.findAll();
        for (io.translab.tantor.server.domain.ExternalClusterNode n : allExternalNodes) {
            if ((host.getHostname() != null && host.getHostname().equals(n.getHost())) || 
                (hostIps.length > 0 && java.util.Arrays.asList(hostIps).contains(n.getHost()))) {
                hasExternalCluster = true;
                extClusterId = n.getClusterId();
                if (extClusterId != null) {
                    extClusterName = externalClusterRepository.findById(extClusterId)
                        .map(io.translab.tantor.server.domain.ExternalCluster::getName)
                        .orElse("External Cluster");
                }
                break;
            }
        }

        if (hasExternalCluster) {
            summary.put("available", false);
            summary.put("status", "OCCUPIED_EXTERNAL");
            summary.put("clusterName", extClusterName);
            if (extClusterId != null) {
                summary.put("clusterId", extClusterId.toString());
                
                // Override agent details with Discovery Agent if it exists for this cluster
                List<io.translab.tantor.server.domain.DiscoveryAgent> discAgents = discoveryAgentRepository.findByClusterId(extClusterId);
                if (discAgents != null && !discAgents.isEmpty()) {
                    io.translab.tantor.server.domain.DiscoveryAgent discAgent = discAgents.get(0);
                    summary.put("agentName", discAgent.getAgentName() == null || discAgent.getAgentName().isBlank()
                            ? discAgent.getHostname() : discAgent.getAgentName());
                    summary.put("agentStatus", discAgent.getStatus());
                    summary.put("agentType", "KAFKA_DISCOVERY");
                    summary.put("agentVersion", discAgent.getVersion());
                    summary.put("agentPath", "/srv/tantor-agent/tantor-discovery-agent-linux");
                }
            }
        } else if ("PENDING".equalsIgnoreCase(effectiveStatus)) {
            // Host is awaiting approval — keep it in the modal, not the main table
            summary.put("available", false);
            summary.put("status", "PENDING");
        } else if ("OFFLINE".equalsIgnoreCase(effectiveStatus) || "REMOVED".equalsIgnoreCase(effectiveStatus)) {
            summary.put("available", false);
            summary.put("status", effectiveStatus);
        } else if (activeCluster.isPresent() || "OCCUPIED".equalsIgnoreCase(effectiveStatus)) {
            summary.put("available", false);
            summary.put("status", "OCCUPIED_INTERNAL");
        } else {
            summary.put("available", true);
            summary.put("status", "AVAILABLE");
        }
        
        activeCluster.ifPresent(cluster -> {
            summary.put("clusterId", cluster.getId().toString());
            summary.put("clusterName", cluster.getName());
            summary.put("kafkaClusterId", cluster.getKafkaClusterId());
        });
        return summary;
    }

    private String hostIdentity(Map<String, Object> host) {
        String ipAddresses = String.valueOf(host.getOrDefault("ipAddresses", ""));
        String ip = primaryIp(ipAddresses);
        if (!ip.isBlank()) {
            return "ip:" + ip;
        }
        String hostname = String.valueOf(host.getOrDefault("hostname", "")).trim().toLowerCase();
        if (!hostname.isBlank() && !"null".equals(hostname)) {
            return "host:" + hostname;
        }
        return "id:" + String.valueOf(host.get("id"));
    }

    private Map<String, Object> preferredHostSummary(Map<String, Object> existing, Map<String, Object> candidate) {
        boolean candidateOnline = "ONLINE".equalsIgnoreCase(String.valueOf(candidate.get("agentStatus")));
        boolean existingOnline = "ONLINE".equalsIgnoreCase(String.valueOf(existing.get("agentStatus")));
        if (candidateOnline != existingOnline) {
            return candidateOnline ? candidate : existing;
        }
        OffsetDateTime candidateHeartbeat = parseHeartbeat(candidate.get("lastHeartbeat"));
        OffsetDateTime existingHeartbeat = parseHeartbeat(existing.get("lastHeartbeat"));
        if (candidateHeartbeat != null && (existingHeartbeat == null || candidateHeartbeat.isAfter(existingHeartbeat))) {
            return candidate;
        }
        return existing;
    }

    private OffsetDateTime parseHeartbeat(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(String.valueOf(value));
        } catch (Exception ignored) {
            return null;
        }
    }

    private String primaryIp(String raw) {
        if (raw == null || raw.isBlank() || "null".equalsIgnoreCase(raw)) {
            return "";
        }
        String cleaned = raw.replace("[", "").replace("]", "").replace("\"", "");
        String[] parts = cleaned.split(",");
        for (String part : parts) {
            String ip = part.trim();
            if (ip.startsWith("192.168.")) {
                return ip;
            }
        }
        for (String part : parts) {
            String ip = part.trim();
            if (!ip.isBlank() && !ip.startsWith("127.") && !ip.startsWith("172.")) {
                return ip;
            }
        }
        return parts.length == 0 ? "" : parts[0].trim();
    }
}
