package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import io.translab.tantor.server.repository.DiscoveryAgentRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.DiscoveryAgent;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.security.TruststoreStorageService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.scheduling.annotation.Scheduled;
import io.translab.tantor.server.audit.AuditService;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.translab.tantor.server.repository.ExternalClusterRepository;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExternalClusterService {

    private static final String EXTERNAL_MODE = "EXTERNAL";

    @Value("${tantor.discovery-agent.heartbeat-timeout-seconds:45}")
    private long discoveryAgentHeartbeatTimeoutSeconds;

    private final ClusterRepository clusterRepository;
    private final ExternalClusterRepository externalClusterRepository;
    private final io.translab.tantor.server.repository.ExternalClusterNodeRepository externalClusterNodeRepository;
    private final ClusterServiceAssignmentRepository clusterServiceAssignmentRepository;
    private final HostRepository hostRepository;
    private final DiscoveryAgentRepository discoveryAgentRepository;
    private final KafkaAdminService kafkaAdminService;
    private final ObjectMapper objectMapper;
    private final ActivityAlertService activityAlertService;
    private final AuditService auditService;
    private final EncryptionService encryptionService;
    private final TruststoreStorageService truststoreStorageService;
    private final PrometheusMonitoringService prometheusMonitoringService;

    private final Map<String, ExternalAgentTask> pendingTasks = new ConcurrentHashMap<>();
    private final Map<String, ExternalAgentTask> completedTasks = new ConcurrentHashMap<>();
    private final Map<String, ExternalDiscoveryReport> pendingDiscoveries = new ConcurrentHashMap<>();
    private final Map<String, OffsetDateTime> acceptedDiscoveriesByAgent = new ConcurrentHashMap<>();

    public boolean isClusterNameAvailable(String name) {
        String normalizedName = name == null ? "" : name.trim();
        if (normalizedName.isBlank()) {
            return true;
        }
        return !clusterRepository.existsActiveByNormalizedName(normalizedName)
                && !externalClusterRepository.existsActiveByNormalizedName(normalizedName);
    }

    private void requireAvailableClusterName(String name) {
        if (!isClusterNameAvailable(name)) {
            throw new ClusterNameConflictException(
                    "A cluster with this name already exists. Choose a different name.");
        }
    }

    public Map<String, String> getExternalTaskData(String taskId) {
        for (ExternalAgentTask task : pendingTasks.values()) {
            if (taskId.equals(task.getTaskId())) {
                return task.getData();
            }
        }
        return null;
    }

    public void removeExternalTask(String taskId) {
        pendingTasks.entrySet().removeIf(entry -> taskId.equals(entry.getValue().getTaskId()));
    }

    public Map<String, Object> testBootstrap(BootstrapExternalClusterRequest request) {
        String query = request.getBootstrapServers().trim();
        String queryHost = extractHostFromBootstrap(query);

        Map<String, Object> result = new LinkedHashMap<>();
        boolean adminSuccess = false;
        
        try {
            // 1. PRIMARY: Try connecting directly via Kafka Admin API FIRST
            // Since cluster is not registered yet, we create a temporary DTO to pass properties
            ExternalCluster tempCluster = new ExternalCluster();
            tempCluster.setBootstrapServers(query);
            tempCluster.setSecurityProtocol(request.getSecurityProtocol());
            tempCluster.setSaslMechanism(request.getSaslMechanism());
            tempCluster.setSaslUsername(request.getSaslUsername());
            tempCluster.setSaslPasswordEncrypted(request.getSaslPassword()); // Plaintext during test
            tempCluster.setTruststoreType(request.getTruststoreType());
            tempCluster.setTruststorePasswordEncrypted(request.getTruststorePassword());
            tempCluster.setKeystoreType(request.getKeystoreType());
            tempCluster.setKeystorePasswordEncrypted(request.getKeystorePassword());
            tempCluster.setKeyPasswordEncrypted(request.getKeyPassword());
            tempCluster.setDisableHostnameVerification(Boolean.TRUE.equals(request.getDisableHostnameVerification()));
            
            // Handle temporary truststore file for test connection
            if (request.getTruststoreBase64() != null && !request.getTruststoreBase64().isBlank()) {
                String tempPath = truststoreStorageService.saveTruststore(UUID.randomUUID(), request.getTruststoreType(), request.getTruststoreBase64());
                tempCluster.setTruststorePath(tempPath);
            }
            if (request.getKeystoreBase64() != null && !request.getKeystoreBase64().isBlank()) {
                String tempPath = truststoreStorageService.saveTruststore(UUID.randomUUID(), "keystore_" + request.getKeystoreType(), request.getKeystoreBase64());
                tempCluster.setKeystorePath(tempPath);
            }

            try {
                Map<String, Object> adminData = kafkaAdminService.inspectBootstrapServers(tempCluster, false); // false = don't decrypt since we passed plaintext
                result.putAll(adminData);
                enrichTestConnectionNodesFromDiscovery(result);
                adminSuccess = true;
            } finally {
                // Clean up temporary files
                if (tempCluster.getTruststorePath() != null) {
                    try { java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(tempCluster.getTruststorePath())); } catch (Exception ignored) {}
                }
                if (tempCluster.getKeystorePath() != null) {
                    try { java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(tempCluster.getKeystorePath())); } catch (Exception ignored) {}
                }
            }
        } catch (Exception e) {
            // It failed. Don't throw yet, check if agent is enrolled as fallback.
            result.put("connected", false);
            result.put("message", e.getMessage());
        }

        // 2. Check if a Discovery Agent has already reported this cluster
        boolean agentFound = false;
        for (Map.Entry<String, ExternalDiscoveryReport> entry : pendingDiscoveries.entrySet()) {
            ExternalDiscoveryReport report = entry.getValue();
            boolean matchBootstrap = report.getBootstrapServers() != null && report.getBootstrapServers().contains(query);
            boolean matchHostname = report.getHostname() != null && report.getHostname().equalsIgnoreCase(queryHost);
            boolean matchIp = report.getIpAddresses() != null && report.getIpAddresses().contains(queryHost);
            
            if (matchBootstrap || matchHostname || matchIp) {
                agentFound = true;
                result.put("agentFound", true);
                result.put("discoveryKey", entry.getKey());
                result.put("agentType", "KAFKA_DISCOVERY");
                
                // If AdminClient failed, do NOT force success. We just pass the agent data as extra context
                // but leave connected = false so the UI knows the direct connection failed.
                if (!adminSuccess) {
                    result.put("clusterId", report.getKafkaClusterId());
                    result.put("cluster_id", report.getKafkaClusterId());
                    result.put("kafkaVersion", blankToDefault(report.getKafkaVersion(), "Unknown"));
                    result.put("kafka_version", blankToDefault(report.getKafkaVersion(), "Unknown"));
                    result.put("kafkaMode", blankToDefault(report.getKafkaMode(), "Unknown"));
                    result.put("mode", blankToDefault(report.getKafkaMode(), "Unknown"));
                    // Do NOT override brokerCount or security_protocol from the agent here
                    // because the test failed, meaning the listener the user typed is invalid or unreachable.
                    // If we blindly copied the agent's SASL_SSL it would confuse them.
                    result.put("message", result.getOrDefault("message", "Direct Admin API connection failed. Agent is enrolled, but the bootstrap port is unreachable or invalid."));
                } else {
                    // Kafka's Admin API exposes brokers and the elected controller,
                    // but it does not reliably identify whether that controller is
                    // backed by ZooKeeper or the KRaft metadata quorum. The discovery
                    // agent reads the running properties file, so its explicit mode is
                    // authoritative even when the AdminClient connection succeeds.
                    String discoveredMode = normalizeKafkaMode(report.getKafkaMode());
                    if (discoveredMode != null) {
                        result.put("kafkaMode", discoveredMode);
                        result.put("mode", discoveredMode);
                    }
                    result.put("message", "Direct Connection successful. Discovery Agent also enrolled.");
                }
                break;
            }
        }
        
        if (!agentFound) {
            result.put("agentFound", false);
            // If admin also failed and no agent found, rethrow to trigger the UI error properly
            if (!adminSuccess) {
                throw new IllegalArgumentException(String.valueOf(result.getOrDefault("message", "Admin connection failed and no agent enrolled.")));
            }
        }

        normalizeInspectionNodeRolesForMode(
                result,
                firstString(result, "mode", "kafkaMode", "kafka_mode"));

        // 3. Inject node-level agent availability
        if (adminSuccess && result.get("brokers") != null) {
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) result.get("brokers");
            OffsetDateTime now = OffsetDateTime.now();
            List<DiscoveryAgent> dbAgents = discoveryAgentRepository.findAll();
            
            for (Map<String, Object> node : nodes) {
                String host = String.valueOf(node.get("host"));
                boolean nodeAgentFound = false;
                
                // Check pending discoveries first
                for (Map.Entry<String, ExternalDiscoveryReport> entry : pendingDiscoveries.entrySet()) {
                    ExternalDiscoveryReport report = entry.getValue();
                    boolean match = (report.getHostname() != null && report.getHostname().equalsIgnoreCase(host)) || 
                                    (report.getIpAddresses() != null && report.getIpAddresses().contains(host));
                    if (match && report.isRunning()) {
                        try {
                            OffsetDateTime lastSeen = OffsetDateTime.parse(report.getLastSeen());
                            if (java.time.Duration.between(lastSeen, now).getSeconds() <= 120) {
                                node.put("hasActiveAgent", true);
                                node.put("agentDiscoveryKey", entry.getKey());
                                nodeAgentFound = true;
                                break;
                            }
                        } catch (Exception ignored) {}
                    }
                }
                
                // If not in pending, check DB
                if (!nodeAgentFound) {
                    for (DiscoveryAgent agent : dbAgents) {
                        boolean match = (agent.getHostname() != null && agent.getHostname().equalsIgnoreCase(host)) ||
                                        (agent.getIpAddresses() != null && agent.getIpAddresses().contains(host));
                        if (match && "ONLINE".equalsIgnoreCase(agent.getStatus()) && agent.getLastHeartbeat() != null) {
                            if (java.time.Duration.between(agent.getLastHeartbeat(), now).getSeconds() <= 120) {
                                node.put("hasActiveAgent", true);
                                node.put("agentDiscoveryKey", agent.getId());
                                nodeAgentFound = true;
                                break;
                            }
                        }
                    }
                }
                
                if (!nodeAgentFound) {
                    node.put("hasActiveAgent", false);
                }
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    void enrichTestConnectionNodesFromDiscovery(Map<String, Object> inspection) {
        Object rawNodes = inspection.get("brokers");
        if (!(rawNodes instanceof List<?> nodes)) {
            return;
        }

        String kafkaClusterId = blankToDefault(
                String.valueOf(inspection.getOrDefault("clusterId", inspection.getOrDefault("kafka_cluster_id", ""))),
                "");
        if (kafkaClusterId.isBlank()) {
            return;
        }

        Map<Integer, Map<String, Object>> nodesById = new HashMap<>();
        for (Object rawNode : nodes) {
            if (rawNode instanceof Map<?, ?> rawMap) {
                Map<String, Object> node = (Map<String, Object>) rawMap;
                int nodeId = intValue(node.get("id"), intValue(node.get("broker_id"), -1));
                if (nodeId >= 0) {
                    nodesById.put(nodeId, node);
                }
            }
        }

        externalClusterRepository.findByKafkaClusterId(kafkaClusterId).ifPresent(cluster ->
                externalClusterNodeRepository.findByClusterId(cluster.getId()).forEach(savedNode -> {
                    if (savedNode.getNodeId() == null) {
                        return;
                    }
                    Map<String, Object> node = nodesById.get(savedNode.getNodeId());
                    if (node == null) {
                        return;
                    }
                    if (savedNode.getHost() != null && !savedNode.getHost().isBlank()
                            && !"unknown".equalsIgnoreCase(savedNode.getHost())) {
                        node.put("host", savedNode.getHost());
                    }
                    if (savedNode.getPort() != null && savedNode.getPort() > 0) {
                        node.put("port", savedNode.getPort());
                    }
                    if (savedNode.getIsBroker() != null) {
                        node.put("isBroker", savedNode.getIsBroker());
                    }
                    if (savedNode.getIsController() != null) {
                        node.put("isController", savedNode.getIsController());
                    }
                    node.put("endpoint", node.get("host") + ":" + node.get("port"));
                }));

        OffsetDateTime now = OffsetDateTime.now();
        for (Map.Entry<String, ExternalDiscoveryReport> entry : pendingDiscoveries.entrySet()) {
            ExternalDiscoveryReport report = entry.getValue();
            if (report.getNodeId() == null
                    || !kafkaClusterId.equals(report.getKafkaClusterId())) {
                continue;
            }

            Map<String, Object> node = nodesById.get(report.getNodeId());
            if (node == null) {
                continue;
            }

            boolean controller = roleContains(report.getProcessRoles(), "controller");
            boolean broker = roleContains(report.getProcessRoles(), "broker");
            if (controller || broker) {
                node.put("isController", controller);
                node.put("isBroker", broker);
            }

            String endpoint = discoveryListenerEndpoint(report, controller && !broker);
            String discoveredHost = endpointHost(endpoint);
            int discoveredPort = intValue(endpointPort(endpoint), 0);
            if (!discoveredHost.isBlank()) {
                node.put("host", discoveredHost);
            } else if (report.getHostname() != null && !report.getHostname().isBlank()) {
                node.put("host", report.getHostname());
            }
            if (discoveredPort > 0) {
                node.put("port", discoveredPort);
            }
            node.put("endpoint", node.get("host") + ":" + node.get("port"));

            if (report.isRunning() && isFreshDiscoveryReport(report, now)) {
                node.put("hasActiveAgent", true);
                node.put("agentDiscoveryKey", entry.getKey());
            }
        }
    }

    private String discoveryListenerEndpoint(ExternalDiscoveryReport report, boolean controllerOnly) {
        String listeners = blankToDefault(report.getAdvertisedListeners(), report.getListeners());
        String fallback = "";
        for (String listener : listeners.split(",")) {
            String candidate = listener.trim();
            if (candidate.isBlank()) {
                continue;
            }
            String listenerName = candidate.contains("://")
                    ? candidate.substring(0, candidate.indexOf("://"))
                    : "";
            String endpoint = candidate.contains("://")
                    ? candidate.substring(candidate.indexOf("://") + 3)
                    : candidate;
            if (fallback.isBlank()) {
                fallback = endpoint;
            }
            boolean controllerListener = "CONTROLLER".equalsIgnoreCase(listenerName);
            if (controllerOnly == controllerListener) {
                return endpoint;
            }
        }
        return fallback;
    }

    private boolean isFreshDiscoveryReport(ExternalDiscoveryReport report, OffsetDateTime now) {
        try {
            return report.getLastSeen() != null
                    && OffsetDateTime.parse(report.getLastSeen()).isAfter(now.minusSeconds(agentStaleSeconds()));
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean roleContains(String roles, String expectedRole) {
        if (roles == null || roles.isBlank()) {
            return false;
        }
        return java.util.Arrays.stream(roles.split(","))
                .map(String::trim)
                .anyMatch(expectedRole::equalsIgnoreCase);
    }

    @Transactional
    public ExternalCluster registerBootstrapCluster(BootstrapExternalClusterRequest request) {
        if (request.getBootstrapServers() == null || request.getBootstrapServers().isBlank()) {
            throw new IllegalArgumentException("Bootstrap servers are required.");
        }

        requireAvailableClusterName(request.getName());

        String bootstrap = request.getBootstrapServers().trim();
        Map<String, Object> inspection;
        
        if (request.getClusterId() != null && !request.getClusterId().isBlank()) {
            inspection = new HashMap<>();
            inspection.put("connected", true);
            inspection.put("clusterId", request.getClusterId());
            inspection.put("brokerCount", request.getBrokerCount());
            inspection.put("security_protocol", request.getSecurityProtocol() != null ? request.getSecurityProtocol() : request.getSecurity());
            inspection.put("agentFound", request.getAgentFound());
            inspection.put("discoveryKey", request.getDiscoveryKey());
            inspection.put("kafka_version", request.getKafkaVersion());
            inspection.put("mode", request.getKafkaMode());
            inspection.put("controllerId", request.getControllerId());
            inspection.put("brokers", request.getBrokers());
        } else {
            inspection = testBootstrap(request);
        }
        
        // Ensure Kafka Admin API was able to connect
        if (!Boolean.TRUE.equals(inspection.get("connected"))) {
            throw new IllegalArgumentException(String.valueOf(inspection.getOrDefault("message", "Bootstrap connection failed.")));
        }

        ExternalCluster savedCluster = null;

        // Create the ExternalCluster entity based on AdminClient data (source of truth)
        String clusterId = String.valueOf(inspection.get("clusterId"));
        purgeDeletedExternalClusterRemnants(clusterId, request.getName(), bootstrap);
        savedCluster = findReusableExternalCluster(clusterId, request.getName(), bootstrap).orElseGet(ExternalCluster::new);
        savedCluster.setName(request.getName() != null ? request.getName().trim() : savedCluster.getName());
        savedCluster.setBootstrapServers(mergeBootstrapServers(savedCluster.getBootstrapServers(), bootstrap));
        savedCluster.setKafkaClusterId(clusterId);
        savedCluster.setKafkaVersion(blankToDefault(firstString(inspection, "kafkaVersion", "kafka_version"), "Unknown"));
        savedCluster.setEnvironment(blankToDefault(request.getEnvironment(), "unknown"));
        savedCluster.setKafkaMode(blankToDefault(
                normalizeKafkaMode(firstString(inspection, "mode", "kafkaMode", "kafka_mode")),
                "Unknown"));
        savedCluster.setSecurity(blankToDefault(firstString(inspection, "security_protocol", "security"), "PLAINTEXT"));
        savedCluster.setSecurityProtocol(request.getSecurityProtocol());
        savedCluster.setSaslMechanism(request.getSaslMechanism());
        savedCluster.setSaslUsername(request.getSaslUsername());
        if (request.getSaslPassword() != null && !request.getSaslPassword().isBlank()) {
            savedCluster.setSaslPasswordEncrypted(encryptionService.encrypt(request.getSaslPassword()));
        }
        
        savedCluster.setDisableHostnameVerification(Boolean.TRUE.equals(request.getDisableHostnameVerification()));
        savedCluster.setTruststoreType(request.getTruststoreType());
        if (request.getTruststorePassword() != null && !request.getTruststorePassword().isBlank()) {
            savedCluster.setTruststorePasswordEncrypted(encryptionService.encrypt(request.getTruststorePassword()));
        }
        savedCluster.setKeystoreType(request.getKeystoreType());
        if (request.getKeystorePassword() != null && !request.getKeystorePassword().isBlank()) {
            savedCluster.setKeystorePasswordEncrypted(encryptionService.encrypt(request.getKeystorePassword()));
        }
        if (request.getKeyPassword() != null && !request.getKeyPassword().isBlank()) {
            savedCluster.setKeyPasswordEncrypted(encryptionService.encrypt(request.getKeyPassword()));
        }
        
        if (inspection.get("brokerCount") instanceof Number) {
            savedCluster.setBrokerCount(((Number) inspection.get("brokerCount")).intValue());
        }
        savedCluster.setStatus("SUCCESS");
        savedCluster = externalClusterRepository.save(savedCluster);

        // Save truststore/keystore files if provided
        if (request.getTruststoreBase64() != null && !request.getTruststoreBase64().isBlank()) {
            savedCluster.setTruststoreContentEncrypted(encryptionService.encrypt(normalizeBase64(request.getTruststoreBase64())));
            String path = truststoreStorageService.saveTruststore(savedCluster.getId(), request.getTruststoreType(), request.getTruststoreBase64());
            savedCluster.setTruststorePath(path);
            savedCluster = externalClusterRepository.save(savedCluster);
        }
        if (request.getKeystoreBase64() != null && !request.getKeystoreBase64().isBlank()) {
            savedCluster.setKeystoreContentEncrypted(encryptionService.encrypt(normalizeBase64(request.getKeystoreBase64())));
            String path = truststoreStorageService.saveTruststore(savedCluster.getId(), "keystore_" + request.getKeystoreType(), request.getKeystoreBase64());
            savedCluster.setKeystorePath(path);
            savedCluster = externalClusterRepository.save(savedCluster);
        }

        List<ExternalDiscoveryReport> selectedDiscoveryReports = new ArrayList<>();

        // Process Selected Agents
        if (request.getSelectedAgents() != null && !request.getSelectedAgents().isEmpty()) {
            for (Map.Entry<String, String> entry : request.getSelectedAgents().entrySet()) {
                String agentId = entry.getValue();
                // Check if it's a pending discovery
                if (pendingDiscoveries.containsKey(agentId)) {
                    ExternalDiscoveryReport report = pendingDiscoveries.get(agentId);
                    upsertDiscoveryAgent(report, savedCluster);
                    selectedDiscoveryReports.add(report);
                    pendingDiscoveries.remove(agentId);
                } else {
                    // Update existing db agent to link to this cluster
                    Optional<DiscoveryAgent> optAgent = discoveryAgentRepository.findById(agentId);
                    if (optAgent.isPresent()) {
                        DiscoveryAgent agent = optAgent.get();
                        agent.setClusterId(savedCluster.getId());
                        agent.setStatus("ONLINE");
                        agent.setLastHeartbeat(OffsetDateTime.now());
                        discoveryAgentRepository.save(agent);
                    }
                }
            }
        }

        // A selected discovery report is the source of truth for the Kafka
        // coordination mode. Persist it before building the overview so a
        // ZooKeeper cluster can never inherit an AdminClient fallback.
        for (ExternalDiscoveryReport report : selectedDiscoveryReports) {
            if (applyAuthoritativeDiscoveryMetadata(savedCluster, report)) {
                savedCluster = externalClusterRepository.save(savedCluster);
            }
        }

        normalizeInspectionNodeRolesForMode(inspection, savedCluster.getKafkaMode());

        // Map brokers to externalClusterNodeRepository (full topology)
        List<Map<String, Object>> brokers = (List<Map<String, Object>>) inspection.get("brokers");
        if (brokers != null) {
            for (Map<String, Object> b : brokers) {
                String idStr = String.valueOf(b.get("id"));
                Integer nodeId = null;
                if (b.get("id") != null) {
                    try { nodeId = Integer.parseInt(idStr); } catch (NumberFormatException ignored) {}
                }
                String host = String.valueOf(b.get("host"));
                boolean isController = Boolean.TRUE.equals(b.get("isController"));
                boolean isBroker = Boolean.TRUE.equals(b.get("isBroker"));
                
                Integer port = null;
                if (b.get("port") != null) {
                    try { port = Integer.parseInt(String.valueOf(b.get("port"))); } catch (NumberFormatException ignored) {}
                }
                
                externalClusterNodeRepository.upsertTopology(
                        savedCluster.getId(),
                        host,
                        nodeId,
                        isBroker,
                        isController,
                        port
                );
            }
        }

        ExternalCluster selectedCluster = savedCluster;
        for (ExternalDiscoveryReport report : selectedDiscoveryReports) {
            String agentId = report.getHostId() == null || report.getHostId().isBlank()
                    ? discoveryHostId(report)
                    : report.getHostId();
            discoveryAgentRepository.findById(agentId)
                    .ifPresent(agent -> applyDiscoveryReportToNodes(selectedCluster, report, agent));
        }
        auditService.record(
                "CLUSTER_MANAGEMENT",
                "EXTERNAL_CLUSTER_CONNECTED",
                "CLUSTER",
                savedCluster.getId().toString(),
                savedCluster.getId(),
                "SUCCESS",
                null,
                null,
                null,
                externalAuditDetails(savedCluster)
        );
        return savedCluster;
    }

    @Transactional
    public Map<String, Object> recordDiscoveryReport(ExternalDiscoveryReport report) {
        validateDiscoveryReport(report);
        report.setLastSeen(OffsetDateTime.now().toString());
        upsertDiscoveryAgent(report, null);

        String agentId = report.getHostId() == null || report.getHostId().isBlank() 
                ? discoveryHostId(report) 
                : report.getHostId();
        io.translab.tantor.server.domain.DiscoveryAgent agent = discoveryAgentRepository.findById(agentId).orElse(null);

        Optional<ExternalCluster> connectedCluster = resolveConnectedCluster(report, agent);

        if (connectedCluster.isPresent() && agent != null) {
            ExternalCluster cluster = connectedCluster.get();
            linkDiscoveryAgent(agent, cluster);
            if (applyAuthoritativeDiscoveryMetadata(cluster, report)) {
                cluster = externalClusterRepository.save(cluster);
            }
            applyDiscoveryReportToNodes(cluster, report, agent);
            pendingDiscoveries.remove(discoveryKey(report));
            acceptedDiscoveriesByAgent.put(agentId, OffsetDateTime.now());

            return Map.of(
                    "id", cluster.getId(),
                    "name", cluster.getName(),
                    "status", "registered",
                    "managementLevel", "AGENT_MANAGED"
            );
        }

        String key = discoveryKey(report);
        pendingDiscoveries.put(key, report);
        acceptedDiscoveriesByAgent.put(agentId, OffsetDateTime.now());
        return Map.of(
                "discoveryKey", key,
                "name", report.getName(),
                "status", "pending"
        );
    }

    /**
     * A persisted agent-to-cluster link is authoritative after onboarding.
     * Discovery reports are intentionally node-local, so their name, bootstrap
     * address, or Kafka cluster ID may be absent or differ from the value used
     * when the external cluster was connected. Falling back to report identity
     * is only appropriate for agents that have not been linked yet.
     */
    private Optional<ExternalCluster> resolveConnectedCluster(
            ExternalDiscoveryReport report,
            DiscoveryAgent agent
    ) {
        if (agent != null && agent.getClusterId() != null) {
            Optional<ExternalCluster> linkedCluster = externalClusterRepository.findById(agent.getClusterId());
            if (linkedCluster.isPresent()) {
                return linkedCluster;
            }
        }

        Optional<ExternalCluster> identityMatch = findExternalCluster(
                report.getKafkaClusterId(),
                null,
                report.getBootstrapServers().trim()
        );
        if (identityMatch.isPresent()) {
            return identityMatch;
        }

        Set<String> reportedHosts = discoveryHostCandidates(report, agent);
        if (!reportedHosts.isEmpty()) {
            for (ExternalCluster cluster : externalClusterRepository.findByStatusNot("DELETED")) {
                boolean containsReportedHost = externalClusterNodeRepository.findByClusterId(cluster.getId()).stream()
                        .map(io.translab.tantor.server.domain.ExternalClusterNode::getHost)
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .anyMatch(nodeHost -> reportedHosts.stream()
                                .anyMatch(candidate -> candidate.equalsIgnoreCase(nodeHost)));
                if (containsReportedHost) {
                    return Optional.of(cluster);
                }
            }
        }

        return findExternalCluster(null, report.getName(), null);
    }

    public List<Map<String, Object>> listPendingDiscoveries() {
        return pendingDiscoveries.entrySet().stream()
                .filter(entry -> {
                    ExternalDiscoveryReport report = entry.getValue();
                    String agentId = report.getHostId() == null || report.getHostId().isBlank()
                            ? discoveryHostId(report) : report.getHostId();
                    DiscoveryAgent agent = discoveryAgentRepository.findById(agentId).orElse(null);
                    return resolveConnectedCluster(report, agent).isEmpty();
                })
                .filter(entry -> entry.getValue().isRunning())
                .filter(entry -> isFreshDiscovery(entry.getValue()))
                .sorted(Map.Entry.comparingByValue(Comparator.comparing(
                        ExternalDiscoveryReport::getLastSeen,
                        Comparator.nullsLast(Comparator.reverseOrder())
                )))
                .map(entry -> toDiscoverySummary(entry.getKey(), entry.getValue()))
                .toList();
    }

    @Transactional
    public Map<String, Object> recordDiscoveryAgentHeartbeat(ExternalDiscoveryReport report) {
        report.setLastSeen(OffsetDateTime.now().toString());
        if (report.getHostname() == null || report.getHostname().isBlank()) {
            report.setHostname(extractHostFromBootstrap(report.getBootstrapServers()));
        }
        upsertDiscoveryAgent(report, null);
        return Map.of(
                "status", "online",
                "agentId", report.getHostId() == null || report.getHostId().isBlank() ? discoveryHostId(report) : report.getHostId(),
                "lastHeartbeat", report.getLastSeen()
        );
    }

    public InstallValidation installValidation(String agentId, OffsetDateTime since) {
        OffsetDateTime now = OffsetDateTime.now();
        Optional<DiscoveryAgent> agent = discoveryAgentRepository.findById(agentId);
        boolean heartbeatAccepted = since != null && agent.isPresent()
                && agent.get().getLastHeartbeat() != null
                && agent.get().getLastHeartbeat().isAfter(since)
                && agent.get().getLastHeartbeat().isAfter(now.minusSeconds(agentStaleSeconds()));
        OffsetDateTime discoveryAcceptedAt = acceptedDiscoveriesByAgent.get(agentId);
        boolean discoveryReceived = since != null && discoveryAcceptedAt != null
                && discoveryAcceptedAt.isAfter(since);
        return new InstallValidation(now, agent.isPresent(), heartbeatAccepted, discoveryReceived);
    }

    public record InstallValidation(OffsetDateTime serverTime, boolean registered,
                                    boolean heartbeatAccepted, boolean discoveryReceived) {}

    public List<Map<String, Object>> listDiscoveryAgents() {
        OffsetDateTime now = OffsetDateTime.now();
        return discoveryAgentRepository.findAll().stream()
                .sorted(Comparator.comparing(
                        DiscoveryAgent::getLastHeartbeat,
                        Comparator.nullsLast(Comparator.reverseOrder())
                ))
                .map(agent -> {
                    boolean fresh = agent.getLastHeartbeat() != null
                            && agent.getLastHeartbeat().isAfter(now.minusSeconds(agentStaleSeconds()));
                    Map<String, Object> summary = new LinkedHashMap<>();
                    summary.put("id", agent.getId());
                    summary.put("agentName", blankToDefault(agent.getAgentName(), agent.getId()));
                    summary.put("hostname", blankToDefault(agent.getHostname(), ""));
                    summary.put("ipAddresses", blankToDefault(agent.getIpAddresses(), "[]"));
                    summary.put("version", blankToDefault(agent.getVersion(), "tantor-discovery-agent"));
                    summary.put("canExecuteTasks", Boolean.TRUE.equals(agent.getCanExecuteTasks()));
                    summary.put("clusterId", agent.getClusterId());
                    summary.put("lastHeartbeat", agent.getLastHeartbeat());
                    summary.put("fresh", fresh);
                    summary.put("status", fresh ? "ONLINE" : "STALE");
                    summary.put("health", fresh ? "green" : "orange");
                    summary.put("stateLabel", agent.getClusterId() == null
                            ? (fresh ? "Online - no cluster connected" : "Agent disconnected - no recent heartbeat")
                            : (fresh ? "Online - cluster connected" : "Agent disconnected - cluster connection needs attention"));
                    return summary;
                })
                .toList();
    }

    public Map<String, Object> inspectDiscovery(String discoveryKey) {
        ExternalDiscoveryReport report = requiredPendingDiscovery(discoveryKey);
        Map<String, Object> inspection = new LinkedHashMap<>();
        
        inspection.put("connected", true);
        inspection.put("success", true);
        inspection.put("status", "CONNECTED");

        pendingDiscoveries.put(discoveryKey, report);
        inspection.put("discoveryKey", discoveryKey);
        inspection.put("name", report.getName());
        inspection.put("hostname", report.getHostname());
        inspection.put("bootstrapServers", report.getBootstrapServers());
        inspection.put("bootstrap_servers", report.getBootstrapServers());
        inspection.put("kafkaVersion", blankToDefault(report.getKafkaVersion(), "Unknown"));
        inspection.put("kafka_version", blankToDefault(report.getKafkaVersion(), "Unknown"));
        inspection.put("kafkaMode", blankToDefault(report.getKafkaMode(), "Unknown"));
        inspection.put("mode", blankToDefault(report.getKafkaMode(), "Unknown"));
        inspection.put("kafkaClusterId", blankToDefault(report.getKafkaClusterId(), ""));
        inspection.put("kafka_cluster_id", blankToDefault(report.getKafkaClusterId(), ""));
        inspection.put("security", blankToDefault(report.getSecurity(), "PLAINTEXT"));
        inspection.put("environment", blankToDefault(report.getEnvironment(), "unknown"));
        inspection.put("installPath", blankToDefault(report.getInstallPath(), ""));
        inspection.put("logDirs", blankToDefault(report.getLogDirs(), ""));
        inspection.put("nodeId", report.getNodeId());
        inspection.put("lastSeen", report.getLastSeen());
        inspection.put("agentType", "KAFKA_DISCOVERY");
        return inspection;
    }

    @Transactional
    public ExternalCluster connectDiscovery(String discoveryKey) {
        Map<String, Object> inspection = inspectDiscovery(discoveryKey);
        if (!Boolean.TRUE.equals(inspection.get("connected"))) {
            throw new IllegalArgumentException(String.valueOf(inspection.getOrDefault(
                    "message",
                    "The discovered Kafka bootstrap server is not reachable."
            )));
        }

        ExternalDiscoveryReport report = requiredPendingDiscovery(discoveryKey);
        ExternalCluster cluster = upsertDiscoveryCluster(report);
        String agentId = report.getHostId() == null || report.getHostId().isBlank()
                ? discoveryHostId(report)
                : report.getHostId();
        discoveryAgentRepository.findById(agentId).ifPresent(agent -> {
            linkDiscoveryAgent(agent, cluster);
            applyDiscoveryReportToNodes(cluster, report, agent);
        });
        pendingDiscoveries.remove(discoveryKey);
        return cluster;
    }

    @Transactional
    public Optional<ExternalCluster> deleteExternalCluster(UUID id) {
        Optional<ExternalCluster> clusterOpt = externalClusterRepository.findById(id);
        if (clusterOpt.isEmpty()) {
            return Optional.empty();
        }

        ExternalCluster cluster = clusterOpt.get();

        List<DiscoveryAgent> linkedAgents = discoveryAgentRepository.findByClusterId(id);
        for (DiscoveryAgent agent : linkedAgents) {
            agent.setClusterId(null);
        }
        discoveryAgentRepository.saveAll(linkedAgents);
        externalClusterNodeRepository.deleteByClusterId(id);
        pendingDiscoveries.entrySet().removeIf(entry -> matchesExternalCluster(entry.getValue(), cluster));

        // Remove both the inventory mirror and the external source row. Audit rows
        // intentionally remain independent and retain the deleted cluster details.
        clusterRepository.purgeById(id);
        externalClusterRepository.delete(cluster);
        externalClusterRepository.flush();

        return Optional.of(cluster);
    }

    @Transactional
    public ExternalCluster upsertDiscoveryCluster(ExternalDiscoveryReport report) {
        validateDiscoveryReport(report);

        String bootstrap = report.getBootstrapServers().trim();
        ExternalCluster cluster = findExternalCluster(report.getKafkaClusterId(), report.getName(), bootstrap)
                .orElseGet(ExternalCluster::new);

        return saveDiscoveryCluster(report, bootstrap, cluster);
    }

    private void validateDiscoveryReport(ExternalDiscoveryReport report) {
        if (report.getName() == null || report.getName().isBlank()) {
            throw new IllegalArgumentException("Discovered cluster name is required.");
        }
        if (report.getBootstrapServers() == null || report.getBootstrapServers().isBlank()) {
            throw new IllegalArgumentException("Discovered bootstrap servers are required.");
        }
    }

    private ExternalCluster saveDiscoveryCluster(ExternalDiscoveryReport report, String bootstrap, ExternalCluster cluster) {
        boolean isNew = cluster.getId() == null;
        cluster.setName(isNew ? report.getName().trim() : cluster.getName());
        String mergedBootstrapServers = mergeBootstrapServers(cluster.getBootstrapServers(), bootstrap);
        // Discovery agents may send partial follow-up reports. Never erase the
        // Kafka-assigned cluster ID that was captured during registration.
        if (report.getKafkaClusterId() != null && !report.getKafkaClusterId().isBlank()) {
            cluster.setKafkaClusterId(report.getKafkaClusterId().trim());
        }
        cluster.setInstallPath(blankToDefault(report.getInstallPath(), null));
        cluster.setLogDirs(blankToDefault(report.getLogDirs(), null));
        cluster.setKafkaVersion(blankToDefault(report.getKafkaVersion(), "Unknown"));
        cluster.setEnvironment(blankToDefault(report.getEnvironment(), "unknown"));
        cluster.setBootstrapServers(mergedBootstrapServers);
        cluster.setStatus(report.isRunning() ? "SUCCESS" : "DEGRADED");
        String discoveredMode = normalizeKafkaMode(report.getKafkaMode());
        if (discoveredMode != null) {
            cluster.setKafkaMode(discoveredMode);
        } else if (cluster.getKafkaMode() == null || cluster.getKafkaMode().isBlank()) {
            cluster.setKafkaMode("Unknown");
        }
        cluster.setSecurity(blankToDefault(report.getSecurity(), "PLAINTEXT"));
        cluster.setBrokerCount(report.getBrokerCount());
        cluster.setListeners(report.getListeners());
        cluster.setAdvertisedListeners(report.getAdvertisedListeners());
        cluster.setProcessRoles(report.getProcessRoles());
        cluster.setCpuUsagePct(report.getCpuUsagePct());
        cluster.setMemoryUsedMb(report.getMemoryUsedMb());
        cluster.setMemoryTotalMb(report.getMemoryTotalMb());
        cluster.setDiskUsedGb(report.getDiskUsedGb());
        cluster.setDiskTotalGb(report.getDiskTotalGb());
        cluster.setIsRunning(report.isRunning());

        ExternalCluster saved = externalClusterRepository.save(cluster);
        
        report.setLastSeen(OffsetDateTime.now().toString());

        if (isNew) {
            activityAlertService.logActivity("INFO", "Discovered external cluster via agent: " + saved.getName(), saved.getId());
            
            auditService.record(
                "CLUSTER_MANAGEMENT",
                "EXTERNAL_CLUSTER_CONNECTED",
                "CLUSTER",
                saved.getId().toString(),
                saved.getId(),
                "SUCCESS",
                null,
                null,
                null,
                externalAuditDetails(saved)
            );
        }
        
        return saved;
    }

    /**
     * Applies metadata that can only be determined reliably from the running
     * node configuration. In particular, Kafka AdminClient cannot distinguish
     * ZooKeeper from KRaft merely from the elected controller returned by
     * describeCluster().
     */
    private boolean applyAuthoritativeDiscoveryMetadata(
            ExternalCluster cluster,
            ExternalDiscoveryReport report) {
        boolean changed = false;
        String discoveredMode = normalizeKafkaMode(report.getKafkaMode());
        if (discoveredMode != null && !Objects.equals(cluster.getKafkaMode(), discoveredMode)) {
            cluster.setKafkaMode(discoveredMode);
            changed = true;
        }

        String discoveredVersion = cleanReportedValue(report.getKafkaVersion());
        if (discoveredVersion != null && !Objects.equals(cluster.getKafkaVersion(), discoveredVersion)) {
            cluster.setKafkaVersion(discoveredVersion);
            changed = true;
        }

        String discoveredRoles = cleanReportedValue(report.getProcessRoles());
        if ("ZooKeeper".equals(discoveredMode)) {
            // ZooKeeper-backed brokers do not use process.roles. Clear a stale
            // KRaft value if this cluster was previously misclassified.
            discoveredRoles = null;
        }
        if ((discoveredRoles != null || "ZooKeeper".equals(discoveredMode))
                && !Objects.equals(cluster.getProcessRoles(), discoveredRoles)) {
            cluster.setProcessRoles(discoveredRoles);
            changed = true;
        }
        return changed;
    }

    static String normalizeKafkaMode(String value) {
        String cleaned = cleanReportedValue(value);
        if (cleaned == null) {
            return null;
        }
        if ("zookeeper".equalsIgnoreCase(cleaned) || "zk".equalsIgnoreCase(cleaned)) {
            return "ZooKeeper";
        }
        if ("kraft".equalsIgnoreCase(cleaned)) {
            return "KRaft";
        }
        return null;
    }

    private static String cleanReportedValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String cleaned = value.trim();
        if ("null".equalsIgnoreCase(cleaned)
                || "unknown".equalsIgnoreCase(cleaned)
                || "auto-detected".equalsIgnoreCase(cleaned)
                || "auto-detected by Kafka client".equalsIgnoreCase(cleaned)) {
            return null;
        }
        return cleaned;
    }



    @Transactional
    public void receiveMetrics(String clusterName, ExternalBrokerMetricsDto metrics) {
        Optional<ExternalCluster> clusterOpt = findExternalClusterForMetrics(clusterName, metrics);
        if (clusterOpt.isEmpty()) {
            return;
        }

        ExternalCluster cluster = clusterOpt.get();
        List<ExternalBrokerRecord> brokers = readBrokerRecords(cluster);
        String bootstrap = blankToDefault(metrics.getBootstrap(), cluster.getBootstrapServers());
        ExternalBrokerRecord broker = brokers.stream()
                .filter(item -> metrics.getNodeId() != null
                        ? metrics.getNodeId().equals(item.getNodeId())
                        : safeEquals(item.getHostname(), metrics.getHostname())
                                || (item.getHostname() != null && bootstrap != null && bootstrap.contains(item.getHostname())))
                .findFirst()
                .orElseGet(() -> {
                    ExternalBrokerRecord item = new ExternalBrokerRecord();
                    item.setHostname(metrics.getHostname());
                    item.setBootstrap(bootstrap);
                    item.setRole("broker");
                    brokers.add(item);
                    return item;
                });

        broker.setCpuUsagePct(metrics.getCpuUsagePct());
        broker.setMemoryUsedMb(metrics.getMemoryUsedMb());
        broker.setMemoryTotalMb(metrics.getMemoryTotalMb());
        broker.setDiskUsedGb(metrics.getDiskUsedGb());
        broker.setDiskTotalGb(metrics.getDiskTotalGb());
        broker.setDiskUsedBytes(resolveDiskBytes(metrics.getDiskUsedBytes(), metrics.getDiskUsedGb()));
        broker.setDiskTotalBytes(resolveDiskBytes(metrics.getDiskTotalBytes(), metrics.getDiskTotalGb()));
        broker.setMessagesInPerSec(metrics.getMessagesInPerSec());
        broker.setBytesInPerSec(metrics.getBytesInPerSec());
        broker.setLastSeen(OffsetDateTime.now().toString());
        broker.setLastSeen(OffsetDateTime.now().toString());

        OffsetDateTime seen = OffsetDateTime.now();
        Optional<io.translab.tantor.server.domain.ExternalClusterNode> targetNode = metrics.getNodeId() == null
                ? Optional.empty()
                : externalClusterNodeRepository.findByClusterIdAndNodeId(cluster.getId(), metrics.getNodeId());
        if (targetNode.isPresent()) {
            updateNodeTelemetry(targetNode.get(), metrics, seen);
            externalClusterNodeRepository.save(targetNode.get());
        } else {
            externalClusterNodeRepository.upsertTelemetry(
                    cluster.getId(),
                    broker.getHostname(),
                    broker.getCpuUsagePct(),
                    broker.getMemoryUsedMb(),
                    broker.getMemoryTotalMb(),
                    broker.getDiskUsedGb(),
                    broker.getDiskTotalGb(),
                    broker.getDiskUsedBytes(),
                    broker.getDiskTotalBytes(),
                    seen
            );
            externalClusterNodeRepository.updateBrokerIngestionRates(
                    cluster.getId(),
                    broker.getHostname(),
                    metrics.getMessagesInPerSec(),
                    metrics.getBytesInPerSec()
            );
        }

        discoveryAgentRepository.findByHostname(broker.getHostname()).ifPresent(agent -> {
            agent.setStatus("ONLINE");
            agent.setLastHeartbeat(OffsetDateTime.now());
            discoveryAgentRepository.save(agent);
        });
    }

    private Optional<ExternalCluster> findExternalClusterForMetrics(
            String clusterName,
            ExternalBrokerMetricsDto metrics
    ) {
        Optional<DiscoveryAgent> linkedAgent = Optional.empty();
        if (metrics.getHostId() != null && !metrics.getHostId().isBlank()) {
            linkedAgent = discoveryAgentRepository.findById(metrics.getHostId().trim());
        }
        if (linkedAgent.isEmpty() && metrics.getHostname() != null && !metrics.getHostname().isBlank()) {
            linkedAgent = discoveryAgentRepository.findByHostname(metrics.getHostname().trim());
        }
        if (linkedAgent.isPresent() && linkedAgent.get().getClusterId() != null) {
            Optional<ExternalCluster> linkedCluster = externalClusterRepository.findById(linkedAgent.get().getClusterId());
            if (linkedCluster.isPresent() && !"DELETED".equalsIgnoreCase(linkedCluster.get().getStatus())) {
                return linkedCluster;
            }
        }
        return findExternalCluster(null, clusterName, metrics.getBootstrap());
    }

    public List<Map<String, Object>> listExternalClusters() {
        return externalClusterRepository.findByStatusNot("DELETED").stream()
                .sorted(Comparator.comparing(ExternalCluster::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .map(this::toSummary)
                .toList();
    }

    public Map<String, Object> queueRestart(UUID clusterId) {
        ExternalCluster cluster = externalClusterRepository.findById(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("External cluster not found."));

        List<ExternalBrokerRecord> brokers = readBrokerRecords(cluster).stream()
                .filter(this::isAgentRecord)
                .toList();
        if (brokers.isEmpty()) {
            throw new IllegalArgumentException("No discovery agent is attached to this external cluster.");
        }

        String taskId = UUID.randomUUID().toString();
        for (ExternalBrokerRecord broker : brokers) {
            ExternalAgentTask task = new ExternalAgentTask();
            task.setTaskId(taskId);
            task.setTask("RESTART");
            task.setStatus("PENDING");
            task.setClusterName(cluster.getName());
            task.setHostname(broker.getHostname());
            task.setBootstrap(broker.getBootstrap());
            pendingTasks.put(taskKey(cluster.getName(), broker.getHostname(), broker.getBootstrap()), task);
        }

        return Map.of("taskId", taskId, "status", "queued", "brokers", String.valueOf(brokers.size()));
    }

    public Map<String, Object> queueConfigUpdate(UUID clusterId, String configKey, String configValue, boolean restart) {
        ExternalCluster cluster = externalClusterRepository.findById(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("External cluster not found."));

        List<ExternalBrokerRecord> brokers = readBrokerRecords(cluster).stream()
                .filter(this::isAgentRecord)
                .toList();
        if (brokers.isEmpty()) {
            throw new IllegalArgumentException("No discovery agent is attached to this external cluster.");
        }

        String taskId = UUID.randomUUID().toString();
        for (ExternalBrokerRecord broker : brokers) {
            ExternalAgentTask task = new ExternalAgentTask();
            task.setTaskId(taskId);
            task.setTask("UPDATE_CONFIG");
            task.setStatus("PENDING");
            task.setClusterName(cluster.getName());
            task.setHostname(broker.getHostname());
            task.setBootstrap(broker.getBootstrap());
            task.setConfigKey(configKey);
            task.setConfigValue(configValue);
            task.setRestart(restart);
            pendingTasks.put(taskKey(cluster.getName(), broker.getHostname(), broker.getBootstrap()), task);
        }

        return Map.of("taskId", taskId, "status", "queued", "brokers", String.valueOf(brokers.size()));
    }

    public Map<String, Object> queueTask(UUID clusterId, String hostname, String taskName, Map<String, Object> payload) {
        ExternalCluster cluster = externalClusterRepository.findById(clusterId)
                .orElseThrow(() -> new RuntimeException("External cluster not found"));
        
        String taskId = UUID.randomUUID().toString();
        ExternalAgentTask task = new ExternalAgentTask();
        task.setTaskId(taskId);
        task.setTask(taskName);
        task.setClusterName(cluster.getName());
        task.setStatus("PENDING");
        task.setHostname(hostname);
        task.setBootstrap(cluster.getBootstrapServers());
        
        if (payload != null) {
            if (payload.containsKey("configFilePath")) task.setConfigFilePath(String.valueOf(payload.get("configFilePath")));
            if (payload.containsKey("backupDirPath")) task.setBackupDirPath(String.valueOf(payload.get("backupDirPath")));
            if (payload.containsKey("backupFilePath")) task.setBackupFilePath(String.valueOf(payload.get("backupFilePath")));
            if (payload.containsKey("serviceName")) task.setServiceName(String.valueOf(payload.get("serviceName")));
            if (payload.containsKey("configChanges")) task.setConfigChanges((Map<String, String>) payload.get("configChanges"));
        }
        
        String key = taskKey(cluster.getName(), hostname, cluster.getBootstrapServers());
        pendingTasks.put(key, task);
        
        return Map.of("taskId", taskId, "status", "queued");
    }

    public Map<String, Object> pollAgentTask(String clusterName, String hostname, String bootstrap) {
        String key = taskKey(clusterName, hostname, bootstrap);
        ExternalAgentTask[] claimed = new ExternalAgentTask[1];
        pendingTasks.compute(key, (unused, candidate) -> {
            if (candidate == null || !"PENDING".equals(candidate.getStatus())) {
                return candidate;
            }
            candidate.setStatus("IN_PROGRESS");
            claimed[0] = candidate;
            return candidate;
        });
        ExternalAgentTask task = claimed[0];
        if (task == null) {
            return Map.of("task", "NONE");
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("task", task.getTask());
        response.put("taskId", task.getTaskId());
        if (task.getConfigKey() != null) {
            response.put("configKey", task.getConfigKey());
        }
        if (task.getConfigValue() != null) {
            response.put("configValue", task.getConfigValue());
        }
        if (task.getConfigFilePath() != null) {
            response.put("configFilePath", task.getConfigFilePath());
        }
        if (task.getBackupDirPath() != null) {
            response.put("backupDirPath", task.getBackupDirPath());
        }
        if (task.getBackupFilePath() != null) {
            response.put("backupFilePath", task.getBackupFilePath());
        }
        if (task.getConfigChanges() != null) {
            response.put("configChanges", task.getConfigChanges());
        }
        if (task.getServiceName() != null) {
            response.put("serviceName", task.getServiceName());
        }
        response.put("restart", String.valueOf(task.isRestart()));
        return response;
    }

    public void completeAgentTask(String clusterName, String hostname, String bootstrap, AgentTaskCompletion completion) {
        ExternalAgentTask task = pendingTasks.get(taskKey(clusterName, hostname, bootstrap));
        if (task == null) {
            return;
        }
        task.setStatus(blankToDefault(completion.getStatus(), "SUCCESS"));
        task.setMessage(completion.getMessage());
        if (completion.getData() != null) {
            task.setData(completion.getData());
        }
        if (!"FAILED".equalsIgnoreCase(task.getStatus())) {
            completedTasks.put(task.getTaskId(), task);
            pendingTasks.remove(taskKey(clusterName, hostname, bootstrap));
        } else {
            completedTasks.put(task.getTaskId(), task);
            pendingTasks.remove(taskKey(clusterName, hostname, bootstrap));
        }
    }

    public Map<String, Object> getExternalTaskStatus(String taskId) {
        Map<String, Object> result = new HashMap<>();
        
        ExternalAgentTask completed = completedTasks.get(taskId);
        if (completed != null) {
            result.put("taskId", taskId);
            result.put("status", completed.getStatus());
            if (completed.getMessage() != null) {
                result.put("message", completed.getMessage());
            }
            if (completed.getData() != null) {
                result.put("data", completed.getData());
            }
            completedTasks.remove(taskId);
            return result;
        }

        boolean anyPending = false;
        boolean anyFailed = false;
        List<String> messages = new ArrayList<>();
        for (ExternalAgentTask task : pendingTasks.values()) {
            if (!safeEquals(task.getTaskId(), taskId)) {
                continue;
            }
            if ("FAILED".equalsIgnoreCase(task.getStatus())) {
                anyFailed = true;
            }
            if (!"SUCCESS".equalsIgnoreCase(task.getStatus())) {
                anyPending = true;
            }
            if (task.getMessage() != null && !task.getMessage().isBlank()) {
                messages.add(task.getMessage());
            }
        }
        
        result.put("taskId", taskId);
        if (anyFailed) {
            result.put("status", "FAILED");
            result.put("message", String.join("; ", messages));
        } else if (anyPending) {
            result.put("status", "IN_PROGRESS");
        } else {
            result.put("status", "SUCCESS");
        }
        return result;
    }

    public boolean isAgentManaged(ExternalCluster cluster) {
        if (cluster == null) {
            return false;
        }
        return discoveryAgentRepository.findByClusterId(cluster.getId()).stream()
                .anyMatch(agent -> "ONLINE".equalsIgnoreCase(agent.getStatus()));
    }

    public List<ExternalBrokerRecord> brokerRecords(ExternalCluster cluster) {
        return readBrokerRecords(cluster);
    }

    private Optional<ExternalCluster> findExternalCluster(String kafkaClusterId, String name, String bootstrapServers) {
        if (kafkaClusterId != null && !kafkaClusterId.isBlank()) {
            for (ExternalCluster cluster : externalClusterRepository.findByStatusNot("DELETED")) {
                if (safeEquals(cluster.getKafkaClusterId(), kafkaClusterId)) {
                    return Optional.of(cluster);
                }
            }
        }
        if (bootstrapServers != null && !bootstrapServers.isBlank()) {
            Optional<ExternalCluster> byBootstrap = externalClusterRepository.findByBootstrapServersAndStatusNot(bootstrapServers.trim(), "DELETED");
            if (byBootstrap.isPresent()) {
                return byBootstrap;
            }
            for (ExternalCluster cluster : externalClusterRepository.findByStatusNot("DELETED")) {
                if (bootstrapServersOverlap(cluster.getBootstrapServers(), bootstrapServers)) {
                    return Optional.of(cluster);
                }
            }
        }
        if (name != null && !name.isBlank()) {
            return externalClusterRepository.findByNameAndStatusNot(name.trim(), "DELETED");
        }
        return Optional.empty();
    }

    private Optional<ExternalCluster> findReusableExternalCluster(String kafkaClusterId, String name, String bootstrapServers) {
        // Deleted clusters are never reused: a reconnect must receive a fresh UUID
        // and fresh createdAt timestamp instead of reviving historical state.
        return findExternalCluster(kafkaClusterId, name, bootstrapServers);
    }

    private void purgeDeletedExternalClusterRemnants(String kafkaClusterId, String name, String bootstrapServers) {
        List<UUID> staleIds = externalClusterRepository.findByStatus("DELETED").stream()
                .filter(cluster -> safeEquals(cluster.getKafkaClusterId(), kafkaClusterId)
                        || safeEquals(cluster.getName(), name)
                        || bootstrapServersOverlap(cluster.getBootstrapServers(), bootstrapServers))
                .map(ExternalCluster::getId)
                .filter(id -> id != null)
                .toList();
        staleIds.forEach(this::deleteExternalCluster);
    }

    private Map<String, Object> toSummary(ExternalCluster cluster) {
        List<ExternalBrokerRecord> brokers = readBrokerRecords(cluster);
        List<DiscoveryAgent> agents = discoveryAgentRepository.findByClusterId(cluster.getId());
        long agentCount = agents.stream().filter(agent -> "ONLINE".equalsIgnoreCase(agent.getStatus())).count();
        long freshAgents = agents.stream().filter(this::isFreshAgent).count();
        int brokerCount = cluster.getBrokerCount() != null ? cluster.getBrokerCount() : (brokers.isEmpty() ? 0 : brokers.size());

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("id", cluster.getId());
        summary.put("name", cluster.getName());
        summary.put("clusterId", blankToDefault(cluster.getKafkaClusterId(), ""));
        summary.put("kafkaVersion", cluster.getKafkaVersion());
        summary.put("kafkaMode", blankToDefault(cluster.getKafkaMode(), "Unknown"));
        summary.put("security", blankToDefault(cluster.getSecurity(), "PLAINTEXT"));
        summary.put("bootstrapServers", cluster.getBootstrapServers());
        summary.put("environment", cluster.getEnvironment());
        summary.put("brokerCount", brokerCount);
        summary.put("agentCount", agentCount);
        summary.put("managementLevel", agentCount > 0 ? "AGENT_MANAGED" : "BOOTSTRAP_ONLY");
        summary.put("managementLabel", agentCount > 0 ? "Agent managed" : "Bootstrap only");
        summary.put("health", agentCount > 0
                ? (freshAgents == agentCount ? "Agent online" : "Agent stale")
                : "Bootstrap registered");
        summary.put("createdAt", cluster.getCreatedAt());
        String brokerLastSeen = brokers.stream()
                .map(ExternalBrokerRecord::getLastSeen)
                .filter(value -> value != null && !value.isBlank())
                .max(String::compareTo)
                .orElse("");
        String agentLastSeen = agents.stream()
                .map(DiscoveryAgent::getLastHeartbeat)
                .filter(value -> value != null)
                .map(OffsetDateTime::toString)
                .max(String::compareTo)
                .orElse("");
        summary.put("lastSeen", brokerLastSeen.isBlank() ? agentLastSeen : brokerLastSeen);
        summary.put("installPath", blankToDefault(cluster.getInstallPath(), ""));
        summary.put("logDirs", blankToDefault(cluster.getLogDirs(), ""));
        return summary;
    }

    private Map<String, Object> toDiscoverySummary(String discoveryKey, ExternalDiscoveryReport report) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("discoveryKey", discoveryKey);
        summary.put("name", report.getName());
        summary.put("hostname", report.getHostname());
        summary.put("bootstrapServers", report.getBootstrapServers());
        summary.put("kafkaVersion", blankToDefault(report.getKafkaVersion(), "Unknown"));
        summary.put("kafkaMode", blankToDefault(report.getKafkaMode(), "Unknown"));
        summary.put("security", blankToDefault(report.getSecurity(), "PLAINTEXT"));
        summary.put("brokerCount", report.getBrokerCount());
        summary.put("nodeId", report.getNodeId());
        summary.put("environment", blankToDefault(report.getEnvironment(), "unknown"));
        summary.put("installPath", blankToDefault(report.getInstallPath(), ""));
        summary.put("logDirs", blankToDefault(report.getLogDirs(), ""));
        summary.put("running", report.isRunning());
        summary.put("health", report.isRunning() ? "Agent online" : "Agent reported stopped");
        summary.put("lastSeen", report.getLastSeen());
        summary.put("kafkaClusterId", blankToDefault(report.getKafkaClusterId(), ""));
        return summary;
    }

    private void upsertBrokerRecord(ExternalCluster cluster, ExternalBrokerRecord record) {
        OffsetDateTime lastSeen = null;
        try {
            if (record.getLastSeen() != null && !record.getLastSeen().isBlank()) {
                lastSeen = OffsetDateTime.parse(record.getLastSeen());
            }
        } catch (Exception e) {
            lastSeen = OffsetDateTime.now();
        }
        
        externalClusterNodeRepository.upsertTelemetry(
                cluster.getId(),
                record.getHostname(),
                record.getCpuUsagePct(),
                record.getMemoryUsedMb(),
                record.getMemoryTotalMb(),
                record.getDiskUsedGb(),
                record.getDiskTotalGb(),
                record.getDiskUsedBytes(),
                record.getDiskTotalBytes(),
                lastSeen != null ? lastSeen : OffsetDateTime.now()
        );
    }

    private void upsertDiscoveryAgent(ExternalDiscoveryReport report, ExternalCluster cluster) {
        String agentId = report.getHostId() == null ? "" : report.getHostId().trim();
        if (agentId.isEmpty()) {
            throw new IllegalArgumentException("A stable host-id is required for discovery agents.");
        }
        String nodeName = blankToDefault(report.getHostname(), extractHostFromBootstrap(report.getBootstrapServers())).trim();
        if (nodeName.isEmpty()) {
            throw new IllegalArgumentException("A node-name is required for discovery agents.");
        }
        if (discoveryAgentRepository.existsNodeNameOwnedByAnotherHost(nodeName.toLowerCase(Locale.ROOT), agentId)) {
            throw new NodeNameConflictException(nodeName);
        }
        report.setHostId(agentId);
        report.setHostname(nodeName);
                
        io.translab.tantor.server.domain.DiscoveryAgent agent = discoveryAgentRepository.findById(agentId)
                .orElseGet(io.translab.tantor.server.domain.DiscoveryAgent::new);
                
        agent.setId(agentId);
        agent.setAgentName(report.getAgentName());
        agent.setHostname(nodeName);
        agent.setIpAddresses(blankToDefault(report.getIpAddresses(), writeJson(List.of(extractHostFromBootstrap(report.getBootstrapServers())))));
        agent.setVersion(blankToDefault(report.getAgentVersion(), "tantor-discovery-agent"));
        agent.setCanDeployServices(report.isCanDeployServices());
        agent.setStatus(report.isRunning() ? "ONLINE" : "OFFLINE");
        agent.setCanExecuteTasks(report.isCanExecuteTasks());
        agent.setLastHeartbeat(OffsetDateTime.now());
        if (cluster != null) {
            agent.setClusterId(cluster.getId());
        }
        discoveryAgentRepository.save(agent);
    }

    private void linkDiscoveryAgent(DiscoveryAgent agent, ExternalCluster cluster) {
        if (agent == null || cluster == null) {
            return;
        }
        agent.setClusterId(cluster.getId());
        agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());
        discoveryAgentRepository.save(agent);
    }

    private int nextNodeId(UUID clusterId) {
        return clusterServiceAssignmentRepository.findByClusterId(clusterId).stream()
                .map(ClusterServiceAssignment::getNodeId)
                .filter(value -> value != null && value > 0)
                .max(Integer::compareTo)
                .orElse(0) + 1;
    }

    private ExternalBrokerRecord buildAgentBrokerRecord(ExternalDiscoveryReport report) {
        ExternalBrokerRecord record = new ExternalBrokerRecord();
        record.setHostname(report.getHostname());
        record.setBootstrap(report.getBootstrapServers());
        record.setKafkaMode(report.getKafkaMode());
        record.setSecurity(report.getSecurity());
        record.setInstallPath(report.getInstallPath());
        record.setLogDirs(report.getLogDirs());
        record.setRunning(report.isRunning());
        record.setRole(report.getKafkaMode() != null && report.getKafkaMode().equalsIgnoreCase("zookeeper") ? "broker" : "broker_controller");
        record.setNodeId(report.getNodeId());
        record.setLastSeen(report.getLastSeen());
        record.setListeners(report.getListeners());
        record.setAdvertisedListeners(report.getAdvertisedListeners());
        record.setProcessRoles(report.getProcessRoles());
        
        // Map Telemetry
        record.setCpuUsagePct(report.getCpuUsagePct());
        record.setMemoryUsedMb(report.getMemoryUsedMb());
        record.setMemoryTotalMb(report.getMemoryTotalMb());
        record.setDiskUsedGb(report.getDiskUsedGb());
        record.setDiskTotalGb(report.getDiskTotalGb());
        
        return record;
    }

    @SuppressWarnings("unchecked")
    private List<ExternalBrokerRecord> buildBootstrapBrokerRecords(Map<String, Object> inspection) {
        List<ExternalBrokerRecord> records = new ArrayList<>();
        Object brokersObj = inspection.get("brokers");
        if (brokersObj instanceof List<?> brokers) {
            for (Object item : brokers) {
                if (!(item instanceof Map<?, ?> map)) {
                    continue;
                }
                Object host = map.get("host");
                Object endpoint = map.get("endpoint");
                ExternalBrokerRecord record = new ExternalBrokerRecord();
                record.setHostname(host == null ? "" : String.valueOf(host));
                record.setBootstrap(endpoint == null ? "" : String.valueOf(endpoint));
                record.setNodeId(intValue(map.get("id"), 0));
                record.setRole("broker");
                record.setLastSeen(OffsetDateTime.now().toString());
                records.add(record);
            }
        }
        return records;
    }

    private Map<String, Object> metadata(String managementMode, String kafkaMode, String security, Map<String, Object> inspection, ExternalDiscoveryReport report) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("managementMode", managementMode);
        metadata.put("kafkaMode", blankToDefault(kafkaMode, "Unknown"));
        metadata.put("security", blankToDefault(security, "PLAINTEXT"));
        metadata.put("lastDiscoveryAt", OffsetDateTime.now().toString());
        if (inspection != null) {
            metadata.put("kafkaClusterId", inspection.getOrDefault("clusterId", ""));
            metadata.put("brokerCount", inspection.getOrDefault("brokerCount", 0));
            metadata.put("topicCount", inspection.getOrDefault("topicCount", 0));
            metadata.put("controllerId", inspection.getOrDefault("controllerId", ""));
        }
        if (report != null) {
            metadata.put("kafkaClusterId", blankToDefault(report.getKafkaClusterId(), ""));
            metadata.put("brokerCount", report.getBrokerCount());
            metadata.put("installPath", blankToDefault(report.getInstallPath(), ""));
            metadata.put("logDirs", blankToDefault(report.getLogDirs(), ""));
            metadata.put("isRunning", report.isRunning());
        }
        return metadata;
    }

    private String resolveClusterName(String requestedName, Map<String, Object> inspection) {
        if (requestedName != null && !requestedName.isBlank()) {
            return requestedName.trim();
        }
        String clusterId = String.valueOf(inspection.getOrDefault("clusterId", "external"));
        String suffix = clusterId.length() > 8 ? clusterId.substring(0, 8) : clusterId;
        return "external-" + suffix;
    }

    private List<ExternalBrokerRecord> readBrokerRecords(ExternalCluster cluster) {
        List<io.translab.tantor.server.domain.ExternalClusterNode> nodes = externalClusterNodeRepository.findByClusterId(cluster.getId());
        List<DiscoveryAgent> agents = discoveryAgentRepository.findByClusterId(cluster.getId());
        List<ExternalBrokerRecord> records = new ArrayList<>();
        boolean zookeeperMode = "ZooKeeper".equalsIgnoreCase(normalizeKafkaMode(cluster.getKafkaMode()));
        for (io.translab.tantor.server.domain.ExternalClusterNode n : nodes) {
            if (zookeeperMode && !Boolean.TRUE.equals(n.getIsBroker())) {
                continue;
            }
            ExternalBrokerRecord r = new ExternalBrokerRecord();
            r.setHostname(n.getHost());
            r.setBootstrap(cluster.getBootstrapServers());
            boolean isBroker = zookeeperMode || Boolean.TRUE.equals(n.getIsBroker());
            boolean isController = !zookeeperMode && Boolean.TRUE.equals(n.getIsController());
            if (isBroker && isController) r.setRole("broker_controller");
            else if (isBroker) r.setRole("broker");
            else if (isController) r.setRole("controller");
            else r.setRole("unknown");
            r.setNodeId(n.getNodeId());
            Optional<DiscoveryAgent> agent = agents.stream()
                    .filter(candidate -> matchesDiscoveryAgent(candidate, n.getHost()))
                    .findFirst();
            OffsetDateTime lastSeen = n.getLastSeen();
            if (lastSeen == null && agent.isPresent()) {
                lastSeen = agent.get().getLastHeartbeat();
            }
            if (lastSeen != null) r.setLastSeen(lastSeen.toString());
            r.setCpuUsagePct(n.getCpuUsagePct());
            r.setMemoryUsedMb(n.getMemoryUsedMb());
            r.setMemoryTotalMb(n.getMemoryTotalMb());
            r.setDiskUsedGb(n.getDiskUsedGb());
            r.setDiskTotalGb(n.getDiskTotalGb());
            r.setDiskUsedBytes(n.getDiskUsedBytes());
            r.setDiskTotalBytes(n.getDiskTotalBytes());
            r.setMessagesInPerSec(n.getMessagesInPerSec());
            r.setBytesInPerSec(n.getBytesInPerSec());
            r.setInstallPath(blankToDefault(n.getInstallDir(), cluster.getInstallPath()));
            r.setLogDirs(blankToDefault(n.getLogDirs(), cluster.getLogDirs()));
            r.setRunning(lastSeen != null && lastSeen.isAfter(OffsetDateTime.now().minusSeconds(agentStaleSeconds())));
            records.add(r);
        }
        return records;
    }

    private void applyDiscoveryReportToNodes(ExternalCluster cluster, ExternalDiscoveryReport report, DiscoveryAgent agent) {
        List<io.translab.tantor.server.domain.ExternalClusterNode> nodes = externalClusterNodeRepository.findByClusterId(cluster.getId());
        OffsetDateTime seen = OffsetDateTime.now();
        boolean matched = false;
        for (io.translab.tantor.server.domain.ExternalClusterNode node : nodes) {
            if (!matchesDiscoveryNode(node, report, agent)) {
                continue;
            }
            matched = true;
            enrichDiscoveryNodeIdentity(node, report);
            node.setCpuUsagePct(report.getCpuUsagePct());
            node.setMemoryUsedMb(report.getMemoryUsedMb());
            node.setMemoryTotalMb(report.getMemoryTotalMb());
            node.setDiskUsedGb(report.getDiskUsedGb());
            node.setDiskTotalGb(report.getDiskTotalGb());
            node.setDiskUsedBytes(resolveDiskBytes(report.getDiskUsedBytes(), report.getDiskUsedGb()));
            node.setDiskTotalBytes(resolveDiskBytes(report.getDiskTotalBytes(), report.getDiskTotalGb()));
            applyReportedJmxExporterPort(node, report.getJmxExporterPort());
            node.setLastSeen(seen);
            node.setInstallDir(blankToDefault(report.getInstallPath(), node.getInstallDir()));
            node.setLogDirs(blankToDefault(report.getLogDirs(), node.getLogDirs()));
            node.setConfigFile(blankToDefault(report.getConfigFile(), node.getConfigFile()));
            node.setDataDirs(blankToDefault(report.getDataDirs(), blankToDefault(report.getLogDirs(), node.getDataDirs())));
            externalClusterNodeRepository.save(node);
        }

        if (!matched && report.getNodeId() != null) {
            io.translab.tantor.server.domain.ExternalClusterNode node = new io.translab.tantor.server.domain.ExternalClusterNode();
            node.setClusterId(cluster.getId());
            node.setHost(firstNonBlank(extractHostFromBootstrap(report.getBootstrapServers()), report.getHostname(), agent.getHostname()));
            node.setNodeId(report.getNodeId());
            String roles = blankToDefault(report.getProcessRoles(), "").toLowerCase();
            boolean zookeeperMode = "ZooKeeper".equalsIgnoreCase(normalizeKafkaMode(cluster.getKafkaMode()));
            node.setIsBroker(zookeeperMode || roles.isBlank() || roles.contains("broker"));
            node.setIsController(!zookeeperMode && roles.contains("controller"));
            node.setCpuUsagePct(report.getCpuUsagePct());
            node.setMemoryUsedMb(report.getMemoryUsedMb());
            node.setMemoryTotalMb(report.getMemoryTotalMb());
            node.setDiskUsedGb(report.getDiskUsedGb());
            node.setDiskTotalGb(report.getDiskTotalGb());
            node.setDiskUsedBytes(resolveDiskBytes(report.getDiskUsedBytes(), report.getDiskUsedGb()));
            node.setDiskTotalBytes(resolveDiskBytes(report.getDiskTotalBytes(), report.getDiskTotalGb()));
            applyReportedJmxExporterPort(node, report.getJmxExporterPort());
            node.setLastSeen(seen);
            node.setInstallDir(blankToDefault(report.getInstallPath(), null));
            node.setLogDirs(blankToDefault(report.getLogDirs(), null));
            node.setConfigFile(blankToDefault(report.getConfigFile(), null));
            node.setDataDirs(blankToDefault(report.getDataDirs(), blankToDefault(report.getLogDirs(), null)));
            externalClusterNodeRepository.save(node);
        }
    }

    boolean matchesDiscoveryNode(
            io.translab.tantor.server.domain.ExternalClusterNode node,
            ExternalDiscoveryReport report,
            DiscoveryAgent agent
    ) {
        if (node == null) {
            return false;
        }
        if (report.getNodeId() != null) {
            return report.getNodeId().equals(node.getNodeId());
        }
        String nodeHost = node.getHost();
        if (nodeHost == null || nodeHost.isBlank()) {
            return false;
        }
        for (String candidate : discoveryHostCandidates(report, agent)) {
            if (candidate.equalsIgnoreCase(nodeHost)) {
                return true;
            }
        }
        return matchesDiscoveryAgent(agent, nodeHost);
    }

    private Set<String> discoveryHostCandidates(ExternalDiscoveryReport report, DiscoveryAgent agent) {
        Set<String> candidates = new HashSet<>();
        addCandidate(candidates, report.getHostname());
        addCandidate(candidates, extractHostFromBootstrap(report.getBootstrapServers()));
        if (agent != null) {
            addCandidate(candidates, agent.getHostname());
            candidates.addAll(parseAgentAddresses(agent.getIpAddresses()));
        }
        return candidates;
    }

    private void enrichDiscoveryNodeIdentity(
            io.translab.tantor.server.domain.ExternalClusterNode node,
            ExternalDiscoveryReport report
    ) {
        String reportedHost = blankToDefault(report.getHostname(), null);
        if (reportedHost != null && (node.getHost() == null || node.getHost().isBlank()
                || "unknown".equalsIgnoreCase(node.getHost()))) {
            node.setHost(reportedHost);
        }
        Integer listenerPort = listenerPort(report.getListeners(), Boolean.TRUE.equals(node.getIsController()));
        if (listenerPort != null && (node.getPort() == null || node.getPort() <= 0)) {
            node.setPort(listenerPort);
        }
    }

    private Integer listenerPort(String listeners, boolean controller) {
        if (listeners == null || listeners.isBlank()) {
            return null;
        }
        String fallback = null;
        for (String rawListener : listeners.split(",")) {
            String listener = rawListener.trim();
            if (listener.isBlank()) continue;
            int scheme = listener.indexOf("://");
            String name = scheme > 0 ? listener.substring(0, scheme) : "";
            String address = scheme >= 0 ? listener.substring(scheme + 3) : listener;
            int separator = address.lastIndexOf(':');
            if (separator < 0 || separator == address.length() - 1) continue;
            String port = address.substring(separator + 1).trim();
            if (fallback == null) fallback = port;
            if (controller && !"CONTROLLER".equalsIgnoreCase(name)) continue;
            try {
                int parsed = Integer.parseInt(port);
                if (parsed > 0 && parsed <= 65535) return parsed;
            } catch (NumberFormatException ignored) {
                // Ignore malformed listener entries and continue looking.
            }
        }
        if (controller || fallback == null) return null;
        try {
            int parsed = Integer.parseInt(fallback);
            return parsed > 0 && parsed <= 65535 ? parsed : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private void updateNodeTelemetry(
            io.translab.tantor.server.domain.ExternalClusterNode node,
            ExternalBrokerMetricsDto metrics,
            OffsetDateTime seen
    ) {
        node.setCpuUsagePct(metrics.getCpuUsagePct());
        node.setMemoryUsedMb(metrics.getMemoryUsedMb());
        node.setMemoryTotalMb(metrics.getMemoryTotalMb());
        node.setDiskUsedGb(metrics.getDiskUsedGb());
        node.setDiskTotalGb(metrics.getDiskTotalGb());
        node.setDiskUsedBytes(resolveDiskBytes(metrics.getDiskUsedBytes(), metrics.getDiskUsedGb()));
        node.setDiskTotalBytes(resolveDiskBytes(metrics.getDiskTotalBytes(), metrics.getDiskTotalGb()));
        node.setMessagesInPerSec(metrics.getMessagesInPerSec());
        node.setBytesInPerSec(metrics.getBytesInPerSec());
        node.setLastSeen(seen);
    }

    private Long resolveDiskBytes(Long exactBytes, Long legacyGiB) {
        if (exactBytes != null && exactBytes >= 0) {
            return exactBytes;
        }
        if (legacyGiB == null || legacyGiB < 0) {
            return null;
        }
        long gibibyte = 1024L * 1024L * 1024L;
        return legacyGiB > Long.MAX_VALUE / gibibyte ? Long.MAX_VALUE : legacyGiB * gibibyte;
    }

    private void applyReportedJmxExporterPort(
            io.translab.tantor.server.domain.ExternalClusterNode node,
            Integer reportedPort
    ) {
        if (reportedPort != null && reportedPort >= 1024 && reportedPort <= 65535) {
            node.setJmxExporterPort(reportedPort);
        }
    }

    private void addCandidate(Set<String> candidates, String value) {
        if (value != null && !value.isBlank()) {
            candidates.add(value.trim());
        }
    }

    private Map<String, Object> readMetadata(Cluster cluster) {
        if (cluster.getConfigJson() == null || cluster.getConfigJson().isBlank()) {
            return new HashMap<>();
        }
        try {
            return objectMapper.readValue(cluster.getConfigJson(), new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize external cluster metadata.", e);
        }
    }

    private String discoveryHostId(ExternalDiscoveryReport report) {
        return discoveryHostId(report.getHostname(), report.getBootstrapServers());
    }

    private String discoveryHostId(String hostname, String bootstrapServers) {
        String source = blankToDefault(hostname, "")
                + "|" + extractHostFromBootstrap(bootstrapServers);
        UUID stable = UUID.nameUUIDFromBytes(source.getBytes(StandardCharsets.UTF_8));
        return "discovery-" + stable.toString().substring(0, 18);
    }

    private Optional<Host> findDiscoveryHost(String hostname, String bootstrapServers) {
        String effectiveHostname = blankToDefault(hostname, extractHostFromBootstrap(bootstrapServers));
        String stableId = discoveryHostId(effectiveHostname, bootstrapServers);
        return hostRepository.findById(stableId)
                .or(() -> hostRepository.findFirstByHostnameAndAgentVersion(effectiveHostname, "tantor-discovery-agent"));
    }

    private String taskKey(String clusterName, String hostname, String bootstrap) {
        return blankToDefault(hostname, "") + "|" + blankToDefault(bootstrap, "");
    }

    private String discoveryKey(ExternalDiscoveryReport report) {
        String source = blankToDefault(report.getKafkaClusterId(), "")
                + "|" + blankToDefault(report.getName(), "")
                + "|" + blankToDefault(report.getHostname(), "")
                + "|" + blankToDefault(report.getBootstrapServers(), "");
        return UUID.nameUUIDFromBytes(source.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private boolean isAgentRecord(ExternalBrokerRecord record) {
        return record.getInstallPath() != null && !record.getInstallPath().isBlank();
    }

    long agentStaleSeconds() {
        return Math.max(15, discoveryAgentHeartbeatTimeoutSeconds);
    }

    private boolean isFreshAgent(ExternalBrokerRecord record) {
        try {
            OffsetDateTime seen = OffsetDateTime.parse(record.getLastSeen());
            return seen.isAfter(OffsetDateTime.now().minusSeconds(agentStaleSeconds()));
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isFreshAgent(DiscoveryAgent agent) {
        return agent.getLastHeartbeat() != null
                && agent.getLastHeartbeat().isAfter(OffsetDateTime.now().minusSeconds(agentStaleSeconds()));
    }

    private boolean isFreshOnlineAgent(DiscoveryAgent agent) {
        return "ONLINE".equalsIgnoreCase(agent.getStatus()) && isFreshAgent(agent);
    }

    private boolean isFreshDiscovery(ExternalDiscoveryReport report) {
        try {
            OffsetDateTime seen = OffsetDateTime.parse(report.getLastSeen());
            return seen.isAfter(OffsetDateTime.now().minusSeconds(agentStaleSeconds()));
        } catch (Exception e) {
            return false;
        }
    }

    private ExternalDiscoveryReport requiredPendingDiscovery(String discoveryKey) {
        ExternalDiscoveryReport report = pendingDiscoveries.get(discoveryKey);
        if (report == null || !isFreshDiscovery(report)) {
            throw new IllegalArgumentException("No live discovery-agent report was found. Refresh after the agent reports again.");
        }
        return report;
    }

    private String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private void copyBroker(ExternalBrokerRecord from, ExternalBrokerRecord to) {
        to.setHostname(from.getHostname());
        to.setBootstrap(from.getBootstrap());
        to.setKafkaMode(from.getKafkaMode());
        to.setSecurity(from.getSecurity());
        to.setInstallPath(from.getInstallPath());
        to.setLogDirs(from.getLogDirs());
        to.setRunning(from.isRunning());
        to.setRole(from.getRole());
        to.setNodeId(from.getNodeId());
        to.setLastSeen(from.getLastSeen());
        to.setListeners(from.getListeners());
        to.setAdvertisedListeners(from.getAdvertisedListeners());
        to.setProcessRoles(from.getProcessRoles());
    }

    private int intValue(Object value, int defaultValue) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException ignored) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private String extractHostFromBootstrap(String bootstrap) {
        if (bootstrap == null || bootstrap.isBlank()) {
            return "";
        }
        String first = bootstrap.split(",")[0].trim();
        if (first.contains("://")) {
            first = first.substring(first.indexOf("://") + 3);
        }
        int idx = first.lastIndexOf(":");
        return idx > 0 ? first.substring(0, idx) : first;
    }

    private boolean bootstrapServersOverlap(String left, String right) {
        List<String> leftEndpoints = splitBootstrapEndpoints(left);
        List<String> rightEndpoints = splitBootstrapEndpoints(right);
        if (leftEndpoints.isEmpty() || rightEndpoints.isEmpty()) {
            return false;
        }
        for (String leftEndpoint : leftEndpoints) {
            for (String rightEndpoint : rightEndpoints) {
                if (leftEndpoint.equalsIgnoreCase(rightEndpoint)) {
                    return true;
                }
                String leftPort = endpointPort(leftEndpoint);
                String rightPort = endpointPort(rightEndpoint);
                if (!leftPort.isBlank()
                        && leftPort.equals(rightPort)
                        && hostsCompatible(endpointHost(leftEndpoint), endpointHost(rightEndpoint))) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<String> splitBootstrapEndpoints(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<String> endpoints = new ArrayList<>();
        for (String endpoint : value.split(",")) {
            String normalized = endpoint.trim();
            if (normalized.contains("://")) {
                normalized = normalized.substring(normalized.indexOf("://") + 3);
            }
            if (!normalized.isBlank()) {
                endpoints.add(normalized);
            }
        }
        return endpoints;
    }

    private String endpointHost(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return "";
        }
        String value = endpoint.trim();
        if (value.startsWith("[")) {
            int end = value.indexOf(']');
            return end > 0 ? value.substring(1, end) : "";
        }
        int idx = value.lastIndexOf(":");
        return idx > 0 ? value.substring(0, idx) : value;
    }

    private String endpointPort(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return "";
        }
        String value = endpoint.trim();
        int idx = value.lastIndexOf(":");
        return idx > 0 && idx < value.length() - 1 ? value.substring(idx + 1) : "";
    }

    private boolean hostsCompatible(String left, String right) {
        if (left == null || right == null || left.isBlank() || right.isBlank()) {
            return false;
        }
        if (left.equalsIgnoreCase(right)) {
            return true;
        }
        return isWildcardHost(left) || isWildcardHost(right);
    }

    private boolean isWildcardHost(String host) {
        return "localhost".equalsIgnoreCase(host)
                || "0.0.0.0".equals(host)
                || "::".equals(host)
                || "*".equals(host);
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private boolean matchesDiscoveryAgent(DiscoveryAgent agent, String host) {
        if (agent == null || host == null || host.isBlank()) {
            return false;
        }
        if (agent.getHostname() != null && agent.getHostname().equalsIgnoreCase(host)) {
            return true;
        }
        return parseAgentAddresses(agent.getIpAddresses()).stream()
                .anyMatch(address -> address.equalsIgnoreCase(host));
    }

    private List<String> parseAgentAddresses(String ipAddresses) {
        if (ipAddresses == null || ipAddresses.isBlank()) {
            return List.of();
        }
        try {
            List<String> values = objectMapper.readValue(ipAddresses, new TypeReference<List<String>>() {});
            return values.stream().filter(value -> value != null && !value.isBlank()).toList();
        } catch (Exception ignored) {
            List<String> values = new ArrayList<>();
            for (String part : ipAddresses.replaceAll("\\[|\\]|\\\"", "").split(",")) {
                String value = part.trim();
                if (!value.isBlank()) {
                    values.add(value);
                }
            }
            return values;
        }
    }

    private String mergeBootstrapServers(String existing, String reported) {
        Map<String, Boolean> endpoints = new LinkedHashMap<>();
        for (String value : List.of(blankToDefault(existing, ""), blankToDefault(reported, ""))) {
            for (String endpoint : value.split(",")) {
                String normalized = endpoint.trim();
                if (!normalized.isBlank()) {
                    endpoints.put(normalized, true);
                }
            }
        }
        return String.join(",", endpoints.keySet());
    }

    private String blankToDefault(String value, String defaultValue) {
        return value == null || value.isBlank() || "null".equalsIgnoreCase(value) ? defaultValue : value;
    }

    private String normalizeBase64(String value) {
        return value == null ? null : value.replaceAll("\\s", "");
    }

    private String firstString(Map<String, Object> values, String... keys) {
        if (values == null) return null;
        for (String key : keys) {
            Object value = values.get(key);
            if (value != null && !String.valueOf(value).isBlank() && !"null".equalsIgnoreCase(String.valueOf(value))) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private Map<String, Object> externalAuditDetails(ExternalCluster cluster) {
        List<ExternalBrokerRecord> brokers = readBrokerRecords(cluster);
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("name", cluster.getName());
        details.put("kafkaClusterId", cluster.getKafkaClusterId());
        details.put("bootstrapServers", cluster.getBootstrapServers());
        details.put("kafkaVersion", blankToDefault(cluster.getKafkaVersion(), "Unknown"));
        details.put("kafkaMode", blankToDefault(cluster.getKafkaMode(), "Unknown"));
        details.put("environment", blankToDefault(cluster.getEnvironment(), "unknown"));
        details.put("security", blankToDefault(cluster.getSecurity(), "PLAINTEXT"));
        details.put("listeners", cluster.getListeners());
        details.put("advertisedListeners", cluster.getAdvertisedListeners());
        details.put("processRoles", cluster.getProcessRoles());
        details.put("externalBrokerHosts", brokers.stream().map(ExternalBrokerRecord::getHostname).toList());
        details.put("brokerCount", cluster.getBrokerCount());
        details.put("installPath", cluster.getInstallPath());
        details.put("logDirs", cluster.getLogDirs());
        details.put("cpuUsagePct", cluster.getCpuUsagePct());
        details.put("memoryUsedMb", cluster.getMemoryUsedMb());
        details.put("memoryTotalMb", cluster.getMemoryTotalMb());
        details.put("diskUsedGb", cluster.getDiskUsedGb());
        details.put("diskTotalGb", cluster.getDiskTotalGb());
        details.put("running", cluster.getIsRunning());
        return details;
    }

    private boolean safeEquals(String left, String right) {
        return left != null && right != null && left.equals(right);
    }

    private boolean matchesExternalCluster(ExternalDiscoveryReport report, ExternalCluster cluster) {
        if (report == null || cluster == null) {
            return false;
        }
        return safeEquals(report.getKafkaClusterId(), cluster.getKafkaClusterId())
                || safeEquals(report.getName(), cluster.getName())
                || safeEquals(report.getBootstrapServers(), cluster.getBootstrapServers());
    }

    @Data
    public static class BootstrapExternalClusterRequest {
        private String name;
        private String environment;
        private String bootstrapServers;
        private String kafkaVersion;
        private String kafkaMode;
        private String security; // Legacy or UI fallback
        private String securityProtocol;
        private String saslMechanism;
        private String saslUsername;
        private String saslPassword;
        private String truststoreType;
        private String truststorePassword;
        private String truststoreBase64;
        private String truststoreFilename;
        private String keystoreType;
        private String keystorePassword;
        private String keyPassword;
        private String keystoreBase64;
        private String keystoreFilename;
        private Boolean disableHostnameVerification;
        private String clusterId;
        private Integer brokerCount;
        private Boolean agentFound;
        private String discoveryKey;
        private String controllerId;
        private java.util.List<java.util.Map<String, Object>> brokers;
        private java.util.Map<String, String> selectedAgents;
    }

    @Data
    public static class ExternalDiscoveryReport {
        private String hostId;
        private String agentName;
        private String name;
        private String environment;
        private String bootstrapServers;
        private String kafkaVersion;
        private String kafkaClusterId;
        private String kafkaMode;
        private String security;
        private int brokerCount;
        private Integer nodeId;
        @JsonProperty("isRunning")
        private boolean isRunning;
        private String installPath;
        private String logDirs;
        private String configFile;
        private String dataDirs;
        private String hostname;
        private String ipAddresses;
        private String lastSeen;
        private boolean canExecuteTasks;
        private String agentVersion;
        private boolean canDeployServices;
        private String listeners;
        private String advertisedListeners;
        private String processRoles;
        private Double cpuUsagePct;
        private Long memoryUsedMb;
        private Long memoryTotalMb;
        private Long diskUsedGb;
        private Long diskTotalGb;
        private Long diskUsedBytes;
        private Long diskTotalBytes;
        private Integer jmxExporterPort;
    }

    @Data
    public static class ExternalBrokerMetricsDto {
        private String hostId;
        private String hostname;
        private String bootstrap;
        private Integer nodeId;
        private Double cpuUsagePct;
        private Long memoryUsedMb;
        private Long memoryTotalMb;
        private Long diskUsedGb;
        private Long diskTotalGb;
        private Long diskUsedBytes;
        private Long diskTotalBytes;
        private Double messagesInPerSec;
        private Double bytesInPerSec;
    }

    @Data
    public static class ExternalBrokerRecord {
        private String hostname;
        private String bootstrap;
        private String kafkaMode;
        private String security;
        private String installPath;
        private String logDirs;
        private String role;
        private Integer nodeId;
        private boolean running;
        private String lastSeen;
        private Double cpuUsagePct;
        private Long memoryUsedMb;
        private Long memoryTotalMb;
        private Long diskUsedGb;
        private Long diskTotalGb;
        private Long diskUsedBytes;
        private Long diskTotalBytes;
        private Double messagesInPerSec;
        private Double bytesInPerSec;
        private String listeners;
        private String advertisedListeners;
        private String processRoles;
    }

    @Data
    public static class ExternalAgentTask {
        private String taskId;
        private String task;
        private String status;
        private String clusterName;
        private String hostname;
        private String bootstrap;
        private String configKey;
        private String configValue;
        private boolean restart;
        private String message;
        private String configFilePath;
        private String backupDirPath;
        private String backupFilePath;
        private Map<String, String> configChanges;
        private String serviceName;
        private Map<String, String> data;
    }

    @Scheduled(fixedDelay = 60000)
    @Transactional
    public void checkExternalClustersHealth() {
        List<ExternalCluster> externalClusters = externalClusterRepository.findByStatusNot("DELETED");
        for (ExternalCluster cluster : externalClusters) {
            try {
                String previousStatus = cluster.getStatus();
                Map<String, Object> adminData = kafkaAdminService.inspectBootstrapServers(cluster, true);
                boolean connected = Boolean.TRUE.equals(adminData.get("connected"));
                String detectedKafkaClusterId = firstString(adminData, "clusterId", "kafka_cluster_id");
                boolean kafkaClusterIdBackfilled = (cluster.getKafkaClusterId() == null || cluster.getKafkaClusterId().isBlank())
                        && detectedKafkaClusterId != null && !detectedKafkaClusterId.isBlank();
                if (kafkaClusterIdBackfilled) {
                    cluster.setKafkaClusterId(detectedKafkaClusterId.trim());
                }
                
                List<DiscoveryAgent> discoveryAgents = discoveryAgentRepository.findByClusterId(cluster.getId());
                long registeredAgents = discoveryAgents.size();
                long freshAgents = discoveryAgents.stream()
                        .filter(this::isFreshOnlineAgent)
                        .count();
                List<ActivityAlertService.OfflineAgentInfo> offlineAgents = discoveryAgents.stream()
                        .filter(agent -> !isFreshOnlineAgent(agent))
                        .map(agent -> new ActivityAlertService.OfflineAgentInfo(
                                agent.getId(),
                                agent.getHostname(),
                                parseAgentAddresses(agent.getIpAddresses()).stream()
                                        .filter(address -> address != null && !address.isBlank())
                                        .distinct()
                                        .toList()
                        ))
                        .toList();

                String newStatus;
                if (!connected) {
                    newStatus = "FAILED";
                } else {
                    // Check if discovery agent is healthy
                    // Every registered agent must be reachable. A healthy peer must
                    // not hide an agent outage for the same external cluster.
                    boolean agentHealthy = registeredAgents > 0 && freshAgents == registeredAgents;
                    
                    newStatus = agentHealthy ? "SUCCESS" : "DEGRADED";
                }
                
                boolean statusChanged = !newStatus.equals(previousStatus);
                if (statusChanged || kafkaClusterIdBackfilled) {
                    cluster.setStatus(newStatus);
                    externalClusterRepository.save(cluster);
                    
                }
                // Synchronize on every health cycle, not only on a status
                // transition. This also repairs legacy ACTIVE alerts left behind
                // after an agent recovered before this lifecycle was introduced.
                activityAlertService.synchronizeExternalClusterHealth(
                        cluster.getId(), cluster.getName(), newStatus, freshAgents, registeredAgents, offlineAgents);
            } catch (Exception e) {
                log.error("Failed to check health for external cluster {}", cluster.getName(), e);
            }
        }
        activityAlertService.resolveOrphanedExternalClusterHealthAlerts(
                externalClusters.stream()
                        .map(ExternalCluster::getId)
                        .filter(java.util.Objects::nonNull)
                        .collect(java.util.stream.Collectors.toSet()));
    }

    @SuppressWarnings("unchecked")
    private void normalizeInspectionNodeRolesForMode(Map<String, Object> inspection, String kafkaMode) {
        if (!"ZooKeeper".equalsIgnoreCase(normalizeKafkaMode(kafkaMode))) {
            return;
        }
        Object rawNodes = inspection.get("brokers");
        if (!(rawNodes instanceof List<?> nodes)) {
            return;
        }
        for (Object rawNode : nodes) {
            if (rawNode instanceof Map<?, ?> rawMap) {
                Map<String, Object> node = (Map<String, Object>) rawMap;
                node.put("isBroker", true);
                node.put("isController", false);
            }
        }
    }

    @Data
    public static class AgentTaskCompletion {
        private String status;
        private String message;
        private Map<String, String> data;
    }
}
