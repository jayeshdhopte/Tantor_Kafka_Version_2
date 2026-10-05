package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.service.DeploymentService;
import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import io.translab.tantor.server.service.HostStatusService;
import io.translab.tantor.server.service.JobService;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobType;
import io.translab.tantor.server.domain.JobStatus;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/ui/clusters")
@RequiredArgsConstructor
@lombok.extern.slf4j.Slf4j
public class ClusterController {
    private static final int DEFAULT_JMX_EXPORTER_PORT = 7071;
    private static final int DEFAULT_CONTROLLER_JMX_EXPORTER_PORT = 7072;

    private final DeploymentService deploymentService;
    private final ClusterRepository clusterRepository;
    private final ExternalClusterRepository externalClusterRepository;
    private final io.translab.tantor.server.repository.ExternalClusterNodeRepository externalClusterNodeRepository;
    private final io.translab.tantor.server.repository.TaskRepository taskRepository;
    private final io.translab.tantor.server.repository.HostRepository hostRepository;
    private final io.translab.tantor.server.repository.HostParcelRepository hostParcelRepository;
    private final io.translab.tantor.server.service.BrokerMetricsCacheService brokerMetricsCacheService;
    private final io.translab.tantor.server.service.ClusterOverviewService clusterOverviewService;
    private final ObjectMapper objectMapper;
    private final io.translab.tantor.server.service.ActivityAlertService activityAlertService;
    private final HostStatusService hostStatusService;
    private final io.translab.tantor.server.service.ExternalClusterService externalClusterService;
    private final io.translab.tantor.server.service.KafkaAdminService kafkaAdminService;
    private final JobService jobService;
    private final io.translab.tantor.server.audit.AuditService auditService;
    private final io.translab.tantor.server.repository.DiscoveryAgentRepository discoveryAgentRepository;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    private final ArtifactRepositoryProperties artifactRepositoryProperties;

    @Value("${tantor.discovery-agent.heartbeat-timeout-seconds:45}")
    private long discoveryAgentHeartbeatTimeoutSeconds;

    @Value("${tantor.external-clusters.kafka-health-timeout-seconds:5}")
    private long externalKafkaHealthTimeoutSeconds;

    @GetMapping
    public List<Map<String, Object>> listClusters() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Cluster c : clusterRepository.findByStatusNot("DELETED")) {
            if ("EXTERNAL".equalsIgnoreCase(c.getMode())) {
                continue;
            }
            Map<String, Object> m = new HashMap<>();
            m.put("id", c.getId());
            m.put("name", c.getName());
            m.put("clusterName", c.getName());
            m.put("originType", c.getOriginType());
            m.put("installDirectory", c.getInstallDirectory());
            m.put("kafkaVersion", c.getKafkaVersion());
            m.put("mode", c.getMode());
            m.put("environment", c.getEnvironment());
            m.put("createdAt", c.getCreatedAt());
            m.put("createdBy", c.getCreatedBy());
            m.put("updatedBy", c.getUpdatedBy());
            m.put("user", c.getUser());
            m.put("role", c.getRole());
            m.put("configPath", c.getConfigPath());
            m.put("nodeIds", c.getNodeIds());
            m.put("status", c.getStatus());
            m.put("bootstrapServers", c.getBootstrapServers());
            m.put("clusterId", c.getId().toString());
            m.put("kafkaClusterId", kafkaClusterId(c));
            m.put("config", parseConfigJson(c.getConfigJson()));
            m.put("serviceRoles", c.getServices() == null ? List.of() : c.getServices().stream().map(ClusterServiceAssignment::getRole).distinct().toList());
            m.put("managementLevel", managementLevel(c));
            m.put("sourceLabel", sourceLabel(c));
            m.put("accessLabel", accessLabel(c));
            List<Map<String, Object>> hosts = clusterHosts(c);
            m.put("nodeCount", hosts.isEmpty() && c.getServices() != null ? c.getServices().size() : hosts.size());
            m.put("hosts", hosts);
            m.putAll(internalRuntimeHealth(c));
            result.add(m);
        }
        List<io.translab.tantor.server.domain.DiscoveryAgent> discoveryAgents = discoveryAgentRepository.findAll();
        for (ExternalCluster c : externalClusterRepository.findByStatusNot("DELETED")) {
            Map<String, Object> m = new HashMap<>();
            m.put("id", c.getId());
            m.put("name", c.getName());
            m.put("clusterName", c.getName());
            m.put("originType", "EXTERNAL");
            m.put("installDirectory", c.getInstallPath());
            m.put("kafkaVersion", c.getKafkaVersion());
            m.put("mode", "EXTERNAL");
            m.put("environment", c.getEnvironment());
            m.put("createdAt", c.getCreatedAt());
            m.put("createdBy", c.getCreatedBy());
            m.put("updatedBy", c.getUpdatedBy());
            m.put("nodeIds", new ArrayList<>());
            m.put("status", c.getStatus());
            m.put("bootstrapServers", c.getBootstrapServers());
            m.put("clusterId", c.getId().toString());
            m.put("kafkaClusterId", c.getKafkaClusterId());
            m.put("config", new HashMap<>());
            List<io.translab.tantor.server.domain.ExternalClusterNode> clusterNodes = externalClusterNodeRepository.findByClusterId(c.getId());
            ExternalHealthView health = externalHealthView(c, clusterNodes, discoveryAgents, false);

            m.put("managementLevel", health.managementLevel());
            m.put("sourceLabel", "External");
            m.put("accessLabel", health.managementLevel());
            m.put("telemetry", health.telemetry());
            m.put("managedHostsCount", health.managedHostsCount());
            m.put("totalHostsCount", health.totalHostsCount());
            m.put("lastAgentHeartbeat", health.lastAgentHeartbeat() != null ? health.lastAgentHeartbeat().toString() : null);
            putExternalHealth(m, health);
            List<Map<String, Object>> hosts = externalClusterHosts(c, clusterNodes);
            m.put("nodeCount", hosts.isEmpty() ? clusterNodes.size() : hosts.size());
            m.put("hosts", hosts);
            result.add(m);
        }
        return result;
    }

    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getCluster(@PathVariable java.util.UUID id) {
        var internalOpt = clusterRepository.findById(id).map(c -> {
            if ("EXTERNAL".equalsIgnoreCase(c.getMode())) {
                return null;
            }
            Map<String, Object> m = new HashMap<>();
            m.put("id", c.getId());
            m.put("name", c.getName());
            m.put("clusterName", c.getName());
            m.put("originType", c.getOriginType());
            m.put("kafkaVersion", c.getKafkaVersion());
            m.put("mode", c.getMode());
            m.put("environment", c.getEnvironment());
            m.put("createdAt", c.getCreatedAt());
            m.put("createdBy", c.getCreatedBy());
            m.put("updatedBy", c.getUpdatedBy());
            m.put("user", c.getUser());
            m.put("role", c.getRole());
            m.put("configPath", c.getConfigPath());
            m.put("nodeIds", c.getNodeIds());
            m.put("status", c.getStatus());
            m.put("bootstrapServers", c.getBootstrapServers());
            m.put("clusterId", c.getId().toString());
            m.put("kafkaClusterId", kafkaClusterId(c));
            m.put("installDirectory", c.getInstallDirectory());
            m.put("configDirectory", c.getConfigDirectory());
            m.put("dataDirectory", c.getDataDirectory());
            m.put("logDirectory", c.getLogDirectory());
            m.put("config", parseConfigJson(c.getConfigJson()));
            m.put("serviceRoles", c.getServices() == null ? List.of() : c.getServices().stream().map(ClusterServiceAssignment::getRole).distinct().toList());
            m.put("managementLevel", managementLevel(c));
            m.put("sourceLabel", sourceLabel(c));
            m.put("accessLabel", accessLabel(c));
            List<Map<String, Object>> hosts = clusterHosts(c);
            m.put("nodeCount", hosts.isEmpty() && c.getServices() != null ? c.getServices().size() : hosts.size());
            m.put("hosts", hosts);
            m.putAll(internalRuntimeHealth(c));
            return ResponseEntity.ok(m);
        });
        
        if (internalOpt.isPresent() && internalOpt.get() != null) {
            return internalOpt.get();
        }
        
        return externalClusterRepository.findById(id).map(c -> {
            Map<String, Object> m = new HashMap<>();
            m.put("id", c.getId());
            m.put("name", c.getName());
            m.put("clusterName", c.getName());
            m.put("originType", "EXTERNAL");
            m.put("kafkaVersion", c.getKafkaVersion());
            m.put("mode", "EXTERNAL");
            m.put("environment", c.getEnvironment());
            m.put("createdAt", c.getCreatedAt());
            m.put("createdBy", c.getCreatedBy());
            m.put("updatedBy", c.getUpdatedBy());
            m.put("nodeIds", new ArrayList<>());
            m.put("status", c.getStatus());
            m.put("bootstrapServers", c.getBootstrapServers());
            m.put("clusterId", c.getId().toString());
            m.put("kafkaClusterId", c.getKafkaClusterId());
            m.put("installDirectory", c.getInstallPath());
            m.put("configDirectory", "");
            m.put("dataDirectory", "");
            m.put("logDirectory", c.getLogDirs());
            m.put("config", new HashMap<>());
            m.put("sourceLabel", "External");

            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes = externalClusterNodeRepository.findByClusterId(c.getId());
            ExternalHealthView health = externalHealthView(c, nodes, discoveryAgentRepository.findAll(), true);
            List<Map<String, Object>> hosts = externalClusterHosts(c, nodes);
            m.put("managementLevel", health.managementLevel());
            m.put("accessLabel", health.managementLevel());
            m.put("telemetry", health.telemetry());
            m.put("nodeCount", hosts.isEmpty() ? nodes.size() : hosts.size());
            m.put("hosts", hosts);
            m.put("managedHostsCount", health.managedHostsCount());
            m.put("totalHostsCount", health.totalHostsCount());
            m.put("lastAgentHeartbeat", health.lastAgentHeartbeat() != null ? health.lastAgentHeartbeat().toString() : null);
            putExternalHealth(m, health);
            return ResponseEntity.ok(m);
        }).orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID id,
            @RequestBody UpdateClusterRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_CHANGE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        Cluster cluster = clusterRepository.findById(id).orElse(null);
        if (cluster == null) return ResponseEntity.notFound().build();
        String name = request.getName() == null ? "" : request.getName().trim();
        String environment = request.getEnvironment() == null ? "" : request.getEnvironment().trim().toUpperCase();
        if (name.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Cluster name is required."));
        }
        boolean duplicateName = clusterRepository.findByNameAndStatusNot(name, "DELETED")
                .filter(existing -> !existing.getId().equals(id))
                .isPresent();
        if (duplicateName) {
            return ResponseEntity.badRequest().body(Map.of("error", "Another active cluster already uses this name."));
        }
        if (!Set.of("DEV", "SIT", "UAT").contains(environment)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Environment must be DEV, SIT, or UAT."));
        }
        cluster.setName(name);
        cluster.setEnvironment(environment);
        String username = roleAuthenticationUtil.extractUsername(authorization);
        cluster.setUpdatedBy(username);
        clusterRepository.save(cluster);
        auditService.record("CLUSTER_CHANGE", "CLUSTER_DETAILS_UPDATED", "CLUSTER", cluster.getId().toString(),
                cluster.getId(), "SUCCESS", null, null, null,
                Map.of("clusterName", cluster.getName(), "nodeIds", cluster.getNodeIds(), "createdBy", cluster.getCreatedBy()));
        activityAlertService.logActivity("INFO", "Updated cluster details for " + name, id);
        return ResponseEntity.ok(Map.of("id", id.toString(), "name", name, "environment", environment));
    }

    @GetMapping("/{id}/tasks")
    public ResponseEntity<List<io.translab.tantor.server.domain.Task>> getClusterTasks(@PathVariable java.util.UUID id) {
        return clusterRepository.findById(id).map(cluster -> {
            List<io.translab.tantor.server.domain.Task> tasks = taskRepository.findByClusterIdOrderByCreatedAtDesc(id);
            return ResponseEntity.ok(tasks);
        }).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/bind-agent")
    public ResponseEntity<?> bindAgent(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable java.util.UUID id,
            @RequestBody Map<String, Object> request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.BIND_AGENT)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        String agentIdStr = (String) request.get("agentId");
        if (agentIdStr == null || agentIdStr.isBlank()) return ResponseEntity.badRequest().body(Map.of("error", "Agent ID required"));
        
        Optional<io.translab.tantor.server.domain.ExternalCluster> extClusterOpt = externalClusterRepository.findById(id);
        if (extClusterOpt.isEmpty()) return ResponseEntity.notFound().build();
        
        Optional<io.translab.tantor.server.domain.DiscoveryAgent> agentOpt = discoveryAgentRepository.findById(agentIdStr);
        if (agentOpt.isPresent()) {
            io.translab.tantor.server.domain.DiscoveryAgent agent = agentOpt.get();
            agent.setClusterId(id);
            discoveryAgentRepository.save(agent);
            
            // Also invoke activity log
            activityAlertService.logActivity("INFO", "Bound discovery agent " + agent.getHostname() + " to external cluster " + extClusterOpt.get().getName(), id);
                
            return ResponseEntity.ok(Map.of("success", true));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "Agent not found"));
    }

    @GetMapping("/{id}/brokers")
    public ResponseEntity<Map<String, Object>> getClusterBrokers(@PathVariable java.util.UUID id) {
        Optional<Cluster> clusterOpt = clusterRepository.findById(id);
        if (clusterOpt.isPresent() && !"EXTERNAL".equalsIgnoreCase(clusterOpt.get().getMode())) {
            return clusterOpt.map(cluster -> {
                List<io.translab.tantor.server.dto.BrokerSummaryDto> brokers = brokerMetricsCacheService.getBrokerSummaries(cluster);
                Map<String, Object> response = new HashMap<>();
                response.put("clusterId", cluster.getId());
                response.put("brokers", brokers);
                return ResponseEntity.ok(response);
            }).orElse(ResponseEntity.notFound().build());
        }

        return externalClusterRepository.findById(id).map(extCluster -> {
            List<io.translab.tantor.server.dto.BrokerSummaryDto> brokers = brokerMetricsCacheService.getBrokerSummaries(extCluster);
            Map<String, Object> response = new HashMap<>();
            response.put("clusterId", extCluster.getId());
            response.put("brokers", brokers);
            return ResponseEntity.ok(response);
        }).orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}/overview")
    public ResponseEntity<io.translab.tantor.server.dto.ClusterOverviewDto> getClusterOverview(@PathVariable java.util.UUID id) {
        Optional<Cluster> clusterOpt = clusterRepository.findById(id);
        if (clusterOpt.isPresent() && !"EXTERNAL".equalsIgnoreCase(clusterOpt.get().getMode())) {
            return ResponseEntity.ok(clusterOverviewService.getOverview(id));
        }

        return externalClusterRepository.findById(id).map(extCluster -> {
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes = externalClusterNodeRepository.findByClusterId(id).stream()
                    .sorted(Comparator
                            .comparing(io.translab.tantor.server.domain.ExternalClusterNode::getNodeId,
                                    Comparator.nullsLast(Comparator.naturalOrder()))
                            .thenComparing(io.translab.tantor.server.domain.ExternalClusterNode::getHost,
                                    Comparator.nullsLast(Comparator.naturalOrder()))
                            .thenComparing(io.translab.tantor.server.domain.ExternalClusterNode::getId,
                                    Comparator.nullsLast(Comparator.naturalOrder())))
                    .toList();
            List<String> warnings = new ArrayList<>();
            io.translab.tantor.server.dto.ClusterOverviewDto liveOverview = null;
            try {
                liveOverview = clusterOverviewService.getOverview(id);
            } catch (Exception e) {
                log.warn("Live external cluster overview failed for {}: {}", id, e.getMessage());
                warnings.add("Live Kafka metadata is unavailable. Showing the last saved external cluster metadata.");
            }

            int brokerCount = 0;
            int activeControllerCount = 0;
            List<io.translab.tantor.server.dto.ClusterOverviewDto.BrokerRow> brokerRows = new ArrayList<>();
            List<io.translab.tantor.server.dto.ClusterOverviewDto.ControllerRow> controllerRows = new ArrayList<>();
            List<io.translab.tantor.server.dto.ClusterOverviewDto.NodePathRow> nodePathRows = new ArrayList<>();
            String overviewInstallDir = null;
            String overviewConfigDir = null;
            String overviewDataDir = null;
            String overviewLogDir = null;
            String displayVersion = externalKafkaVersion(extCluster, nodes);
            String displayControllerType = externalControllerType(extCluster, nodes);
            boolean zookeeperMode = "ZooKeeper".equalsIgnoreCase(displayControllerType);
            List<io.translab.tantor.server.domain.DiscoveryAgent> linkedAgents =
                    discoveryAgentRepository.findByClusterId(id);
            List<io.translab.tantor.server.domain.DiscoveryAgent> allAgents =
                    discoveryAgentRepository.findAll();

            for (io.translab.tantor.server.domain.ExternalClusterNode node : nodes) {
                boolean isBroker = Boolean.TRUE.equals(node.getIsBroker());
                boolean isController = Boolean.TRUE.equals(node.getIsController());
                boolean hasFreshAgent = matchingFreshAgent(
                        node.getHost(), id, linkedAgents, allAgents).isPresent();
                boolean hasFreshTelemetry = hasFreshAgent
                        && node.getLastSeen() != null
                        && node.getLastSeen().isAfter(java.time.OffsetDateTime.now()
                                .minusSeconds(discoveryAgentFreshnessSeconds()));
                if (isBroker) brokerCount++;
                if (isController) activeControllerCount++;

                if (isBroker) {
                    brokerRows.add(io.translab.tantor.server.dto.ClusterOverviewDto.BrokerRow.builder()
                            .brokerId(node.getNodeId() != null ? node.getNodeId() : -1)
                            .host(node.getHost())
                            .port(node.getPort())
                            .controller(isController)
                            .diskUsageBytes(0L)
                            .diskTotalBytes(0L)
                            .hostDiskUsedBytes(hasFreshTelemetry ? externalDiskBytes(node.getDiskUsedBytes(), node.getDiskUsedGb()) : null)
                            .hostDiskTotalBytes(hasFreshTelemetry ? externalDiskBytes(node.getDiskTotalBytes(), node.getDiskTotalGb()) : null)
                            .hostDiskMetricStatus(hasFreshTelemetry ? "LIVE" : (node.getLastSeen() == null ? "UNAVAILABLE" : "STALE"))
                            .hostDiskLastSeen(node.getLastSeen())
                            .build());
                }
                
                if (isController && !zookeeperMode) {
                    controllerRows.add(io.translab.tantor.server.dto.ClusterOverviewDto.ControllerRow.builder()
                            .nodeId(node.getNodeId() != null ? node.getNodeId() : -1)
                            .host(node.getHost())
                            .port(node.getPort())
                            .build());
                }
                
                String role = zookeeperMode && isBroker
                        ? "broker"
                        : ((isBroker && isController) ? "broker_controller" : (isBroker ? "broker" : (isController ? "controller" : "unknown")));
                String installDir = hasFreshAgent
                        ? firstNonBlank(node.getInstallDir(), extCluster.getInstallPath())
                        : null;
                String configFile = hasFreshAgent
                        ? firstNonBlank(node.getConfigFile(),
                                inferredExternalConfigFile(role, displayControllerType, displayVersion, installDir))
                        : null;
                String dataDir = hasFreshAgent ? firstNonBlank(node.getDataDirs()) : null;
                String logDir = hasFreshAgent
                        ? firstNonBlank(node.getLogDirs(), extCluster.getLogDirs())
                        : null;
                overviewInstallDir = firstNonBlank(overviewInstallDir, installDir);
                overviewConfigDir = firstNonBlank(overviewConfigDir, parentPath(configFile));
                overviewDataDir = firstNonBlank(overviewDataDir, dataDir);
                overviewLogDir = firstNonBlank(overviewLogDir, logDir);
                nodePathRows.add(io.translab.tantor.server.dto.ClusterOverviewDto.NodePathRow.builder()
                        .nodeId(node.getNodeId() != null ? node.getNodeId() : -1)
                        .host(node.getHost())
                        .role(role)
                        .installDir(installDir)
                        .config(configFile)
                        .dataDir(dataDir)
                        .logDir(logDir)
                        .hasTelemetry(hasFreshAgent)
                        .build());
            }

            if (liveOverview != null) {
                // Kafka's Admin API reports the size of Kafka replica files only.  For
                // external clusters, the discovery agent also reports the host file
                // system capacity.  Prefer that fresh, node-specific telemetry so the
                // overview's disk column means the same thing for both cluster types.
                applyExternalNodeDiskTelemetry(liveOverview, nodes, id, linkedAgents, allAgents);
                liveOverview.setOriginType("EXTERNAL");
                liveOverview.setKafkaVersion(firstNonBlank(cleanExternalValue(liveOverview.getKafkaVersion()), displayVersion));
                liveOverview.setControllerType(firstNonBlank(cleanExternalValue(liveOverview.getControllerType()), displayControllerType));
                liveOverview.setInstallDirectory(firstNonBlank(liveOverview.getInstallDirectory(), overviewInstallDir));
                liveOverview.setConfigDirectory(firstNonBlank(liveOverview.getConfigDirectory(), overviewConfigDir));
                liveOverview.setDataDirectory(firstNonBlank(liveOverview.getDataDirectory(), overviewDataDir));
                liveOverview.setLogDirectory(firstNonBlank(liveOverview.getLogDirectory(), overviewLogDir));
                liveOverview.setControllers(controllerRows);
                liveOverview.setNodePaths(nodePathRows);
                if (liveOverview.getUptime() != null) {
                    liveOverview.getUptime().setVersion(firstNonBlank(cleanExternalValue(liveOverview.getUptime().getVersion()), displayVersion));
                    liveOverview.getUptime().setControllerType(firstNonBlank(cleanExternalValue(liveOverview.getUptime().getControllerType()), displayControllerType));
                    liveOverview.getUptime().setConfiguredControllerCount(activeControllerCount);
                    Integer activeControllerId = liveOverview.getUptime().getActiveControllerId();
                    for (io.translab.tantor.server.dto.ClusterOverviewDto.ControllerRow controllerRow : controllerRows) {
                        controllerRow.setActiveLeader(activeControllerId != null
                                && activeControllerId == controllerRow.getNodeId());
                    }
                }
                if (!warnings.isEmpty()) {
                    List<String> mergedWarnings = new ArrayList<>();
                    if (liveOverview.getWarnings() != null) {
                        mergedWarnings.addAll(liveOverview.getWarnings());
                    }
                    for (String warning : warnings) {
                        if (!mergedWarnings.contains(warning)) {
                            mergedWarnings.add(warning);
                        }
                    }
                    liveOverview.setWarnings(mergedWarnings);
                }
                return ResponseEntity.ok(liveOverview);
            }

            io.translab.tantor.server.dto.ClusterOverviewDto dto = io.translab.tantor.server.dto.ClusterOverviewDto.builder()
                    .clusterId(extCluster.getId())
                    .kafkaClusterId(extCluster.getKafkaClusterId())
                    .name(extCluster.getName())
                    .kafkaVersion(displayVersion)
                    .controllerType(displayControllerType)
                    .originType("EXTERNAL")
                    .installDirectory(overviewInstallDir)
                    .configDirectory(overviewConfigDir)
                    .dataDirectory(overviewDataDir)
                    .logDirectory(overviewLogDir)
                    .generatedAt(java.time.OffsetDateTime.now())
                    .warnings(warnings)
                    .uptime(io.translab.tantor.server.dto.ClusterOverviewDto.UptimeSummary.builder()
                            .brokerCount(extCluster.getBrokerCount() != null ? extCluster.getBrokerCount() : brokerCount)
                            .activeController(null)
                            .activeControllerId(null)
                            .configuredControllerCount(activeControllerCount)
                            .version(displayVersion)
                            .controllerType(displayControllerType)
                            .build())
                    .partitions(io.translab.tantor.server.dto.ClusterOverviewDto.PartitionSummary.builder().build())
                    .brokers(brokerRows)
                    .controllers(controllerRows)
                    .nodePaths(nodePathRows)
                    .build();

            return ResponseEntity.ok(dto);
        }).orElse(ResponseEntity.notFound().build());
    }

    private void applyExternalNodeDiskTelemetry(
            io.translab.tantor.server.dto.ClusterOverviewDto overview,
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes,
            UUID clusterId,
            List<io.translab.tantor.server.domain.DiscoveryAgent> linkedAgents,
            List<io.translab.tantor.server.domain.DiscoveryAgent> allAgents
    ) {
        if (overview.getBrokers() == null || overview.getBrokers().isEmpty()) {
            return;
        }

        Map<Integer, io.translab.tantor.server.domain.ExternalClusterNode> nodesById = new HashMap<>();
        for (io.translab.tantor.server.domain.ExternalClusterNode node : nodes) {
            if (Boolean.TRUE.equals(node.getIsBroker()) && node.getNodeId() != null) {
                nodesById.put(node.getNodeId(), node);
            }
        }

        for (io.translab.tantor.server.dto.ClusterOverviewDto.BrokerRow broker : overview.getBrokers()) {
            io.translab.tantor.server.domain.ExternalClusterNode node = nodesById.get(broker.getBrokerId());
            if (node == null) {
                continue;
            }
            broker.setHostDiskLastSeen(node.getLastSeen());
            boolean fresh = matchingFreshAgent(node.getHost(), clusterId, linkedAgents, allAgents).isPresent()
                    && node.getLastSeen() != null
                    && node.getLastSeen().isAfter(java.time.OffsetDateTime.now()
                            .minusSeconds(discoveryAgentFreshnessSeconds()));
            if (!fresh) {
                broker.setHostDiskUsedBytes(null);
                broker.setHostDiskTotalBytes(null);
                broker.setHostDiskMetricStatus(node.getLastSeen() == null ? "UNAVAILABLE" : "STALE");
                continue;
            }
            broker.setHostDiskUsedBytes(externalDiskBytes(node.getDiskUsedBytes(), node.getDiskUsedGb()));
            broker.setHostDiskTotalBytes(externalDiskBytes(node.getDiskTotalBytes(), node.getDiskTotalGb()));
            broker.setHostDiskMetricStatus("LIVE");
        }
    }

    private Long externalDiskBytes(Long exactBytes, Long legacyGiB) {
        if (exactBytes != null && exactBytes >= 0) {
            return exactBytes;
        }
        return legacyGiB == null ? null : gibibytesToBytes(legacyGiB);
    }

    private long gibibytesToBytes(long value) {
        if (value <= 0) {
            return 0L;
        }
        long gibibyte = 1024L * 1024L * 1024L;
        return value > Long.MAX_VALUE / gibibyte ? Long.MAX_VALUE : value * gibibyte;
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/deploy")
    public ResponseEntity<Map<String, String>> deployCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody DeployClusterRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CREATE_CLUSTER)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        ResponseEntity<Map<String, String>> validationError = validateDeployRequest(request);
        if (validationError != null) {
            return validationError;
        }

        String deploymentMode = normalizeDeploymentMode(request.getMode());
        Map<String, Object> deploymentConfig = buildDeploymentConfig(request, deploymentMode);
        String quorumVoters = String.valueOf(deploymentConfig.getOrDefault("quorum_voters", ""));
        String bootstrapServers = String.valueOf(deploymentConfig.getOrDefault("bootstrap_servers", ""));
        SchemaRegistryAddonReq schemaAddon = schemaAddon(request);
        KafkaConnectAddonReq connectAddon = connectAddon(request);
        
        // 1. Save Cluster to Database
        Cluster cluster = new Cluster();
        cluster.setName(request.getName());
        cluster.setKafkaVersion(request.getKafka_version());
        cluster.setMode(deploymentMode);
        cluster.setEnvironment(request.getEnvironment());
        cluster.setBootstrapServers(bootstrapServers);
        cluster.setOriginType("INTERNAL");
        cluster.setMonitoringEnabled(true);
        cluster.setJmxEnabled(true);
        cluster.setJmxExporterPort(DEFAULT_JMX_EXPORTER_PORT);
        cluster.setKafkaClusterId(blankString(deploymentConfig.get("cluster_uuid")));
        cluster.setInstallDirectory(blankString(deploymentConfig.get("kafka_install_dir")));
        cluster.setConfigDirectory(activeKafkaInstallDir(deploymentConfig) + "/config");
        cluster.setDataDirectory(blankString(deploymentConfig.get("kafka_data_dir")));
        cluster.setLogDirectory(blankString(deploymentConfig.get("kafka_app_log_dir")));
        String clusterRole = clusterRoleForServices(request.getServices());
        String username = roleAuthenticationUtil.extractUsername(authorization);
        cluster.setUser(username);
        cluster.setRole(clusterRole);
        cluster.setConfigPath(configFileForRole(clusterRole, deploymentMode, request.getKafka_version(), activeKafkaInstallDir(deploymentConfig)));
        cluster.setCreatedBy(username);
        cluster.setUpdatedBy(username);
        cluster.setNodeIds(request.getServices().stream()
                .map(ServiceAssignmentReq::getNode_id)
                .filter(java.util.Objects::nonNull)
                .distinct().sorted().toList());
        
        try {
            cluster.setConfigJson(objectMapper.writeValueAsString(deploymentConfig));
        } catch (Exception e) {
            cluster.setConfigJson("{}");
        }

        List<ClusterServiceAssignment> assignments = new ArrayList<>();
        for (ServiceAssignmentReq sa : request.getServices()) {
            ClusterServiceAssignment assign = new ClusterServiceAssignment();
            assign.setCluster(cluster);
            assign.setHostId(sa.getHost_id());
            assign.setRole(sa.getRole());
            assign.setNodeId(sa.getNode_id());
            assign.setJmxExporterPort(jmxPortForService(sa));
            assign.setConfigJson(buildServiceConfigJson(deploymentConfig, sa));
            assignments.add(assign);
        }
        cluster.setServices(assignments);
        clusterRepository.save(cluster);
        Map<String, Object> schemaRegistryPayload = null;
        if (schemaAddon != null) {
            schemaRegistryPayload = schemaAddonPayload(
                    schemaAddon,
                    cluster.getId(),
                    bootstrapServers,
                    activeKafkaInstallDir(deploymentConfig),
                    (int) request.getServices().stream().filter(service -> isBrokerRole(service.getRole())).count()
            );
            ClusterServiceAssignment schemaAssignment = new ClusterServiceAssignment();
            addDataServiceProtectedPaths(schemaRegistryPayload, deploymentConfig);
            if (connectAddon != null && schemaAddon.getHost_id().equals(connectAddon.getHost_id())) {
                addOtherServicePaths(schemaRegistryPayload, connectAddon.getInstall_dir(), connectAddon.getConfig_dir(),
                        connectAddon.getLog_dir(), connectAddon.getWorking_dir(), connectAddon.getPlugin_dir());
            }
            schemaAssignment.setCluster(cluster);
            schemaAssignment.setHostId(schemaAddon.getHost_id());
            schemaAssignment.setRole("schema_registry");
            schemaAssignment.setStatus("PENDING");
            schemaAssignment.setConfigJson(writeJson(schemaRegistryPayload));
            cluster.getServices().add(schemaAssignment);
            clusterRepository.save(cluster);
        }
        Map<String, Object> kafkaConnectPayload = null;
        if (connectAddon != null) {
            kafkaConnectPayload = connectAddonPayload(connectAddon, cluster.getId(), bootstrapServers,
                    activeKafkaInstallDir(deploymentConfig),
                    (int) request.getServices().stream().filter(service -> isBrokerRole(service.getRole())).count());
            ClusterServiceAssignment connectAssignment = new ClusterServiceAssignment();
            addDataServiceProtectedPaths(kafkaConnectPayload, deploymentConfig);
            if (schemaAddon != null && connectAddon.getHost_id().equals(schemaAddon.getHost_id())) {
                addOtherServicePaths(kafkaConnectPayload, schemaAddon.getInstall_dir(), schemaAddon.getConfig_dir(),
                        schemaAddon.getLog_dir(), schemaAddon.getWorking_dir(), null);
            }
            connectAssignment.setCluster(cluster);
            connectAssignment.setHostId(connectAddon.getHost_id());
            connectAssignment.setRole("kafka_connect");
            connectAssignment.setStatus("PENDING");
            connectAssignment.setConfigJson(writeJson(kafkaConnectPayload));
            cluster.getServices().add(connectAssignment);
            clusterRepository.save(cluster);
        }

        // Update host cluster_id references
        for (ServiceAssignmentReq sa : request.getServices()) {
            hostRepository.findById(sa.getHost_id()).ifPresent(host -> {
                host.setClusterId(cluster.getId());
                hostRepository.save(host);
            });
        }

        // 2. Dispatch tasks via Job Engine
        String finalArtifactUrl = resolveAgentArtifactUrl(request.getArtifactUrl());
        List<ServiceAssignmentReq> deployOrder = request.getServices().stream()
                .sorted((left, right) -> Boolean.compare(!isMetadataService(left.getRole()), !isMetadataService(right.getRole())))
                .toList();

        List<Map<String, Object>> deployOrderPayload = new ArrayList<>();
        for (ServiceAssignmentReq svc : deployOrder) {
            Map<String, Object> svcPayload = new HashMap<>();
            svcPayload.put("host_id", svc.getHost_id());
            svcPayload.put("role", svc.getRole());
            svcPayload.put("node_id", svc.getNode_id());
            svcPayload.put("operation", "deploy");
            svcPayload.put("serviceConfigJson", buildServiceConfigJson(deploymentConfig, svc));
            deployOrderPayload.add(svcPayload);
        }

        Map<String, Object> jobPayload = new HashMap<>();
        jobPayload.put("clusterId", cluster.getId().toString());
        jobPayload.put("deployOrder", deployOrderPayload);
        jobPayload.put("finalArtifactUrl", finalArtifactUrl);
        jobPayload.put("quorumVoters", quorumVoters);
        jobPayload.put("kafkaVersion", request.getKafka_version());
        jobPayload.put("requestedBy", username);
        if (schemaRegistryPayload != null) {
            jobPayload.put("schemaRegistry", schemaRegistryPayload);
        }
        if (kafkaConnectPayload != null) {
            jobPayload.put("kafkaConnect", kafkaConnectPayload);
        }

        Job job = new Job();
        job.setType(JobType.DEPLOYMENT);
        job.setStatus(JobStatus.PENDING);
        job.setRollbackSupported(true);
        job.setResourceKey("cluster:" + cluster.getId());
        job.setRequestedBy(username);
        try {
            job.setPayload(objectMapper.writeValueAsString(jobPayload));
        } catch (Exception e) {
            job.setPayload("{}");
        }
        List<JobStep> deploymentSteps = deploymentJobSteps(deployOrderPayload, deploymentConfig, deploymentMode);
        if (schemaAddon != null) {
            appendSchemaRegistrySteps(deploymentSteps, schemaAddon.getHost_id());
        }
        if (connectAddon != null) {
            appendKafkaConnectSteps(deploymentSteps, connectAddon.getHost_id());
        }
        Job savedJob = jobService.createJob(job, deploymentSteps);
        
        activityAlertService.logActivity("INFO", "Created deployment job for cluster: " + request.getName(), cluster.getId());
        
        return ResponseEntity.ok(Map.of(
            "id", cluster.getId().toString(),
            "jobId", savedJob.getId().toString()
        ));
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/{id}/services")
    public ResponseEntity<Map<String, String>> addServicesToCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID id, @RequestBody AddonsReq request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ADD_NODE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        Cluster cluster = clusterRepository.findForServiceUpdate(id).orElse(null);
        if (cluster == null) return ResponseEntity.notFound().build();
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode()) || !"SUCCESS".equalsIgnoreCase(cluster.getStatus())) {
            return ResponseEntity.badRequest().body(Map.of("error", "Services can only be added to a successfully deployed managed cluster."));
        }
        SchemaRegistryAddonReq schema = request.getSchema_registry();
        KafkaConnectAddonReq connect = request.getKafka_connect();
        if (schema == null && connect == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Select at least one service."));
        }
        Map<String, Object> config = parseConfigJson(cluster.getConfigJson());
        List<ServiceAssignmentReq> nodes = new ArrayList<>();
        for (ClusterServiceAssignment assignment : cluster.getServices()) {
            if ((schema != null && "schema_registry".equals(assignment.getRole()))
                    || (connect != null && "kafka_connect".equals(assignment.getRole()))) {
                return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", "A selected service has already been added. Check its existing deployment job."));
            }
            if (Set.of("schema_registry", "kafka_connect").contains(assignment.getRole())) continue;
            Map<String, Object> nodeConfig = new HashMap<>(config);
            nodeConfig.putAll(parseConfigJson(assignment.getConfigJson()));
            ServiceAssignmentReq node = new ServiceAssignmentReq();
            node.setHost_id(assignment.getHostId());
            node.setRole(assignment.getRole());
            node.setListener_port(parseIntConfig(nodeConfig.get("listener_port"), 9092));
            node.setController_port(parseIntConfig(nodeConfig.get("controller_port"), "zookeeper".equalsIgnoreCase(cluster.getMode()) ? 2181 : 9093));
            node.setJmx_port(assignment.getJmxExporterPort());
            node.setZookeeper_peer_port(parseIntConfig(nodeConfig.get("zookeeper_peer_port"), 2888));
            node.setZookeeper_election_port(parseIntConfig(nodeConfig.get("zookeeper_election_port"), 3888));
            nodes.add(node);
        }
        try {
            validateSchemaAddon(schema, nodes);
            validateConnectAddon(connect, nodes);
            if (schema != null && connect != null && schema.getHost_id().equals(connect.getHost_id())
                    && java.util.Objects.equals(schema.getPort(), connect.getPort())) {
                throw new IllegalArgumentException("Services on the same host must use different REST ports.");
            }
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
        if (cluster.getBootstrapServers() == null || cluster.getBootstrapServers().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "The cluster has no Kafka bootstrap servers."));
        }
        int brokers = (int) nodes.stream().filter(node -> isBrokerRole(node.getRole())).count();
        Map<String, Map<String, Object>> payloads = new LinkedHashMap<>();
        if (schema != null) payloads.put("schema_registry", schemaAddonPayload(schema, id,
                cluster.getBootstrapServers(), activeKafkaInstallDir(config), brokers));
        if (connect != null) payloads.put("kafka_connect", connectAddonPayload(connect, id,
                cluster.getBootstrapServers(), activeKafkaInstallDir(config), brokers));
        for (Map.Entry<String, Map<String, Object>> entry : payloads.entrySet()) {
            Map<String, Object> payload = entry.getValue();
            addDataServiceProtectedPaths(payload, config);
            List<Map<String, Object>> otherServices = new ArrayList<>();
            payloads.forEach((role, other) -> { if (!role.equals(entry.getKey())) otherServices.add(other); });
            cluster.getServices().stream().filter(service -> Set.of("schema_registry", "kafka_connect").contains(service.getRole()))
                    .map(service -> parseConfigJson(service.getConfigJson())).forEach(otherServices::add);
            for (Map<String, Object> other : otherServices) {
                if (!java.util.Objects.equals(payload.get("host_id"), other.get("host_id"))) continue;
                if (java.util.Objects.equals(payload.get("rest_port"), other.get("rest_port"))) {
                    return ResponseEntity.badRequest().body(Map.of("error", "The REST port conflicts with an existing service on this host."));
                }
                addOtherServicePaths(payload, blankString(other.get("install_dir")), blankString(other.get("config_dir")),
                        blankString(other.get("log_dir")), blankString(other.get("working_dir")), blankString(other.get("plugin_dir")));
            }
        }
        List<JobStep> steps = new ArrayList<>();
        Map<String, Object> jobPayload = new HashMap<>();
        jobPayload.put("clusterId", id.toString());
        jobPayload.put("servicesOnly", true);
        jobPayload.put("kafkaVersion", cluster.getKafkaVersion());
        String username = roleAuthenticationUtil.extractUsername(authorization);
        for (Map.Entry<String, Map<String, Object>> entry : payloads.entrySet()) {
            ClusterServiceAssignment assignment = new ClusterServiceAssignment();
            assignment.setCluster(cluster);
            assignment.setRole(entry.getKey());
            assignment.setHostId(String.valueOf(entry.getValue().get("host_id")));
            assignment.setStatus("PENDING");
            assignment.setConfigJson(writeJson(entry.getValue()));
            cluster.getServices().add(assignment);
            if ("schema_registry".equals(entry.getKey())) {
                jobPayload.put("schemaRegistry", entry.getValue());
                appendSchemaRegistrySteps(steps, assignment.getHostId());
            } else {
                jobPayload.put("kafkaConnect", entry.getValue());
                appendKafkaConnectSteps(steps, assignment.getHostId());
            }
        }
        cluster.setUpdatedBy(username);
        clusterRepository.save(cluster);
        Job job = new Job();
        job.setType(JobType.DEPLOYMENT);
        job.setStatus(JobStatus.PENDING);
        // These steps do not deploy Kafka nodes; cluster rollback must never remove existing brokers.
        job.setRollbackSupported(false);
        job.setResourceKey("cluster:" + id);
        job.setRequestedBy(username);
        job.setPayload(writeJson(jobPayload));
        Job saved = jobService.createJob(job, steps);
        activityAlertService.logActivity("INFO", "Created service deployment job for cluster: " + cluster.getName(), id);
        return ResponseEntity.ok(Map.of("id", id.toString(), "jobId", saved.getId().toString()));
    }

    @PostMapping("/validate-kraft")
    public ResponseEntity<Map<String, Object>> validateKraftTopology(@RequestBody DeployClusterRequest request) {
        if (!"kraft".equals(normalizeDeploymentMode(request.getMode()))) {
            return ResponseEntity.badRequest().body(Map.of("errors", List.of("KRaft validation requires KRaft mode.")));
        }
        if (request.getServices() == null || request.getServices().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("errors", List.of("Select at least one service.")));
        }
        Map<String, Object> config = buildDeploymentConfig(request, "kraft");
        Map<String, Object> report = kraftValidationReport(request, config);
        report.put("generatedConfig", Map.of(
                "cluster_uuid", String.valueOf(config.get("cluster_uuid")),
                "kraft_quorum_mode", String.valueOf(config.get("kraft_quorum_mode")),
                "quorum_voters", String.valueOf(config.getOrDefault("quorum_voters", "")),
                "quorum_bootstrap_servers", String.valueOf(config.getOrDefault("quorum_bootstrap_servers", "")),
                "initial_controllers", String.valueOf(config.getOrDefault("initial_controllers", ""))
        ));
        return ResponseEntity.ok(report);
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/{id}/nodes")
    public ResponseEntity<Map<String, String>> addNodesToCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID id,
            @RequestBody DeployClusterRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ADD_NODE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        java.util.Optional<Cluster> optionalCluster = clusterRepository.findById(id);
        if (optionalCluster.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        Cluster cluster = optionalCluster.get();
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            return ResponseEntity.badRequest().body(Map.of("error", "Use Existing cluster flow for external clusters."));
        }
        if (request.getServices() == null || request.getServices().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Select at least one node to add."));
        }

        String deploymentMode = normalizeDeploymentMode(cluster.getMode());
        ResponseEntity<Map<String, String>> validationError = validateAddNodeRequest(cluster, request, deploymentMode);
        if (validationError != null) {
            return validationError;
        }

        List<ServiceAssignmentReq> allServices = new ArrayList<>();
        if (cluster.getServices() != null) {
            for (ClusterServiceAssignment existing : cluster.getServices()) {
                ServiceAssignmentReq svc = new ServiceAssignmentReq();
                svc.setHost_id(existing.getHostId());
                svc.setRole(existing.getRole());
                svc.setNode_id(existing.getNodeId());
                svc.setJmx_port(existing.getJmxExporterPort());
                allServices.add(svc);
            }
        }
        allServices.addAll(request.getServices());

        DeployClusterRequest mergedRequest = new DeployClusterRequest();
        mergedRequest.setName(cluster.getName());
        mergedRequest.setKafka_version(cluster.getKafkaVersion());
        mergedRequest.setMode(deploymentMode);
        mergedRequest.setEnvironment(cluster.getEnvironment());
        mergedRequest.setArtifactUrl(request.getArtifactUrl());
        Map<String, Object> mergedConfig = new HashMap<>(parseConfigJson(cluster.getConfigJson()));
        if (cluster.getKafkaClusterId() != null && !cluster.getKafkaClusterId().isBlank()) {
            mergedConfig.put("cluster_uuid", cluster.getKafkaClusterId());
        }
        if (request.getConfig() != null) {
            mergedConfig.putAll(request.getConfig());
        }
        mergedRequest.setConfig(mergedConfig);
        mergedRequest.setServices(allServices);

        Map<String, Object> deploymentConfig = buildDeploymentConfig(mergedRequest, deploymentMode);
        DeployClusterRequest persistedRequest = new DeployClusterRequest();
        persistedRequest.setName(cluster.getName());
        persistedRequest.setKafka_version(cluster.getKafkaVersion());
        persistedRequest.setMode(deploymentMode);
        persistedRequest.setEnvironment(cluster.getEnvironment());
        Map<String, Object> persistedConfig = new HashMap<>(parseConfigJson(cluster.getConfigJson()));
        if (cluster.getKafkaClusterId() != null && !cluster.getKafkaClusterId().isBlank()) {
            persistedConfig.put("cluster_uuid", cluster.getKafkaClusterId());
        }
        persistedRequest.setConfig(persistedConfig);
        persistedRequest.setServices(allServices);
        Map<String, Object> persistedClusterConfig = buildDeploymentConfig(persistedRequest, deploymentMode);
        String quorumVoters = String.valueOf(deploymentConfig.getOrDefault("quorum_voters", ""));
        String finalArtifactUrl = resolveAgentArtifactUrl(request.getArtifactUrl());

        if (cluster.getServices() == null) {
            cluster.setServices(new ArrayList<>());
        }
        for (ServiceAssignmentReq sa : request.getServices()) {
            ClusterServiceAssignment assign = new ClusterServiceAssignment();
            assign.setCluster(cluster);
            assign.setHostId(sa.getHost_id());
            assign.setRole(sa.getRole());
            assign.setNodeId(sa.getNode_id());
            assign.setJmxExporterPort(jmxPortForService(sa));
            assign.setConfigJson(buildServiceConfigJson(deploymentConfig, sa));
            cluster.getServices().add(assign);
        }
        cluster.setStatus("RUNNING");
        String username = roleAuthenticationUtil.extractUsername(authorization);
        cluster.setUpdatedBy(username);
        cluster.setNodeIds(allServices.stream()
                .map(ServiceAssignmentReq::getNode_id)
                .filter(java.util.Objects::nonNull)
                .distinct().sorted().toList());
        cluster.setBootstrapServers(String.valueOf(persistedClusterConfig.getOrDefault("bootstrap_servers", cluster.getBootstrapServers())));
        String clusterRole = clusterRoleForServices(allServices);
        cluster.setRole(clusterRole);
        cluster.setConfigPath(configFileForRole(clusterRole, deploymentMode, cluster.getKafkaVersion(), activeKafkaInstallDir(persistedClusterConfig)));
        try {
            cluster.setConfigJson(objectMapper.writeValueAsString(persistedClusterConfig));
        } catch (Exception e) {
            // keep existing config if serialization fails
        }
        clusterRepository.save(cluster);
        auditService.record("CLUSTER_CHANGE", "CLUSTER_NODES_ADDED", "CLUSTER", cluster.getId().toString(),
                cluster.getId(), "SUCCESS", null, null, null,
                Map.of("nodeIds", request.getServices().stream().map(ServiceAssignmentReq::getNode_id).toList(),
                        "updatedBy", cluster.getUpdatedBy()));

        for (ServiceAssignmentReq sa : request.getServices()) {
            hostRepository.findById(sa.getHost_id()).ifPresent(host -> {
                host.setClusterId(cluster.getId());
                hostRepository.save(host);
            });
        }

        List<ServiceAssignmentReq> deployOrder = request.getServices().stream()
                .sorted((left, right) -> Boolean.compare(!isMetadataService(left.getRole()), !isMetadataService(right.getRole())))
                .toList();
        List<Map<String, Object>> deployOrderPayload = new ArrayList<>();
        for (ServiceAssignmentReq svc : deployOrder) {
            Map<String, Object> svcPayload = new HashMap<>();
            svcPayload.put("host_id", svc.getHost_id());
            svcPayload.put("role", svc.getRole());
            svcPayload.put("node_id", svc.getNode_id());
            svcPayload.put("operation", "deploy");
            svcPayload.put("serviceConfigJson", buildServiceConfigJson(deploymentConfig, svc));
            deployOrderPayload.add(svcPayload);
        }

        Map<String, Object> jobPayload = new HashMap<>();
        jobPayload.put("clusterId", cluster.getId().toString());
        jobPayload.put("finalArtifactUrl", finalArtifactUrl);
        jobPayload.put("quorumVoters", quorumVoters);
        jobPayload.put("kafkaVersion", cluster.getKafkaVersion());
        jobPayload.put("addNode", true);

        Job job = new Job();
        job.setType(JobType.ADD_HOST);
        job.setStatus(JobStatus.PENDING);
        job.setRollbackSupported(true);
        job.setResourceKey("cluster:" + cluster.getId());
        try {
            job.setPayload(objectMapper.writeValueAsString(jobPayload));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create add-node job payload", e);
        }
        Job savedJob = jobService.createJob(job, deploymentJobSteps(deployOrderPayload, deploymentConfig, deploymentMode));

        activityAlertService.logActivity("INFO", "Created add-node job for cluster: " + cluster.getName(), cluster.getId());
        return ResponseEntity.ok(Map.of(
                "id", cluster.getId().toString(),
                "jobId", savedJob.getId().toString(),
                "status", "scheduled"
        ));
    }

    @PostMapping("/external")
    public ResponseEntity<?> addExternalCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestBody ExternalClusterRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CREATE_CLUSTER)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        io.translab.tantor.server.service.ExternalClusterService.ExternalDiscoveryReport report =
                new io.translab.tantor.server.service.ExternalClusterService.ExternalDiscoveryReport();
        report.setName(request.getName());
        report.setEnvironment(request.getEnvironment());
        report.setBootstrapServers(request.getBootstrapServers());
        report.setKafkaVersion(request.getKafkaVersion());
        report.setKafkaClusterId(request.getKafkaClusterId());
        report.setKafkaMode(request.getKafkaMode());
        report.setSecurity(request.getSecurity());
        report.setBrokerCount(request.getBrokerCount());
        report.setNodeId(request.getNodeId());
        report.setRunning(request.isRunning());
        report.setInstallPath(request.getInstallPath());
        report.setLogDirs(request.getLogDirs());
        report.setHostname(request.getHostname());
        return ResponseEntity.ok(externalClusterService.recordDiscoveryReport(report));
    }

    @org.springframework.transaction.annotation.Transactional
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable java.util.UUID id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.DELETE_CLUSTER)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        java.util.Optional<ExternalCluster> external = externalClusterRepository.findById(id);
        if (external.isPresent()) {
            ExternalCluster cluster = external.get();
            recordExternalClusterDeletion(cluster, "EXTERNAL_CLUSTER_DELETED");
            externalClusterService.deleteExternalCluster(id);
            activityAlertService.logActivity("INFO", "Deleted external cluster", id);
            return ResponseEntity.ok().build();
        }

        java.util.Optional<Cluster> existing = clusterRepository.findById(id);
        if (existing.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        Cluster cluster = existing.get();
        if ("DELETING".equalsIgnoreCase(cluster.getStatus())) {
            return ResponseEntity.accepted().build();
        }
        boolean cleanupScheduled = !"EXTERNAL".equalsIgnoreCase(cluster.getMode())
                && initiateClusterCleanup(cluster);
        if (cleanupScheduled) {
            activityAlertService.logActivity("INFO", "Deleting cluster; waiting for VM cleanup", id);
            return ResponseEntity.accepted().build();
        }
        hardDeleteCluster(cluster, "CLUSTER_DELETED", false);
        activityAlertService.logActivity("INFO",
                "Deleted cluster with no VM cleanup required", id);
        return ResponseEntity.ok().build();
    }

    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/force-delete/{id}")
    public ResponseEntity<Void> forceDeleteCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable java.util.UUID id) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.DELETE_CLUSTER)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        java.util.Optional<ExternalCluster> external = externalClusterRepository.findById(id);
        if (external.isPresent()) {
            ExternalCluster cluster = external.get();
            recordExternalClusterDeletion(cluster, "EXTERNAL_CLUSTER_FORCE_DELETED");
            externalClusterService.deleteExternalCluster(id);
            activityAlertService.logActivity("INFO", "Force-deleted external cluster", id);
            return ResponseEntity.ok().build();
        }

        java.util.Optional<Cluster> existing = clusterRepository.findById(id);
        if (existing.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        Cluster cluster = existing.get();
        boolean cleanupScheduled = cluster.getServices() != null && !cluster.getServices().isEmpty()
                && !"EXTERNAL".equalsIgnoreCase(cluster.getMode())
                && initiateClusterCleanup(cluster);
        hardDeleteCluster(cluster, "CLUSTER_FORCE_DELETED", cleanupScheduled);
        activityAlertService.logActivity("WARN",
                cleanupScheduled ? "Force-deleted cluster after dispatching VM cleanup" : "Force-deleted cluster", id);
        return ResponseEntity.ok().build();
    }
    @org.springframework.transaction.annotation.Transactional
    @PostMapping("/{id}/upgrade")
    public ResponseEntity<Map<String, String>> upgradeCluster(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable java.util.UUID id,
            @RequestBody UpgradeClusterRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_CHANGE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        java.util.Optional<Cluster> optionalCluster = clusterRepository.findById(id);
        if (optionalCluster.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        Cluster cluster = optionalCluster.get();
        String targetVersion = request == null ? null : request.getTargetVersion();
        if (targetVersion == null || targetVersion.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Target Kafka version is required."));
        }
        targetVersion = targetVersion.trim();

        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            return ResponseEntity.badRequest().body(Map.of("error", "External clusters cannot be upgraded by Tantor."));
        }
        if (!"SUCCESS".equalsIgnoreCase(cluster.getStatus())) {
            return ResponseEntity.badRequest().body(Map.of("error", "Cluster must be active before upgrade. Current status: " + cluster.getStatus() + "."));
        }
        if (targetVersion.equals(cluster.getKafkaVersion())) {
            return ResponseEntity.badRequest().body(Map.of("error", "Cluster is already on Kafka " + targetVersion + "."));
        }
        if ("zookeeper".equalsIgnoreCase(cluster.getMode()) && !isZooKeeperSupported(targetVersion)) {
            return ResponseEntity.badRequest().body(Map.of("error", "ZooKeeper deployments are not supported for Kafka 4.0.0 and newer."));
        }
        if (cluster.getServices() == null || cluster.getServices().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Cluster has no host assignments."));
        }

        for (ClusterServiceAssignment service : cluster.getServices()) {
            String hostId = service.getHostId();
            io.translab.tantor.server.domain.Host host = hostRepository.findById(hostId).orElse(null);
            if (host == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "Host " + hostId + " was not found."));
            }
            String effectiveStatus = hostStatusService.effectiveStatus(host);
            if (!"ONLINE".equalsIgnoreCase(effectiveStatus)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Host " + hostId + " is not online. Current status: " + effectiveStatus + "."));
            }
            String finalTargetVersion = targetVersion;
            boolean activeOnHost = hostParcelRepository.findLatestActive(hostId, "KAFKA").stream()
                    .anyMatch(parcel -> finalTargetVersion.equals(parcel.getVersion()));
            if (!activeOnHost) {
                return ResponseEntity.badRequest().body(Map.of("error", "Kafka " + targetVersion + " must be active as a parcel on host " + hostId + " before upgrade."));
            }
        }

        String previousVersion = cluster.getKafkaVersion();
        cluster.setStatus("RUNNING");
        clusterRepository.save(cluster);

        for (ClusterServiceAssignment service : cluster.getServices()) {
            deploymentService.upgradeKafkaOnHost(
                cluster.getId(),
                service.getHostId(),
                previousVersion,
                targetVersion,
                service.getNodeId() == null ? "1" : String.valueOf(service.getNodeId()),
                service.getRole(),
                service.getConfigJson() == null || service.getConfigJson().isBlank()
                        ? cluster.getConfigJson()
                        : service.getConfigJson()
            );
        }

        activityAlertService.logActivity("INFO", "Initialized Kafka upgrade to " + targetVersion + " for cluster: " + cluster.getName(), cluster.getId());
        return ResponseEntity.ok(Map.of("status", "scheduled", "targetVersion", targetVersion));
    }

    private List<JobStep> deploymentJobSteps(
            List<Map<String, Object>> deploymentOrder,
            Map<String, Object> deploymentConfig,
            String deploymentMode
    ) {
        List<JobStep> steps = new ArrayList<>();
        if (!"kraft".equals(deploymentMode)) {
            for (Map<String, Object> service : deploymentOrder) {
                if (isZooKeeperRole(String.valueOf(service.get("role")))) {
                    steps.add(deploymentStep(steps.size() + 1, service));
                }
            }

            String zookeeperConnect = String.valueOf(deploymentConfig.getOrDefault("zookeeper_connect", ""));
            String firstZkHost = firstZooKeeperHost(deploymentConfig);
            if (!firstZkHost.isBlank()) {
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("operation", "verify_zk_quorum");
                payload.put("host_id", firstZkHost);
                payload.put("zookeeper_connect", zookeeperConnect);
                payload.put("serviceConfigJson", writeJson(deploymentConfig));
                steps.add(jobStep(
                        steps.size() + 1,
                        firstZkHost,
                        "Verify ZooKeeper quorum",
                        payload
                ));
            }

            for (Map<String, Object> service : deploymentOrder) {
                if (isBrokerRole(String.valueOf(service.get("role")))) {
                    steps.add(deploymentStep(steps.size() + 1, service));
                }
            }
            return steps;
        }

        for (Map<String, Object> service : deploymentOrder) {
            if (isControllerRole(String.valueOf(service.get("role")))) {
                steps.add(deploymentStep(steps.size() + 1, service));
            }
        }

        String controllerEndpoints = String.valueOf(deploymentConfig.getOrDefault("controller_endpoints", ""));
        Set<String> connectivityHosts = new HashSet<>();
        for (Map<String, Object> service : deploymentOrder) {
            String hostId = String.valueOf(service.get("host_id"));
            if (!connectivityHosts.add(hostId)) continue;
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("operation", "connectivity");
            payload.put("host_id", hostId);
            payload.put("controller_endpoints", controllerEndpoints);
            steps.add(jobStep(
                    steps.size() + 1,
                    hostId,
                    "Check controller connectivity from " + hostId,
                    payload
            ));
        }

        for (Map<String, Object> service : deploymentOrder) {
            if (!isControllerRole(String.valueOf(service.get("role")))) {
                steps.add(deploymentStep(steps.size() + 1, service));
            }
        }

        String controllerHost = firstControllerHost(deploymentConfig);
        if (!controllerHost.isBlank()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("operation", "verify_quorum");
            payload.put("host_id", controllerHost);
            payload.put("controller_endpoints", controllerEndpoints);
            payload.put("cluster_uuid", String.valueOf(deploymentConfig.getOrDefault("cluster_uuid", "")));
            payload.put("expected_controller_count", String.valueOf(controllerCount(deploymentConfig)));
            payload.put("serviceConfigJson", writeJson(deploymentConfig));
            steps.add(jobStep(
                    steps.size() + 1,
                    controllerHost,
                    "Verify KRaft leader and cluster identity",
                    payload
            ));
        }
        return steps;
    }

    private void appendSchemaRegistrySteps(List<JobStep> steps, String hostId) {
        for (Map.Entry<String, String> entry : List.of(
                Map.entry("precheck_schema", "Pre-check Schema Registry"),
                Map.entry("create_schema_topic", "Create or verify _schemas topic"),
                Map.entry("install_schema", "Install Schema Registry"),
                Map.entry("verify_schema", "Verify Schema Registry REST API"),
                Map.entry("save_schema_connection", "Save Schema Registry connection"))) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("operation", entry.getKey());
            payload.put("host_id", hostId);
            steps.add(jobStep(steps.size() + 1, hostId, entry.getValue(), payload));
        }
    }

    private void addDataServiceProtectedPaths(Map<String, Object> payload, Map<String, Object> configuration) {
        payload.put("kafka_data_dir", configuration.getOrDefault("data_dir", "/data/kafka"));
        payload.put("kafka_log_dir", configuration.getOrDefault("log_dir", "/var/log/kafka"));
    }

    private void addOtherServicePaths(Map<String, Object> payload, String install, String config,
                                      String log, String working, String plugin) {
        payload.put("other_install_dir", install);
        payload.put("other_config_dir", config);
        payload.put("other_log_dir", log);
        payload.put("other_working_dir", working);
        if (plugin != null) payload.put("other_plugin_dir", plugin);
    }

    private void appendKafkaConnectSteps(List<JobStep> steps, String hostId) {
        for (Map.Entry<String, String> entry : List.of(
                Map.entry("precheck_connect", "Pre-check Kafka Connect"),
                Map.entry("create_connect_topics", "Create or verify Kafka Connect internal topics"),
                Map.entry("install_connect", "Install Kafka Connect"),
                Map.entry("verify_connect", "Verify Kafka Connect REST API"),
                Map.entry("save_connect_connection", "Save Kafka Connect connection"))) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("operation", entry.getKey());
            payload.put("host_id", hostId);
            steps.add(jobStep(steps.size() + 1, hostId, entry.getValue(), payload));
        }
    }

    private SchemaRegistryAddonReq schemaAddon(DeployClusterRequest request) {
        if (request.getAddons() == null || request.getAddons().getSchema_registry() == null
                || !request.getAddons().getSchema_registry().isEnabled()) {
            return null;
        }
        return request.getAddons().getSchema_registry();
    }

    private KafkaConnectAddonReq connectAddon(DeployClusterRequest request) {
        if (request.getAddons() == null || request.getAddons().getKafka_connect() == null
                || !request.getAddons().getKafka_connect().isEnabled()) {
            return null;
        }
        return request.getAddons().getKafka_connect();
    }

    private void validateSchemaAddon(SchemaRegistryAddonReq addon, List<ServiceAssignmentReq> services) {
        if (addon == null) return;
        if (addon.getHost_id() == null || addon.getHost_id().isBlank()) {
            throw new IllegalArgumentException("Schema Registry host is required.");
        }
        if (services == null || services.stream().noneMatch(service -> addon.getHost_id().equals(service.getHost_id()))) {
            throw new IllegalArgumentException("Schema Registry must be assigned to a selected Kafka node.");
        }
        if (addon.getArtifact_id() == null || addon.getArtifact_id().isBlank()) {
            throw new IllegalArgumentException("Schema Registry artifact is required.");
        }
        validateAddonHeap(addon.getHeap_size(), "Schema Registry");
        validateDataServiceAgent(addon.getHost_id());
        int port = addon.getPort() == null ? 8081 : addon.getPort();
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Schema Registry port must be between 1 and 65535.");
        }
        Set<Integer> reservedPorts = services.stream()
                .filter(service -> addon.getHost_id().equals(service.getHost_id()))
                .flatMap(service -> java.util.stream.Stream.of(
                        service.getListener_port(), service.getController_port(), service.getJmx_port(),
                        service.getZookeeper_peer_port(), service.getZookeeper_election_port()))
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toSet());
        if (reservedPorts.contains(port)) {
            throw new IllegalArgumentException("Schema Registry port " + port
                    + " conflicts with another service port on the selected host.");
        }
        for (String path : new String[]{addon.getInstall_dir(), addon.getConfig_dir(), addon.getLog_dir(), addon.getWorking_dir()}) {
            if (path == null || !path.startsWith("/") || "/".equals(path) || path.contains("..")) {
                throw new IllegalArgumentException("Schema Registry paths must be absolute, non-root paths without '..'.");
            }
        }
        String compatibility = String.valueOf(addon.getCompatibility_level()).toUpperCase(Locale.ROOT);
        if (!Set.of("BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE",
                "FULL", "FULL_TRANSITIVE", "NONE").contains(compatibility)) {
            throw new IllegalArgumentException("Unsupported Schema Registry compatibility level.");
        }
    }

    private void validateConnectAddon(KafkaConnectAddonReq addon, List<ServiceAssignmentReq> services) {
        if (addon == null) return;
        if (addon.getHost_id() == null || addon.getHost_id().isBlank()) {
            throw new IllegalArgumentException("Kafka Connect host is required.");
        }
        if (services == null || services.stream().noneMatch(service -> addon.getHost_id().equals(service.getHost_id()))) {
            throw new IllegalArgumentException("Kafka Connect must be assigned to a selected Kafka node.");
        }
        if (addon.getArtifact_id() == null || addon.getArtifact_id().isBlank()) {
            throw new IllegalArgumentException("Kafka Connect artifact is required.");
        }
        validateAddonHeap(addon.getHeap_size(), "Kafka Connect");
        validateDataServiceAgent(addon.getHost_id());
        int port = addon.getPort() == null ? 8083 : addon.getPort();
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Kafka Connect port must be between 1 and 65535.");
        }
        Set<Integer> reservedPorts = services.stream()
                .filter(service -> addon.getHost_id().equals(service.getHost_id()))
                .flatMap(service -> java.util.stream.Stream.of(service.getListener_port(), service.getController_port(),
                        service.getJmx_port(), service.getZookeeper_peer_port(), service.getZookeeper_election_port()))
                .filter(java.util.Objects::nonNull).collect(Collectors.toSet());
        if (reservedPorts.contains(port)) {
            throw new IllegalArgumentException("Kafka Connect port " + port + " conflicts with another service port on the selected host.");
        }
        for (String path : new String[]{addon.getInstall_dir(), addon.getConfig_dir(), addon.getLog_dir(),
                addon.getWorking_dir(), addon.getPlugin_dir()}) {
            if (path == null || !path.startsWith("/") || "/".equals(path) || path.contains("..")) {
                throw new IllegalArgumentException("Kafka Connect paths must be absolute, non-root paths without '..'.");
            }
        }
        if (addon.getGroup_id() == null || !addon.getGroup_id().matches("[A-Za-z0-9._-]{1,200}")) {
            throw new IllegalArgumentException("Kafka Connect worker group ID must contain 1–200 letters, numbers, dots, underscores or hyphens.");
        }
    }

    private void validateAddonHeap(String heap, String service) {
        if (heap == null || !heap.matches("[1-9][0-9]*[mMgG]")) {
            throw new IllegalArgumentException(service + " heap size must be a positive size such as 512M or 1G.");
        }
    }

    private void validateDataServiceAgent(String hostId) {
        hostRepository.findById(hostId).ifPresent(host -> {
            String blocked = io.translab.tantor.server.service.DataServiceAgentSupport.precheckBlockReason(host.getAgentVersion());
            if (blocked != null) throw new IllegalArgumentException(blocked);
        });
    }

    private Map<String, Object> schemaAddonPayload(SchemaRegistryAddonReq addon, UUID clusterId,
                                                    String bootstrapServers, String kafkaInstallDir,
                                                    int brokerCount) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("host_id", addon.getHost_id());
        payload.put("host_name", hostRepository.findById(addon.getHost_id())
                .map(host -> host.getHostIp() == null || host.getHostIp().isBlank()
                        ? host.getHostname() : host.getHostIp())
                .orElse(addon.getHost_id()));
        payload.put("artifact_id", addon.getArtifact_id());
        payload.put("artifact_url", addon.getArtifact_url() == null || addon.getArtifact_url().isBlank()
                ? joinArtifactRepoBase("/api/v1/artifacts/" + addon.getArtifact_id() + "/download")
                : resolveAgentArtifactUrl(addon.getArtifact_url()));
        payload.put("checksum", addon.getChecksum() == null ? "" : addon.getChecksum());
        payload.put("schema_version", addon.getVersion() == null ? "" : addon.getVersion());
        payload.put("rest_port", addon.getPort() == null ? 8081 : addon.getPort());
        payload.put("install_dir", addon.getInstall_dir());
        payload.put("config_dir", addon.getConfig_dir());
        payload.put("log_dir", addon.getLog_dir());
        payload.put("working_dir", addon.getWorking_dir());
        payload.put("kafka_install_dir", kafkaInstallDir);
        payload.put("heap_size", addon.getHeap_size());
        payload.put("compatibility_level", String.valueOf(addon.getCompatibility_level()).toUpperCase(Locale.ROOT));
        payload.put("bootstrap_servers", schemaBootstrapServers(bootstrapServers));
        payload.put("kafkastore_topic", "_schemas");
        payload.put("schemas_topic", "_schemas");
        payload.put("replication_factor", Math.max(1, Math.min(3, brokerCount)));
        payload.put("group_id", "tantor-sr-" + clusterId);
        payload.put("service_user", "root");
        return payload;
    }

    private Map<String, Object> connectAddonPayload(KafkaConnectAddonReq addon, UUID clusterId,
                                                     String bootstrapServers, String kafkaInstallDir,
                                                     int brokerCount) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("host_id", addon.getHost_id());
        payload.put("host_name", hostRepository.findById(addon.getHost_id())
                .map(host -> host.getHostIp() == null || host.getHostIp().isBlank() ? host.getHostname() : host.getHostIp())
                .orElse(addon.getHost_id()));
        payload.put("artifact_id", addon.getArtifact_id());
        payload.put("artifact_url", addon.getArtifact_url() == null || addon.getArtifact_url().isBlank()
                ? joinArtifactRepoBase("/api/v1/artifacts/" + addon.getArtifact_id() + "/download")
                : resolveAgentArtifactUrl(addon.getArtifact_url()));
        payload.put("checksum", addon.getChecksum() == null ? "" : addon.getChecksum());
        payload.put("connect_version", addon.getVersion() == null ? "" : addon.getVersion());
        payload.put("rest_port", addon.getPort() == null ? 8083 : addon.getPort());
        payload.put("install_dir", addon.getInstall_dir());
        payload.put("config_dir", addon.getConfig_dir());
        payload.put("log_dir", addon.getLog_dir());
        payload.put("working_dir", addon.getWorking_dir());
        payload.put("plugin_dir", addon.getPlugin_dir());
        payload.put("kafka_install_dir", kafkaInstallDir);
        payload.put("heap_size", addon.getHeap_size());
        payload.put("bootstrap_servers", kafkaClientBootstrapServers(bootstrapServers));
        payload.put("group_id", addon.getGroup_id() + "-" + clusterId);
        payload.put("offset_topic", "connect-offsets");
        payload.put("config_topic", "connect-configs");
        payload.put("status_topic", "connect-status");
        payload.put("min_free_disk_mb", 5120);
        payload.put("replication_factor", Math.max(1, Math.min(3, brokerCount)));
        payload.put("service_user", "root");
        return payload;
    }

    private String schemaBootstrapServers(String bootstrapServers) {
        return Arrays.stream(String.valueOf(bootstrapServers).split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(value -> value.contains("://") ? value : "PLAINTEXT://" + value)
                .collect(Collectors.joining(","));
    }

    private String kafkaClientBootstrapServers(String bootstrapServers) {
        return Arrays.stream(String.valueOf(bootstrapServers).split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(value -> value.contains("://") ? value.substring(value.indexOf("://") + 3) : value)
                .collect(Collectors.joining(","));
    }

    private JobStep deploymentStep(int order, Map<String, Object> service) {
        return jobStep(
                order,
                String.valueOf(service.get("host_id")),
                "Deploy " + service.get("role") + " node " + service.get("node_id")
                        + " on " + service.get("host_id"),
                service
        );
    }

    private JobStep jobStep(int order, String targetId, String name, Map<String, Object> payload) {
        JobStep step = new JobStep();
        step.setStepOrder(order);
        step.setTargetId(targetId);
        step.setName(name);
        step.setPayload(writeJson(payload));
        return step;
    }

    private String firstControllerHost(Map<String, Object> deploymentConfig) {
        Object topologyValue = deploymentConfig.get("service_topology");
        if (!(topologyValue instanceof List<?> topology)) return "";
        for (Object itemValue : topology) {
            if (!(itemValue instanceof Map<?, ?> item)) continue;
            if (isControllerRole(String.valueOf(item.get("role")))) {
                return String.valueOf(item.get("hostId"));
            }
        }
        return "";
    }

    private String firstZooKeeperHost(Map<String, Object> deploymentConfig) {
        Object topologyValue = deploymentConfig.get("service_topology");
        if (!(topologyValue instanceof List<?> topology)) return "";
        for (Object itemValue : topology) {
            if (!(itemValue instanceof Map<?, ?> item)) continue;
            if (isZooKeeperRole(String.valueOf(item.get("role")))) {
                return String.valueOf(item.get("hostId"));
            }
        }
        return "";
    }

    private long controllerCount(Map<String, Object> deploymentConfig) {
        Object topologyValue = deploymentConfig.get("service_topology");
        if (!(topologyValue instanceof List<?> topology)) return 0;
        return topology.stream()
                .filter(itemValue -> itemValue instanceof Map<?, ?>)
                .map(itemValue -> (Map<?, ?>) itemValue)
                .filter(item -> isControllerRole(String.valueOf(item.get("role"))))
                .count();
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize deployment job step", e);
        }
    }

    @Data
    static class DeployClusterRequest {
        private String name;
        private String kafka_version;
        private String mode;
        private List<ServiceAssignmentReq> services;
        private Map<String, Object> config;
        private String environment;
        private String artifactUrl;
        private boolean acknowledge_kraft_risk;
        private AddonsReq addons;
    }

    @Data
    static class AddonsReq {
        private SchemaRegistryAddonReq schema_registry;
        private KafkaConnectAddonReq kafka_connect;
    }

    @Data
    static class SchemaRegistryAddonReq {
        private boolean enabled;
        private String host_id;
        private String artifact_id;
        private String artifact_url;
        private String checksum;
        private String version;
        private Integer port = 8081;
        private String install_dir = "/opt/tantor/schema-registry";
        private String config_dir = "/opt/tantor/schema-registry/etc/schema-registry";
        private String log_dir = "/var/log/tantor/schema-registry";
        private String working_dir = "/var/lib/tantor/schema-registry";
        private String heap_size = "1G";
        private String compatibility_level = "BACKWARD";
    }

    @Data
    static class KafkaConnectAddonReq {
        private boolean enabled;
        private String host_id;
        private String artifact_id;
        private String artifact_url;
        private String checksum;
        private String version;
        private Integer port = 8083;
        private String install_dir = "/opt/tantor/kafka-connect";
        private String config_dir = "/opt/tantor/kafka-connect/config";
        private String log_dir = "/var/log/tantor/kafka-connect";
        private String working_dir = "/var/lib/tantor/kafka-connect";
        private String plugin_dir = "/opt/tantor/kafka-connect/plugins";
        private String heap_size = "1G";
        private String group_id = "tantor-connect";
    }

    @Data
    static class ExternalClusterRequest {
        private String name;
        private String environment;
        private String bootstrapServers;
        private String kafkaVersion;
        private String kafkaClusterId;
        private String kafkaMode;
        private String security;
        private int brokerCount = 1;
        private Integer nodeId;
        private boolean isRunning = true;
        private String installPath;
        private String logDirs;
        private String hostname;
    }

    @Data
    static class UpdateClusterRequest {
        private String name;
        private String environment;
    }

    @Data
    static class UpgradeClusterRequest {
        private String targetVersion;
    }

    @Data
    static class ServiceAssignmentReq {
        private String host_id;
        private String role;
        private Integer node_id;
        private String configuration_mode;
        private String properties_template;
        private String heap_size;
        private String heap_xms;
        private String heap_xmx;
        private Integer listener_port;
        private Integer controller_port;
        private Integer jmx_port;
        private Integer zookeeper_peer_port;
        private Integer zookeeper_election_port;
    }

    static String heapRangeError(String heapSize, String heapXms, String heapXmx) {
        if ((heapSize == null || heapSize.isBlank())
                && (heapXms == null || heapXms.isBlank())
                && (heapXmx == null || heapXmx.isBlank())) {
            return null;
        }
        if (heapSize != null && !heapSize.isBlank() && heapSizeMiB(heapSize) == null) {
            return "Heap size must be a positive size such as 512M or 1G.";
        }
        String min = heapXms == null || heapXms.isBlank() ? heapSize : heapXms;
        String max = heapXmx == null || heapXmx.isBlank() ? heapSize : heapXmx;
        Long minMiB = heapSizeMiB(min);
        Long maxMiB = heapSizeMiB(max);
        if (minMiB == null || maxMiB == null) {
            return "Min Heap and Max Heap must be positive sizes such as 512M or 1G.";
        }
        return minMiB > maxMiB ? "Min Heap cannot exceed Max Heap." : null;
    }

    private static Long heapSizeMiB(String value) {
        if (value == null || !value.trim().matches("[1-9][0-9]*[mMgG]")) {
            return null;
        }
        String size = value.trim();
        try {
            long amount = Long.parseLong(size.substring(0, size.length() - 1));
            return Math.multiplyExact(amount, Character.toUpperCase(size.charAt(size.length() - 1)) == 'G' ? 1024L : 1L);
        } catch (NumberFormatException | ArithmeticException e) {
            return null;
        }
    }

    private String buildServiceConfigJson(Map<String, Object> deploymentConfig, ServiceAssignmentReq svc) {
        Map<String, Object> serviceConfig = new HashMap<>(deploymentConfig);
        if (svc.getConfiguration_mode() != null && !svc.getConfiguration_mode().isBlank()) {
            serviceConfig.put("configuration_mode", svc.getConfiguration_mode());
        }
        if (svc.getHeap_size() != null && !svc.getHeap_size().isBlank()) {
            serviceConfig.put("heap_size", svc.getHeap_size());
        }
        if (svc.getHeap_xms() != null && !svc.getHeap_xms().isBlank()) {
            serviceConfig.put("heap_xms", svc.getHeap_xms());
        }
        if (svc.getHeap_xmx() != null && !svc.getHeap_xmx().isBlank()) {
            serviceConfig.put("heap_xmx", svc.getHeap_xmx());
        }
        if (svc.getListener_port() != null) {
            serviceConfig.put("listener_port", svc.getListener_port());
            serviceConfig.put("broker_port", svc.getListener_port());
        }
        if (svc.getController_port() != null) {
            serviceConfig.put("controller_port", svc.getController_port());
        }
        Integer jmxPort = jmxPortForService(svc);
        if (jmxPort != null) {
            serviceConfig.put("jmx_port", jmxPort);
            serviceConfig.put("jmx_enabled", true);
        }
        if (svc.getProperties_template() != null && !svc.getProperties_template().isBlank()) {
            String role = svc.getRole();
            if ("controller".equals(role)) {
                serviceConfig.put("controller_properties_template", svc.getProperties_template());
            } else if ("zookeeper".equals(role)) {
                serviceConfig.put("zookeeper_properties_template", svc.getProperties_template());
            } else if ("broker".equals(role)) {
                serviceConfig.put("broker_properties_template", svc.getProperties_template());
            } else {
                serviceConfig.put("server_properties_template", svc.getProperties_template());
            }
        }
        try {
            return objectMapper.writeValueAsString(serviceConfig);
        } catch (Exception e) {
            return "{}";
        }
    }

    private ResponseEntity<Map<String, String>> validateDeployRequest(DeployClusterRequest request) {
        if (request.getName() == null || request.getName().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Cluster name is required."));
        }
        clusterRepository.findByNameAndStatus(request.getName(), "DELETED")
                .forEach(cluster -> clusterRepository.purgeById(cluster.getId()));
        if (clusterRepository.findByNameAndStatusNot(request.getName(), "DELETED").isPresent()) {
            return ResponseEntity.badRequest().body(Map.of("error", "A non-deleted cluster with this name already exists."));
        }
        if (request.getServices() == null || request.getServices().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "At least one host assignment is required."));
        }

        String deploymentMode = normalizeDeploymentMode(request.getMode());
        boolean zookeeperMode = "zookeeper".equals(deploymentMode);
        if (zookeeperMode && !isZooKeeperSupported(request.getKafka_version())) {
            return ResponseEntity.badRequest().body(Map.of("error", "ZooKeeper deployments are not supported for Kafka 4.0.0 and newer."));
        }

        Set<String> assignmentKeys = new HashSet<>();
        Set<Integer> nodeIds = new HashSet<>();
        Map<String, Set<Integer>> jmxPortsByHost = new HashMap<>();
        boolean hasBroker = false;
        int brokerCount = 0;
        boolean hasController = false;
        boolean hasZooKeeper = false;
        for (ServiceAssignmentReq service : request.getServices()) {
            if (service.getHost_id() == null || service.getHost_id().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Every service assignment must include a host."));
            }
            if (service.getRole() == null || service.getRole().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Every service assignment must include a role."));
            }
            if (!isRoleAllowedForMode(service.getRole(), deploymentMode)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Role " + service.getRole() + " is not valid for " + deploymentMode + " deployments."));
            }
            if (service.getNode_id() == null || service.getNode_id() <= 0) {
                return ResponseEntity.badRequest().body(Map.of("error", "Every service assignment must include a positive node id."));
            }
            String heapError = heapRangeError(service.getHeap_size(), service.getHeap_xms(), service.getHeap_xmx());
            if (heapError != null) {
                return ResponseEntity.badRequest().body(Map.of("error", heapError + " Host: " + service.getHost_id() + "."));
            }
            Integer jmxPort = jmxPortForService(service);
            if (jmxPort != null && (jmxPort < 1 || jmxPort > 65535)) {
                return ResponseEntity.badRequest().body(Map.of("error", "JMX exporter port must be between 1 and 65535 for host " + service.getHost_id() + "."));
            }
            if (jmxPort != null && !jmxPortsByHost.computeIfAbsent(service.getHost_id(), ignored -> new HashSet<>()).add(jmxPort)) {
                return ResponseEntity.badRequest().body(Map.of("error", "JMX exporter port " + jmxPort + " is assigned more than once on host " + service.getHost_id() + "."));
            }
            if (!nodeIds.add(service.getNode_id())) {
                return ResponseEntity.badRequest().body(Map.of("error", "Node id " + service.getNode_id() + " is assigned more than once."));
            }
            for (String roleKind : serviceRoleKinds(service.getRole())) {
                String assignmentKey = service.getHost_id() + "::" + roleKind;
                if (!assignmentKeys.add(assignmentKey)) {
                    return ResponseEntity.badRequest().body(Map.of("error", "Host " + service.getHost_id() + " has duplicate " + roleKind + " service assignments."));
                }
            }

            if (isBrokerRole(service.getRole())) {
                hasBroker = true;
                brokerCount++;
            }
            if (isControllerRole(service.getRole())) {
                hasController = true;
            }
            if (isZooKeeperRole(service.getRole())) {
                hasZooKeeper = true;
            }

            io.translab.tantor.server.domain.Host host = hostRepository.findById(service.getHost_id()).orElse(null);
            if (host == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "Host " + service.getHost_id() + " was not found."));
            }
            String effectiveStatus = hostStatusService.effectiveStatus(host);
            if (!"ONLINE".equalsIgnoreCase(effectiveStatus)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Host " + service.getHost_id() + " is not online. Current status: " + effectiveStatus + "."));
            }
            if (host.getClusterId() != null) {
                java.util.Optional<Cluster> activeCluster = clusterRepository.findById(host.getClusterId())
                    .filter(cluster -> !"DELETED".equals(cluster.getStatus()));
                if (activeCluster.isPresent()) {
                    return ResponseEntity.badRequest().body(Map.of(
                        "error",
                        "Host " + service.getHost_id() + " is already assigned to cluster " + activeCluster.get().getName() + ". Delete or force-delete that cluster before reusing the host."
                    ));
                }
            }
        }
        if (!hasBroker) {
            return ResponseEntity.badRequest().body(Map.of("error", "At least one broker node is required."));
        }
        int replicationFactor = parseIntConfig(request.getConfig() == null ? null : request.getConfig().get("replication_factor"), 1);
        int minInSyncReplicas = parseIntConfig(request.getConfig() == null ? null : request.getConfig().get("min_insync_replicas"), 1);
        if (replicationFactor < 1 || replicationFactor > brokerCount) {
            return ResponseEntity.badRequest().body(Map.of("error", "Replication factor must be between 1 and the selected broker count (" + brokerCount + ")."));
        }
        if (minInSyncReplicas < 1 || minInSyncReplicas > replicationFactor) {
            return ResponseEntity.badRequest().body(Map.of("error", "Minimum in-sync replicas must be between 1 and the replication factor (" + replicationFactor + ")."));
        }
        if (zookeeperMode && !hasZooKeeper) {
            return ResponseEntity.badRequest().body(Map.of("error", "ZooKeeper deployments require at least one ZooKeeper or broker-zookeeper node."));
        }
        if (!zookeeperMode && !hasController) {
            return ResponseEntity.badRequest().body(Map.of("error", "KRaft deployments require at least one controller or broker-controller node."));
        }
        if (!zookeeperMode) {
            Map<String, Object> config = buildDeploymentConfig(request, deploymentMode);
            Map<String, Object> report = kraftValidationReport(request, config);
            @SuppressWarnings("unchecked")
            List<String> errors = (List<String>) report.get("errors");
            @SuppressWarnings("unchecked")
            List<String> warnings = (List<String>) report.get("warnings");
            if (!errors.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", errors.get(0)));
            }
            if (!warnings.isEmpty() && !request.isAcknowledge_kraft_risk()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error",
                        "Acknowledge the KRaft availability warning before deployment: " + warnings.get(0)
                ));
            }
        }
        try {
            validateSchemaAddon(schemaAddon(request), request.getServices());
            validateConnectAddon(connectAddon(request), request.getServices());
            SchemaRegistryAddonReq schema = schemaAddon(request);
            KafkaConnectAddonReq connect = connectAddon(request);
            if (schema != null && connect != null
                    && schema.getHost_id().equals(connect.getHost_id())
                    && java.util.Objects.equals(schema.getPort(), connect.getPort())) {
                throw new IllegalArgumentException("Schema Registry and Kafka Connect cannot use the same port on the same host.");
            }
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
        return null;
    }

    private ResponseEntity<Map<String, String>> validateAddNodeRequest(Cluster cluster, DeployClusterRequest request, String deploymentMode) {
        Set<String> assignmentKeys = new HashSet<>();
        Set<Integer> nodeIds = new HashSet<>();
        if (cluster.getServices() != null) {
            for (ClusterServiceAssignment existing : cluster.getServices()) {
                if (existing.getNodeId() != null) {
                    nodeIds.add(existing.getNodeId());
                }
                for (String roleKind : serviceRoleKinds(existing.getRole())) {
                    assignmentKeys.add(existing.getHostId() + "::" + roleKind);
                }
            }
        }

        for (ServiceAssignmentReq service : request.getServices()) {
            if (service.getHost_id() == null || service.getHost_id().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Every service assignment must include a host."));
            }
            if (service.getRole() == null || service.getRole().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Every service assignment must include a role."));
            }
            if (!isRoleAllowedForMode(service.getRole(), deploymentMode)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Role " + service.getRole() + " is not valid for " + deploymentMode + " deployments."));
            }
            if (!"broker".equals(service.getRole())) {
                return ResponseEntity.badRequest().body(Map.of("error", "Online Add Node currently supports broker services only. Controller and ZooKeeper voter changes require a dedicated quorum reconfiguration workflow."));
            }
            if (service.getNode_id() == null || service.getNode_id() <= 0) {
                return ResponseEntity.badRequest().body(Map.of("error", "Every service assignment must include a positive node id."));
            }
            Integer jmxPort = jmxPortForService(service);
            if (jmxPort != null && (jmxPort < 1 || jmxPort > 65535)) {
                return ResponseEntity.badRequest().body(Map.of("error", "JMX exporter port must be between 1 and 65535 for host " + service.getHost_id() + "."));
            }
            if (!nodeIds.add(service.getNode_id())) {
                return ResponseEntity.badRequest().body(Map.of("error", "Node id " + service.getNode_id() + " is already used in this cluster."));
            }
            for (String roleKind : serviceRoleKinds(service.getRole())) {
                String assignmentKey = service.getHost_id() + "::" + roleKind;
                if (!assignmentKeys.add(assignmentKey)) {
                    return ResponseEntity.badRequest().body(Map.of("error", "Host " + service.getHost_id() + " already has a " + roleKind + " service in this cluster."));
                }
            }

            io.translab.tantor.server.domain.Host host = hostRepository.findById(service.getHost_id()).orElse(null);
            if (host == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "Host " + service.getHost_id() + " was not found."));
            }
            String effectiveStatus = hostStatusService.effectiveStatus(host);
            if (!"ONLINE".equalsIgnoreCase(effectiveStatus)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Host " + service.getHost_id() + " is not online. Current status: " + effectiveStatus + "."));
            }
            if (host.getClusterId() != null && !cluster.getId().equals(host.getClusterId())) {
                java.util.Optional<Cluster> activeCluster = clusterRepository.findById(host.getClusterId())
                        .filter(existing -> !"DELETED".equals(existing.getStatus()));
                if (activeCluster.isPresent()) {
                    return ResponseEntity.badRequest().body(Map.of(
                            "error",
                            "Host " + service.getHost_id() + " is already assigned to cluster " + activeCluster.get().getName() + "."
                    ));
                }
            }
        }
        return null;
    }

    private Map<String, Object> buildDeploymentConfig(DeployClusterRequest request, String deploymentMode) {
        Map<String, Object> config = new HashMap<>();
        if (request.getConfig() != null) {
            config.putAll(request.getConfig());
        }

        config.put("mode", deploymentMode);
        config.put("version", request.getKafka_version());
        config.putIfAbsent("kafka_install_dir", "/opt");
        int listenerPort = requiredPort(config.get("listener_port"), "listener_port");
        config.put("listener_port", listenerPort);
        config.put("bootstrap_servers", buildBootstrapServers(request.getServices(), listenerPort));
        if ("zookeeper".equals(deploymentMode)) {
            int zookeeperPort = requiredPort(config.get("zookeeper_port"), "zookeeper_port");
            int zookeeperPeerPort = requiredPort(config.get("zookeeper_peer_port"), "zookeeper_peer_port");
            int zookeeperElectionPort = requiredPort(config.get("zookeeper_election_port"), "zookeeper_election_port");
            config.put("zookeeper_port", zookeeperPort);
            config.put("controller_port", zookeeperPort);
            config.put("zookeeper_connect", buildZooKeeperConnect(request.getServices(), zookeeperPort));
            config.put("zookeeper_peer_port", zookeeperPeerPort);
            config.put("zookeeper_election_port", zookeeperElectionPort);
            String zookeeperServers = buildZooKeeperServers(request.getServices(), zookeeperPeerPort, zookeeperElectionPort);
            if (!zookeeperServers.isBlank()) {
                config.put("zookeeper_servers", zookeeperServers);
            }
        } else {
            int controllerPort = requiredPort(config.get("controller_port"), "controller_port");
            config.put("controller_port", controllerPort);
            String quorumMode = normalizedKraftQuorumMode(request.getKafka_version(), config.get("kraft_quorum_mode"));
            String quorumVoters = buildQuorumVoters(request.getServices(), controllerPort);
            String quorumBootstrapServers = buildControllerBootstrapServers(request.getServices(), controllerPort);
            config.put("kraft_quorum_mode", quorumMode);
            config.put("quorum_bootstrap_servers", quorumBootstrapServers);
            config.put("controller_endpoints", quorumBootstrapServers);
            if ("dynamic".equals(quorumMode)) {
                config.remove("quorum_voters");
                config.putIfAbsent("initial_controllers", buildInitialControllers(request.getServices(), controllerPort));
            } else {
                config.put("quorum_voters", quorumVoters);
                config.remove("initial_controllers");
            }
            config.put("bootstrap_servers", buildBootstrapServers(request.getServices(), listenerPort));
            config.putIfAbsent("cluster_uuid", generateKafkaClusterUuid());
        }
        config.put("service_topology", buildServiceTopology(request.getServices(), deploymentMode, config));
        return config;
    }

    private String generateKafkaClusterUuid() {
        while (true) {
            UUID uuid = UUID.randomUUID();
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            String id = Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
            if (!id.isBlank() && Character.isLetterOrDigit(id.charAt(0))) {
                return id;
            }
        }
    }

    private String buildBootstrapServers(List<ServiceAssignmentReq> services, int listenerPort) {
        return services.stream()
                .filter(service -> isBrokerRole(service.getRole()))
                .map(service -> resolveHostAddress(service.getHost_id()) + ":" + (service.getListener_port() != null ? service.getListener_port() : listenerPort))
                .collect(Collectors.joining(","));
    }

    private List<Map<String, Object>> buildServiceTopology(List<ServiceAssignmentReq> services, String deploymentMode, Map<String, Object> config) {
        List<Map<String, Object>> topology = new ArrayList<>();
        String installDir = activeKafkaInstallDir(config);
        String dataDir = defaultKafkaDataDir(config);
        String listenerPort = String.valueOf(requiredPort(config.get("listener_port"), "listener_port"));
        String controllerPort = String.valueOf(requiredPort(config.get("controller_port"), "controller_port"));
        for (ServiceAssignmentReq service : services) {
            String role = service.getRole();
            String hostAddress = resolveHostAddress(service.getHost_id());
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("hostId", service.getHost_id());
            item.put("hostAddress", hostAddress);
            item.put("role", role);
            item.put("nodeId", service.getNode_id());
            item.put("serviceName", systemdServiceName(role));
            item.put("configFile", configFileForRole(role, deploymentMode,
                    String.valueOf(config.getOrDefault("version", config.getOrDefault("kafka_version", "0"))), installDir));
            item.put("listenerPort", isBrokerRole(role) ? (service.getListener_port() != null ? String.valueOf(service.getListener_port()) : listenerPort) : "");
            item.put("controllerPort", isControllerRole(role) || isZooKeeperRole(role) ? (service.getController_port() != null ? String.valueOf(service.getController_port()) : controllerPort) : "");
            item.put("logDirs", isBrokerRole(role) ? brokerLogDirs(config, dataDir) : "");
            item.put("metadataLogDir", metadataLogDirForRole(role, config, dataDir));
            topology.add(item);
        }
        return topology;
    }
    private String buildQuorumVoters(List<ServiceAssignmentReq> services, int controllerPort) {
        StringBuilder quorumVoters = new StringBuilder();
        List<ServiceAssignmentReq> controllers = services.stream()
                .filter(service -> isControllerRole(service.getRole()))
                .toList();

        for (int i = 0; i < controllers.size(); i++) {
            if (i > 0) quorumVoters.append(",");
            ServiceAssignmentReq controller = controllers.get(i);
            quorumVoters
                    .append(controller.getNode_id())
                    .append("@")
                    .append(resolveHostAddress(controller.getHost_id()))
                    .append(":")
                    .append(controller.getController_port() != null ? controller.getController_port() : controllerPort);
        }
        return quorumVoters.toString();
    }

    private String buildControllerBootstrapServers(List<ServiceAssignmentReq> services, int controllerPort) {
        return services.stream()
                .filter(service -> isControllerRole(service.getRole()))
                .map(service -> resolveHostAddress(service.getHost_id()) + ":" + (service.getController_port() != null ? service.getController_port() : controllerPort))
                .collect(Collectors.joining(","));
    }

    private String buildInitialControllers(List<ServiceAssignmentReq> services, int controllerPort) {
        return services.stream()
                .filter(service -> isControllerRole(service.getRole()))
                .map(service -> service.getNode_id() + "@" + resolveHostAddress(service.getHost_id()) + ":"
                        + (service.getController_port() != null ? service.getController_port() : controllerPort) + ":" + generateKafkaClusterUuid())
                .collect(Collectors.joining(","));
    }

    private String normalizedKraftQuorumMode(String kafkaVersion, Object configured) {
        String requested = configured == null ? "" : String.valueOf(configured).trim().toLowerCase();
        if ("static".equals(requested) || "dynamic".equals(requested)) return requested;
        return "static";
    }

    private Map<String, Object> kraftValidationReport(DeployClusterRequest request, Map<String, Object> config) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        List<Map<String, Object>> nodes = new ArrayList<>();
        Set<Integer> ids = new HashSet<>();
        Set<String> controllerEndpoints = new HashSet<>();
        int controllerCount = 0;
        int brokerCount = 0;
        int controllerPort = requiredPort(config.get("controller_port"), "controller_port");

        for (ServiceAssignmentReq service : request.getServices()) {
            String role = service.getRole() == null ? "" : service.getRole();
            String address = resolveHostAddress(service.getHost_id());
            Integer nodeId = service.getNode_id();
            if (nodeId == null || nodeId < 1 || !ids.add(nodeId)) {
                errors.add("Every KRaft process must have a unique positive node.id. Duplicate or invalid id: " + nodeId + ".");
            }
            if (!isRoleAllowedForMode(role, "kraft")) {
                errors.add("Invalid KRaft role " + role + " on " + service.getHost_id() + ".");
            }
            if (isControllerRole(role)) {
                controllerCount++;
                String endpoint = address + ":" + (service.getController_port() != null ? service.getController_port() : controllerPort);
                if (!controllerEndpoints.add(endpoint)) {
                    errors.add("Controller endpoint " + endpoint + " is assigned more than once.");
                }
            }
            if (isBrokerRole(role)) brokerCount++;
            nodes.add(Map.of(
                    "hostId", String.valueOf(service.getHost_id()),
                    "address", address,
                    "nodeId", nodeId == null ? 0 : nodeId,
                    "role", role
            ));
        }

        if (controllerCount == 0) errors.add("At least one KRaft controller is required.");
        if (brokerCount == 0) errors.add("At least one KRaft broker is required.");
        if (controllerPort < 1 || controllerPort > 65535) errors.add("Controller port must be between 1 and 65535.");

        String clusterId = String.valueOf(config.getOrDefault("cluster_uuid", ""));
        if (!clusterId.matches("[A-Za-z0-9][A-Za-z0-9_-]{19,23}")) {
            errors.add("Kafka storage cluster ID is missing or invalid.");
        }

        String quorumMode = String.valueOf(config.getOrDefault("kraft_quorum_mode", "static"));
        String expectedVoters = buildQuorumVoters(request.getServices(), controllerPort);
        String expectedBootstrap = buildControllerBootstrapServers(request.getServices(), controllerPort);
        Object suppliedVoters = request.getConfig() == null ? null : request.getConfig().get("quorum_voters");
        if ("static".equals(quorumMode)) {
            if (!expectedVoters.equals(String.valueOf(config.getOrDefault("quorum_voters", "")))) {
                errors.add("controller.quorum.voters does not exactly match the selected controller IDs and endpoints.");
            }
            if (suppliedVoters != null && !String.valueOf(suppliedVoters).isBlank()
                    && !expectedVoters.equals(String.valueOf(suppliedVoters))) {
                errors.add("The supplied controller.quorum.voters conflicts with the selected topology.");
            }
        } else {
            if (suppliedVoters != null && !String.valueOf(suppliedVoters).isBlank()) {
                errors.add("Dynamic KRaft quorum must not configure controller.quorum.voters.");
            }
            if (!expectedBootstrap.equals(String.valueOf(config.getOrDefault("quorum_bootstrap_servers", "")))) {
                errors.add("controller.quorum.bootstrap.servers does not match the selected controllers.");
            }
            String initialControllers = String.valueOf(config.getOrDefault("initial_controllers", ""));
            if (initialControllers.split(",").length != controllerCount) {
                errors.add("Dynamic quorum initial controller membership is incomplete.");
            }
        }

        String environment = request.getEnvironment() == null ? "" : request.getEnvironment().trim().toUpperCase();
        boolean weakQuorum = controllerCount < 3;
        boolean evenQuorum = controllerCount > 1 && controllerCount % 2 == 0;
        if (weakQuorum) warnings.add(controllerCount + " controller quorum tolerates no controller failure; 3 controllers are recommended.");
        if (evenQuorum) warnings.add("Even controller count " + controllerCount + " provides no additional failure tolerance over " + (controllerCount - 1) + ".");
        if (controllerCount > 5) warnings.add("Controller count " + controllerCount + " is unusual; 3 or 5 controllers are recommended.");
        if ("UAT".equals(environment) && (weakQuorum || evenQuorum)) {
            errors.add("UAT requires an odd KRaft controller quorum with at least 3 controllers.");
        }

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("valid", errors.isEmpty());
        report.put("errors", errors.stream().distinct().toList());
        report.put("warnings", warnings.stream().distinct().toList());
        report.put("acknowledgementRequired", !warnings.isEmpty() && !"UAT".equals(environment));
        report.put("clusterId", clusterId);
        report.put("quorumMode", quorumMode);
        report.put("controllerCount", controllerCount);
        report.put("brokerCount", brokerCount);
        report.put("failureTolerance", controllerCount == 0 ? 0 : (controllerCount - 1) / 2);
        report.put("controllerQuorum", "static".equals(quorumMode) ? expectedVoters : expectedBootstrap);
        report.put("nodes", nodes);
        return report;
    }

    private String buildZooKeeperConnect(List<ServiceAssignmentReq> services, int zookeeperPort) {
        return services.stream()
                .filter(service -> isZooKeeperRole(service.getRole()))
                .map(service -> resolveHostAddress(service.getHost_id()) + ":" + zookeeperPort)
                .collect(Collectors.joining(","));
    }

    private String buildZooKeeperServers(List<ServiceAssignmentReq> services, int peerPort, int electionPort) {
        List<ServiceAssignmentReq> zookeeperNodes = services.stream()
                .filter(service -> isZooKeeperRole(service.getRole()))
                .toList();
        if (zookeeperNodes.size() <= 1) {
            return "";
        }
        return zookeeperNodes.stream()
                .map(service -> "server." + service.getNode_id() + "=" + resolveHostAddress(service.getHost_id()) + ":" + peerPort + ":" + electionPort)
                .collect(Collectors.joining("\n"));
    }

    private String resolveHostAddress(String hostId) {
        String hostIp = hostId;
        io.translab.tantor.server.domain.Host h = hostRepository.findById(hostId).orElse(null);
        if (h != null && h.getIpAddresses() != null && !h.getIpAddresses().isEmpty() && !h.getIpAddresses().equals("[]")) {
            try {
                List<String> ips = objectMapper.readValue(h.getIpAddresses(), new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});
                if (!ips.isEmpty()) {
                    hostIp = ips.get(0);
                }
            } catch (Exception e) {
                hostIp = h.getIpAddresses().replaceAll("\\[|\\]|\\\"", "").split(",")[0].trim();
            }
        }
        return hostIp;
    }

    private List<String> serviceRoleKinds(String role) {
        if ("broker_controller".equals(role)) {
            return List.of("broker", "controller");
        }
        if ("broker_zookeeper".equals(role)) {
            return List.of("broker", "zookeeper");
        }
        return List.of(role);
    }

    private String clusterRoleForServices(List<ServiceAssignmentReq> services) {
        if (services == null || services.isEmpty()) return "broker";
        boolean hasBroker = false;
        boolean hasController = false;
        for (ServiceAssignmentReq service : services) {
            String role = service.getRole();
            if ("broker_controller".equals(role) || "broker+controller".equals(role)) return "broker_controller";
            if ("broker".equals(role)) hasBroker = true;
            if ("controller".equals(role)) hasController = true;
            if ("schema_registry".equals(role) || "schema registry".equalsIgnoreCase(String.valueOf(role))) return "schema_registry";
            if ("kafka_connect".equals(role) || "kafka connect".equalsIgnoreCase(String.valueOf(role))) return "kafka_connect";
            if ("connect".equals(role)) return "connect";
        }
        if (hasBroker) return "broker";
        if (hasController) return "controller";
        return services.get(0).getRole() == null || services.get(0).getRole().isBlank() ? "broker" : services.get(0).getRole();
    }

    private String systemdServiceName(String role) {
        if ("controller".equals(role)) return "controller";
        if ("zookeeper".equals(role)) return "zookeeper";
        if ("broker_controller".equals(role)) return "kafka";
        if ("broker_zookeeper".equals(role)) return "kafka";
        return "broker";
    }

    private String configFileForRole(String role, String deploymentMode, String kafkaVersion, String installDir) {
        if ("zookeeper".equalsIgnoreCase(deploymentMode)) {
            if ("zookeeper".equals(role)) return installDir + "/config/zookeeper.properties";
            return installDir + "/config/server.properties";
        }
        String configRoot = parseKafkaVersion(kafkaVersion)[0] >= 4 ? installDir + "/config" : installDir + "/config/kraft";
        if ("controller".equals(role)) return configRoot + "/controller.properties";
        if ("broker".equals(role)) return configRoot + "/broker.properties";
        return configRoot + "/server.properties";
    }

    private String activeKafkaInstallDir(Map<String, Object> config) {
        String configured = String.valueOf(config.getOrDefault("kafka_install_base_dir", config.getOrDefault("kafka_install_dir", "/opt"))).trim();
        if (configured.isBlank()) configured = "/opt";
        configured = trimTrailingSlash(configured);
        if (configured.endsWith("/kafka")) return configured;
        String leaf = configured.substring(configured.lastIndexOf('/') + 1);
        if (leaf.startsWith("kafka_")) {
            int lastSlash = configured.lastIndexOf('/');
            return (lastSlash <= 0 ? "" : configured.substring(0, lastSlash)) + "/kafka";
        }
        return configured + "/kafka";
    }

    private String defaultKafkaDataDir(Map<String, Object> config) {
        String configured = String.valueOf(config.getOrDefault("kafka_install_base_dir", config.getOrDefault("kafka_install_dir", "/opt"))).trim();
        if (configured.isBlank()) configured = "/opt";
        configured = trimTrailingSlash(configured);
        if ("/opt".equals(configured) || "/".equals(configured)) return "/data/kafka";
        if (configured.endsWith("/kafka")) {
            int lastSlash = configured.lastIndexOf('/');
            configured = lastSlash <= 0 ? "/" : configured.substring(0, lastSlash);
        }
        return trimTrailingSlash(configured) + "/kafka-data";
    }

    private String trimTrailingSlash(String value) {
        while (value.length() > 1 && value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private String brokerLogDirs(Map<String, Object> config, String dataDir) {
        Object configured = config.get("log_dirs");
        if (configured != null && !String.valueOf(configured).isBlank()) return String.valueOf(configured);
        return dataDir + "/broker-data";
    }

    private String metadataLogDirForRole(String role, Map<String, Object> config, String dataDir) {
        Object configured = config.get("metadata_log_dir");
        if (configured != null && !String.valueOf(configured).isBlank()) return String.valueOf(configured);
        if ("controller".equals(role)) return dataDir + "/controller-data/metadata";
        if (isControllerRole(role) && !isBrokerRole(role)) return dataDir + "/controller-data/metadata";
        return dataDir + "/broker-metadata";
    }
    private boolean isRoleAllowedForMode(String role, String deploymentMode) {
        if ("zookeeper".equals(deploymentMode)) {
            return "broker".equals(role) || "zookeeper".equals(role) || "broker_zookeeper".equals(role);
        }
        return "broker".equals(role) || "controller".equals(role) || "broker_controller".equals(role);
    }

    private Integer jmxPortForService(ServiceAssignmentReq service) {
        if (service == null || service.getRole() == null || isZooKeeperRole(service.getRole())) {
            return null;
        }
        if (service.getJmx_port() != null) {
            return service.getJmx_port();
        }
        return "controller".equals(service.getRole())
                ? DEFAULT_CONTROLLER_JMX_EXPORTER_PORT
                : DEFAULT_JMX_EXPORTER_PORT;
    }

    private boolean isBrokerRole(String role) {
        return "broker".equals(role) || "broker_controller".equals(role) || "broker_zookeeper".equals(role);
    }

    private boolean isControllerRole(String role) {
        return "controller".equals(role) || "broker_controller".equals(role);
    }

    private boolean isZooKeeperRole(String role) {
        return "zookeeper".equals(role) || "broker_zookeeper".equals(role);
    }

    private boolean isMetadataService(String role) {
        return isControllerRole(role) || isZooKeeperRole(role);
    }

    private String normalizeDeploymentMode(String mode) {
        return "zookeeper".equalsIgnoreCase(mode) ? "zookeeper" : "kraft";
    }

    private boolean isZooKeeperSupported(String kafkaVersion) {
        int[] version = parseKafkaVersion(kafkaVersion);
        return version[0] < 4;
    }

    private String externalMetadataValue(Cluster cluster, String key) {
        if (!"EXTERNAL".equalsIgnoreCase(cluster.getMode()) || cluster.getConfigJson() == null || cluster.getConfigJson().isBlank()) {
            return null;
        }
        try {
            Map<String, Object> metadata = objectMapper.readValue(cluster.getConfigJson(), Map.class);
            Object value = metadata.get(key);
            return value == null ? null : String.valueOf(value);
        } catch (Exception e) {
            return null;
        }
    }

    private String kafkaClusterId(Cluster cluster) {
        if (cluster.getKafkaClusterId() != null && !cluster.getKafkaClusterId().isBlank()) {
            return cluster.getKafkaClusterId();
        }
        String externalId = externalMetadataValue(cluster, "kafkaClusterId");
        if (externalId != null && !externalId.isBlank()) {
            return externalId;
        }
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())
                || !("SUCCESS".equalsIgnoreCase(cluster.getStatus()) || "ACTIVE".equalsIgnoreCase(cluster.getStatus()))) {
            return "";
        }
        return kafkaAdminService.getKafkaClusterId(cluster.getId());
    }

    private String blankString(Object value) {
        return value == null ? null : (String.valueOf(value).isBlank() ? null : String.valueOf(value));
    }

    private Map<String, Object> parseConfigJson(String configJson) {
        if (configJson == null || configJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(configJson, Map.class);
        } catch (Exception e) {
            return Map.of();
        }
    }

    private String managementLevel(Cluster cluster) {
        String level = externalMetadataValue(cluster, "managementMode");
        if (level != null && !level.isBlank()) {
            return level;
        }
        if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            return "BOOTSTRAP_ONLY";
        }
        return "INTERNAL_MANAGED";
    }

    private String sourceLabel(Cluster cluster) {
        return "EXTERNAL".equalsIgnoreCase(cluster.getMode()) ? "External" : "Internal";
    }

    private String accessLabel(Cluster cluster) {
        if (!"EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            return "Full access";
        }
        return "AGENT_MANAGED".equalsIgnoreCase(managementLevel(cluster))
                ? "Fully managed"
                : "Metadata available";
    }

    private Map<String, Object> internalRuntimeHealth(Cluster cluster) {
        Map<String, Object> health = new LinkedHashMap<>();
        String deploymentStatus = cluster.getStatus() == null ? "" : cluster.getStatus().trim();

        if (!"SUCCESS".equalsIgnoreCase(deploymentStatus)) {
            health.put("runtimeHealth", deploymentStatus.isBlank() ? "UNKNOWN" : deploymentStatus.toUpperCase());
            health.put("runtimeStatusLabel", deploymentStatus.isBlank() ? "Unknown" : deploymentStatus);
            health.put("runtimeStatusReason", "Deployment is not in SUCCESS state yet.");
            return health;
        }

        try {
            List<io.translab.tantor.server.dto.BrokerSummaryDto> brokers = brokerMetricsCacheService
                    .getBrokerSummaries(cluster)
                    .stream()
                    .filter(broker -> isBrokerRole(broker.getRole()))
                    .toList();
            long total = brokers.size();
            long offline = brokers.stream().filter(b -> "OFFLINE".equalsIgnoreCase(b.getBrokerHealth())).count();
            long degraded = brokers.stream().filter(b -> "DEGRADED".equalsIgnoreCase(b.getBrokerHealth())).count();
            long healthy = brokers.stream().filter(b -> "HEALTHY".equalsIgnoreCase(b.getBrokerHealth())).count();

            health.put("runtimeBrokerCount", total);
            health.put("runtimeHealthyBrokers", healthy);
            health.put("runtimeDegradedBrokers", degraded);
            health.put("runtimeOfflineBrokers", offline);

            if (total == 0) {
                health.put("runtimeHealth", "UNKNOWN");
                health.put("runtimeStatusLabel", "Unknown");
                health.put("runtimeStatusReason", "No broker services are mapped for runtime verification.");
            } else if (offline == total) {
                health.put("runtimeHealth", "OFFLINE");
                health.put("runtimeStatusLabel", "Kafka Offline");
                health.put("runtimeStatusReason", "All broker agents or Kafka endpoints are unavailable.");
            } else if (offline > 0) {
                health.put("runtimeHealth", "DEGRADED");
                health.put("runtimeStatusLabel", "Degraded");
                health.put("runtimeStatusReason", offline + " of " + total + " broker node(s) are offline.");
            } else if (degraded > 0) {
                health.put("runtimeHealth", "DEGRADED");
                health.put("runtimeStatusLabel", "Kafka Check Failed");
                health.put("runtimeStatusReason", "Host agents are online, but Kafka/JMX verification failed for " + degraded + " broker node(s).");
            } else {
                health.put("runtimeHealth", "HEALTHY");
                health.put("runtimeStatusLabel", "Active");
                health.put("runtimeStatusReason", "Deployment succeeded and broker runtime checks are healthy.");
            }
        } catch (Exception e) {
            health.put("runtimeHealth", "DEGRADED");
            health.put("runtimeStatusLabel", "Kafka Check Failed");
            health.put("runtimeStatusReason", "Runtime verification failed: " + e.getMessage());
        }

        return health;
    }

    private ExternalHealthView externalHealthView(
            ExternalCluster cluster,
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes,
            List<io.translab.tantor.server.domain.DiscoveryAgent> knownDiscoveryAgents,
            boolean liveKafkaCheck
    ) {
        return externalHealthView(cluster, nodes, knownDiscoveryAgents, liveKafkaCheck, null);
    }

    private ExternalHealthView externalHealthView(
            ExternalCluster cluster,
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes,
            List<io.translab.tantor.server.domain.DiscoveryAgent> knownDiscoveryAgents,
            boolean liveKafkaCheck,
            String kafkaHealthOverride
    ) {
        List<io.translab.tantor.server.domain.ExternalClusterNode> safeNodes = nodes == null ? List.of() : nodes;
        List<String> hosts = safeNodes.stream()
                .map(io.translab.tantor.server.domain.ExternalClusterNode::getHost)
                .filter(value -> value != null && !value.isBlank())
                .distinct()
                .toList();
        long totalHostsCount = hosts.size();

        List<io.translab.tantor.server.domain.DiscoveryAgent> allAgents =
                knownDiscoveryAgents == null ? discoveryAgentRepository.findAll() : knownDiscoveryAgents;
        List<io.translab.tantor.server.domain.DiscoveryAgent> linkedAgents = allAgents.stream()
                .filter(agent -> cluster.getId().equals(agent.getClusterId()))
                .toList();
        OffsetDateTime maxHeartbeat = null;
        long freshHostsCount = 0;
        for (String host : hosts) {
            Optional<io.translab.tantor.server.domain.DiscoveryAgent> freshAgent = matchingFreshAgent(host, cluster.getId(), linkedAgents, allAgents);
            if (freshAgent.isPresent()) {
                freshHostsCount++;
            }
            Optional<io.translab.tantor.server.domain.DiscoveryAgent> lastReportingAgent =
                    freshAgent.isPresent() ? freshAgent : matchingAgent(host, cluster.getId(), linkedAgents, allAgents);
            if (lastReportingAgent.isPresent()) {
                OffsetDateTime heartbeat = lastReportingAgent.get().getLastHeartbeat();
                if (heartbeat != null && (maxHeartbeat == null || heartbeat.isAfter(maxHeartbeat))) {
                    maxHeartbeat = heartbeat;
                }
            }
        }

        AgentConnectivityState connectivity = agentConnectivityState(freshHostsCount, totalHostsCount);
        String telemetry = connectivity.telemetry();
        String managementLevel = connectivity.label();
        String agentHealth = connectivity.health();

        String kafkaHealth = kafkaHealthOverride != null
                ? kafkaHealthOverride
                : externalKafkaHealth(cluster, safeNodes, liveKafkaCheck);
        String overallHealth = "OFFLINE".equals(kafkaHealth) ? "OFFLINE"
                : "DEGRADED".equals(kafkaHealth) ? "DEGRADED"
                : "UNKNOWN".equals(kafkaHealth) ? "UNKNOWN"
                : "HEALTHY";
        String statusLabel = switch (overallHealth) {
            case "OFFLINE" -> "Kafka Offline";
            case "DEGRADED" -> "Kafka Degraded";
            case "HEALTHY" -> "Kafka Online";
            default -> "Unknown";
        };
        String reason = externalHealthReason(kafkaHealth, agentHealth, freshHostsCount, totalHostsCount);

        return new ExternalHealthView(
                kafkaHealth,
                agentHealth,
                "UNKNOWN",
                overallHealth,
                statusLabel,
                reason,
                telemetry,
                managementLevel,
                freshHostsCount,
                totalHostsCount,
                maxHeartbeat
        );
    }

    /** Keeps label, color-driving health and telemetry on the same live-agent count. */
    static AgentConnectivityState agentConnectivityState(long freshHostsCount, long totalHostsCount) {
        if (freshHostsCount <= 0 || totalHostsCount <= 0) {
            return new AgentConnectivityState("NOT_CONNECTED", "Agent Not Connected", "None");
        }
        if (freshHostsCount >= totalHostsCount) {
            return new AgentConnectivityState("CONNECTED", "Agent Connected", "Full");
        }
        return new AgentConnectivityState("PARTIAL", "Partially Connected", "Partial");
    }

    private Optional<io.translab.tantor.server.domain.DiscoveryAgent> matchingAgent(
            String host,
            UUID clusterId,
            List<io.translab.tantor.server.domain.DiscoveryAgent> linkedAgents,
            List<io.translab.tantor.server.domain.DiscoveryAgent> allAgents
    ) {
        return linkedAgents.stream()
                .filter(agent -> matchesDiscoveryAgent(agent, host))
                .findFirst()
                .or(() -> allAgents.stream()
                        .filter(agent -> matchesDiscoveryAgent(agent, host))
                        .filter(agent -> agent.getClusterId() == null || agent.getClusterId().equals(clusterId))
                        .findFirst());
    }

    private Optional<io.translab.tantor.server.domain.DiscoveryAgent> matchingFreshAgent(
            String host,
            UUID clusterId,
            List<io.translab.tantor.server.domain.DiscoveryAgent> linkedAgents,
            List<io.translab.tantor.server.domain.DiscoveryAgent> allAgents
    ) {
        return linkedAgents.stream()
                .filter(agent -> isFreshOnlineAgent(agent) && matchesDiscoveryAgent(agent, host))
                .findFirst()
                .or(() -> allAgents.stream()
                        .filter(agent -> isFreshOnlineAgent(agent) && matchesDiscoveryAgent(agent, host))
                        .filter(agent -> agent.getClusterId() == null || agent.getClusterId().equals(clusterId))
                        .findFirst());
    }

    private boolean isFreshOnlineAgent(io.translab.tantor.server.domain.DiscoveryAgent agent) {
        return agent != null
                && "ONLINE".equalsIgnoreCase(agent.getStatus())
                && agent.getLastHeartbeat() != null
                && agent.getLastHeartbeat().isAfter(OffsetDateTime.now().minusSeconds(discoveryAgentFreshnessSeconds()));
    }

    private String externalKafkaHealth(
            ExternalCluster cluster,
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes,
            boolean liveKafkaCheck
    ) {
        if (cluster.getBootstrapServers() == null || cluster.getBootstrapServers().isBlank()) {
            return "UNKNOWN";
        }
        if (!liveKafkaCheck) {
            if ("FAILED".equalsIgnoreCase(cluster.getStatus()) || Boolean.FALSE.equals(cluster.getIsRunning())) {
                return "OFFLINE";
            }
            if ("SUCCESS".equalsIgnoreCase(cluster.getStatus())
                    || "DEGRADED".equalsIgnoreCase(cluster.getStatus())
                    || (cluster.getKafkaClusterId() != null && !cluster.getKafkaClusterId().isBlank())
                    || (cluster.getBrokerCount() != null && cluster.getBrokerCount() > 0)
                    || !nodes.isEmpty()) {
                return "HEALTHY";
            }
            return "UNKNOWN";
        }
        return kafkaAdminService.isClusterReachable(cluster.getId(), externalKafkaHealthTimeoutSeconds)
                ? "HEALTHY"
                : "OFFLINE";
    }

    private String externalHealthReason(String kafkaHealth, String agentHealth, long managedHostsCount, long totalHostsCount) {
        if ("OFFLINE".equals(kafkaHealth)) {
            return "Kafka bootstrap/admin connectivity is unavailable.";
        }
        if ("DEGRADED".equals(kafkaHealth)) {
            return "Kafka is reachable but reports a degraded runtime state.";
        }
        if ("CONNECTED".equals(agentHealth)) {
            return "Kafka is reachable and discovery agent heartbeat is fresh.";
        }
        if ("PARTIAL".equals(agentHealth)) {
            return "Kafka is reachable; discovery agent is fresh for " + managedHostsCount + " of " + totalHostsCount + " host(s).";
        }
        if ("NOT_INSTALLED".equals(agentHealth)) {
            return "Kafka is reachable; no discovery agent has reported for this cluster.";
        }
        return "Kafka is reachable; discovery agent heartbeat is missing or stale.";
    }

    private long discoveryAgentFreshnessSeconds() {
        return Math.max(15, discoveryAgentHeartbeatTimeoutSeconds);
    }

    private void putExternalHealth(Map<String, Object> target, ExternalHealthView health) {
        target.put("kafkaHealth", health.kafkaHealth());
        target.put("agentHealth", health.agentHealth());
        target.put("monitoringHealth", health.monitoringHealth());
        target.put("overallHealth", health.overallHealth());
        target.put("runtimeHealth", health.overallHealth());
        target.put("runtimeStatusLabel", health.statusLabel());
        target.put("runtimeStatusReason", health.reason());
    }

    private record ExternalHealthView(
            String kafkaHealth,
            String agentHealth,
            String monitoringHealth,
            String overallHealth,
            String statusLabel,
            String reason,
            String telemetry,
            String managementLevel,
            long managedHostsCount,
            long totalHostsCount,
            OffsetDateTime lastAgentHeartbeat
    ) {}

    record AgentConnectivityState(String health, String label, String telemetry) {}

    private String externalRuntimeHealth(String status) {
        if ("SUCCESS".equalsIgnoreCase(status)) return "HEALTHY";
        if ("FAILED".equalsIgnoreCase(status)) return "OFFLINE";
        if ("DEGRADED".equalsIgnoreCase(status)) return "DEGRADED";
        return status == null || status.isBlank() ? "UNKNOWN" : status.toUpperCase();
    }

    private String externalRuntimeStatusLabel(String status) {
        if ("SUCCESS".equalsIgnoreCase(status)) return "Connected";
        if ("FAILED".equalsIgnoreCase(status)) return "Kafka Offline";
        if ("DEGRADED".equalsIgnoreCase(status)) return "Degraded";
        return status == null || status.isBlank() ? "Unknown" : status;
    }

    private boolean matchesDiscoveryAgent(io.translab.tantor.server.domain.DiscoveryAgent agent, String host) {
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
            return objectMapper.readValue(ipAddresses, new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});
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

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private String firstExternalNodeValue(
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes,
            String host,
            String field
    ) {
        if (nodes == null || host == null || host.isBlank()) {
            return null;
        }
        for (io.translab.tantor.server.domain.ExternalClusterNode node : nodes) {
            if (node == null || node.getHost() == null || !node.getHost().equalsIgnoreCase(host)) {
                continue;
            }
            String value = switch (field) {
                case "installDir" -> node.getInstallDir();
                case "configFile" -> node.getConfigFile();
                case "dataDirs" -> node.getDataDirs();
                case "logDirs" -> node.getLogDirs();
                default -> null;
            };
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private String externalKafkaVersion(
            io.translab.tantor.server.domain.ExternalCluster cluster,
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes
    ) {
        String version = cleanExternalValue(cluster.getKafkaVersion());
        if (version != null) {
            return version;
        }
        version = versionFromKafkaPath(cleanExternalValue(cluster.getInstallPath()));
        if (version != null) {
            return version;
        }
        for (io.translab.tantor.server.domain.ExternalClusterNode node : nodes) {
            version = versionFromKafkaPath(cleanExternalValue(node.getInstallDir()));
            if (version != null) {
                return version;
            }
        }
        return "Not reported";
    }

    private String externalControllerType(
            io.translab.tantor.server.domain.ExternalCluster cluster,
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes
    ) {
        String mode = cleanExternalValue(cluster.getKafkaMode());
        if ("zookeeper".equalsIgnoreCase(mode) || "zk".equalsIgnoreCase(mode)) {
            return "ZooKeeper";
        }
        if ("kraft".equalsIgnoreCase(mode)) {
            return "KRaft";
        }
        // describeCluster().controller() exists in ZooKeeper mode too, so a
        // controller-looking broker is not sufficient evidence of KRaft.
        return "Not reported";
    }

    private String cleanExternalValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String trimmed = value.trim();
        if ("null".equalsIgnoreCase(trimmed)
                || "unknown".equalsIgnoreCase(trimmed)
                || "auto-detected".equalsIgnoreCase(trimmed)
                || "auto-detected by Kafka client".equalsIgnoreCase(trimmed)) {
            return null;
        }
        return trimmed;
    }

    private String versionFromKafkaPath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String normalized = path.replace('\\', '/');
        String[] segments = normalized.split("/");
        for (int i = segments.length - 1; i >= 0; i--) {
            String segment = segments[i];
            int index = segment.lastIndexOf('-');
            if (index < 0 || index == segment.length() - 1) {
                continue;
            }
            String candidate = segment.substring(index + 1);
            if (candidate.matches("\\d+(\\.\\d+){1,3}")) {
                return candidate;
            }
        }
        return null;
    }

    private String inferredExternalConfigFile(String role, String kafkaMode, String kafkaVersion, String installDir) {
        if (installDir == null || installDir.isBlank()) {
            return null;
        }
        if (!"KRaft".equalsIgnoreCase(kafkaMode)) {
            return installDir + "/config/server.properties";
        }
        boolean kafka4OrLater = parseKafkaVersion(kafkaVersion)[0] >= 4;
        String configRoot = kafka4OrLater ? installDir + "/config" : installDir + "/config/kraft";
        if ("controller".equalsIgnoreCase(role)) {
            return configRoot + "/controller.properties";
        }
        if ("broker".equalsIgnoreCase(role)) {
            return configRoot + "/broker.properties";
        }
        return configRoot + "/server.properties";
    }

    private String parentPath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        int index = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        return index > 0 ? path.substring(0, index) : null;
    }

    private List<Map<String, Object>> clusterHosts(Cluster cluster) {
        List<Map<String, Object>> hosts = new ArrayList<>();
        if (cluster.getServices() != null) {
            for (ClusterServiceAssignment service : cluster.getServices()) {
                hostRepository.findById(service.getHostId()).ifPresent(host -> {
                    if (!"EXTERNAL".equalsIgnoreCase(cluster.getMode())
                            || "ONLINE".equalsIgnoreCase(hostStatusService.effectiveStatus(host))) {
                        hosts.add(hostSummary(service, host));
                    }
                });
            }
        }
        return hosts;
    }

    private Map<String, Object> hostSummary(ClusterServiceAssignment service, io.translab.tantor.server.domain.Host host) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("hostId", host.getId());
        summary.put("hostname", host.getHostname());
        summary.put("ipAddress", firstIp(host.getIpAddresses()));
        summary.put("status", hostStatusService.effectiveStatus(host));
        summary.put("agentStatus", hostStatusService.agentConnectivityStatus(host));
        summary.put("role", service.getRole());
        summary.put("nodeId", service.getNodeId());
        summary.put("lastHeartbeat", host.getLastHeartbeat());
        summary.put("diskUsedGb", host.getDiskUsedGb());
        summary.put("diskTotalGb", host.getDiskTotalGb());
        summary.put("bootstrap", "");
        return summary;
    }

    private List<Map<String, Object>> externalClusterHosts(
            ExternalCluster cluster,
            List<io.translab.tantor.server.domain.ExternalClusterNode> nodes
    ) {
        List<Map<String, Object>> hosts = new ArrayList<>();
        List<io.translab.tantor.server.domain.DiscoveryAgent> agents = discoveryAgentRepository.findByClusterId(cluster.getId());
        List<io.translab.tantor.server.domain.DiscoveryAgent> allAgents = discoveryAgentRepository.findAll();
        OffsetDateTime now = OffsetDateTime.now();

        for (io.translab.tantor.server.domain.ExternalClusterNode node : nodes) {
            Optional<io.translab.tantor.server.domain.DiscoveryAgent> agentMatch = agents.stream()
                    .filter(agent -> matchesDiscoveryAgent(agent, node.getHost()))
                    .findFirst();
            boolean agentOnline = agentMatch.filter(this::isFreshOnlineAgent).isPresent();
            boolean hostIsIp = looksLikeIpAddress(node.getHost());

            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("id", node.getId().toString());
            summary.put("hostId", "");
            summary.put("nodeId", node.getNodeId());
            summary.put("hostname", agentMatch.map(io.translab.tantor.server.domain.DiscoveryAgent::getHostname)
                    .filter(value -> value != null && !value.isBlank() && !looksLikeIpAddress(value))
                    .orElse(hostIsIp ? "" : node.getHost()));
            summary.put("ipAddress", hostIsIp ? node.getHost()
                    : agentMatch.map(agent -> firstIp(agent.getIpAddresses())).orElse(""));
            summary.put("role", externalNodeRole(node));
            summary.put("status", agentOnline ? "Managed" : "Bootstrap connected");
            summary.put("lastHeartbeat", agentMatch
                    .map(io.translab.tantor.server.domain.DiscoveryAgent::getLastHeartbeat)
                    .orElse(node.getLastSeen()));
            summary.put("diskUsedGb", node.getDiskUsedGb());
            summary.put("diskTotalGb", node.getDiskTotalGb());
            summary.put("bootstrap", cluster.getBootstrapServers());

            if (agentMatch.isEmpty()) {
                allAgents.stream()
                        .filter(this::isFreshOnlineAgent)
                        .filter(agent -> agent.getLastHeartbeat() != null
                                && java.time.Duration.between(agent.getLastHeartbeat(), now).getSeconds() <= discoveryAgentFreshnessSeconds())
                        .filter(agent -> matchesDiscoveryAgent(agent, node.getHost()))
                        .filter(agent -> agent.getClusterId() == null || !agent.getClusterId().equals(cluster.getId()))
                        .findFirst()
                        .ifPresent(agent -> {
                            summary.put("agentAvailable", true);
                            summary.put("availableAgentId", agent.getId().toString());
                        });
            }

            hosts.add(summary);
        }
        return hosts;
    }

    private String externalNodeRole(io.translab.tantor.server.domain.ExternalClusterNode node) {
        boolean isBroker = Boolean.TRUE.equals(node.getIsBroker());
        boolean isController = Boolean.TRUE.equals(node.getIsController());
        if (isBroker && isController) return "broker_controller";
        if (isBroker) return "broker";
        if (isController) return "controller";
        return "unknown";
    }

    private Map<String, Object> externalBrokerSummary(io.translab.tantor.server.service.ExternalClusterService.ExternalBrokerRecord broker) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("hostId", "");
        summary.put("hostname", broker.getHostname());
        summary.put("ipAddress", extractBootstrapHost(broker.getBootstrap()));
        summary.put("status", broker.isRunning() ? "ONLINE" : "OFFLINE");
        summary.put("role", broker.getRole());
        summary.put("lastHeartbeat", parseOffsetDateTime(broker.getLastSeen()));
        summary.put("diskUsedGb", broker.getDiskUsedGb());
        summary.put("diskTotalGb", broker.getDiskTotalGb());
        summary.put("bootstrap", broker.getBootstrap());
        return summary;
    }

    private OffsetDateTime parseOffsetDateTime(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String firstIp(String ipAddresses) {
        if (ipAddresses == null || ipAddresses.isBlank() || "[]".equals(ipAddresses)) {
            return "";
        }
        try {
            List<String> ips = objectMapper.readValue(ipAddresses, new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});
            return ips.isEmpty() ? "" : ips.get(0);
        } catch (Exception ignored) {
            return ipAddresses.replaceAll("\\[|\\]|\\\"", "").split(",")[0].trim();
        }
    }

    private boolean looksLikeIpAddress(String value) {
        return value != null && (value.matches("^\\d{1,3}(?:\\.\\d{1,3}){3}$") || value.contains(":"));
    }

    private String extractBootstrapHost(String bootstrap) {
        if (bootstrap == null || bootstrap.isBlank()) {
            return "";
        }
        String first = bootstrap.split(",")[0].trim();
        int portIndex = first.lastIndexOf(':');
        return portIndex > 0 ? first.substring(0, portIndex) : first;
    }

    private int[] parseKafkaVersion(String kafkaVersion) {
        int[] fallback = new int[] {0, 0, 0};
        if (kafkaVersion == null || kafkaVersion.isBlank()) {
            return fallback;
        }
        String[] parts = kafkaVersion.trim().split("\\.");
        int[] parsed = new int[] {0, 0, 0};
        for (int i = 0; i < Math.min(parts.length, 3); i++) {
            try {
                parsed[i] = Integer.parseInt(parts[i].replaceAll("[^0-9].*$", ""));
            } catch (NumberFormatException e) {
                parsed[i] = 0;
            }
        }
        return parsed;
    }

    private int parseIntConfig(Object value, int defaultValue) {
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

    private int requiredPort(Object value, String name) {
        int port = parseIntConfig(value, -1);
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(name + " is required and must be between 1 and 65535");
        }
        return port;
    }

    private String resolveAgentArtifactUrl(String artifactUrl) {
        if (artifactUrl == null || artifactUrl.isBlank()) {
            return artifactUrl;
        }

        String trimmed = artifactUrl.trim();
        try {
            URI uri = URI.create(trimmed);
            if (!uri.isAbsolute()) {
                return trimmed.startsWith("/api/v1/artifacts/") ? joinArtifactRepoBase(trimmed) : trimmed;
            }

            String rawPath = uri.getRawPath();
            if (rawPath != null && rawPath.startsWith("/api/v1/artifacts/")) {
                return joinArtifactRepoBase(pathAndQuery(uri));
            }
            if (rawPath != null && rawPath.contains("/api/v1/artifacts/")) {
                return joinArtifactRepoBase(rawPath.substring(rawPath.indexOf("/api/v1/artifacts/"))
                        + (uri.getRawQuery() == null || uri.getRawQuery().isBlank() ? "" : "?" + uri.getRawQuery()));
            }
            if (isLoopbackHost(uri.getHost())) {
                return joinArtifactRepoBase(pathAndQuery(uri));
            }
        } catch (IllegalArgumentException ignored) {
            // Leave custom or malformed URLs unchanged; validation happens when the agent downloads.
        }
        return trimmed;
    }

    private String joinArtifactRepoBase(String pathAndQuery) {
        String base = artifactRepositoryProperties.getPublicUrl().toString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        if (pathAndQuery == null || pathAndQuery.isBlank()) {
            return base;
        }
        String normalizedPath = pathAndQuery.startsWith("/") ? pathAndQuery : "/" + pathAndQuery;
        return base + normalizedPath;
    }

    private String pathAndQuery(URI uri) {
        String rawPath = uri.getRawPath() != null ? uri.getRawPath() : "";
        return uri.getRawQuery() == null ? rawPath : rawPath + "?" + uri.getRawQuery();
    }

    private boolean isLoopbackHost(String host) {
        if (host == null) {
            return false;
        }
        return "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host) || "::1".equals(host);
    }

    private boolean initiateClusterCleanup(Cluster cluster) {
        if ("DELETING".equalsIgnoreCase(cluster.getStatus())) {
            return true;
        }
        if (cluster.getServices() == null || cluster.getServices().isEmpty()) {
            return false;
        }

        cluster.setStatus("DELETING");
        clusterRepository.save(cluster);
        Set<String> cleanupHosts = new HashSet<>();
        for (ClusterServiceAssignment svc : cluster.getServices()) {
            if (cleanupHosts.add(svc.getHostId())) {
                List<Map<String, Object>> dataServices = new ArrayList<>();
                boolean cleanupKafka = false;
                for (ClusterServiceAssignment onHost : cluster.getServices()) {
                    if (!svc.getHostId().equals(onHost.getHostId())) continue;
                    if ("schema_registry".equals(onHost.getRole()) || "kafka_connect".equals(onHost.getRole())) {
                        Map<String, Object> details = new LinkedHashMap<>(parseConfigJson(onHost.getConfigJson()));
                        details.put("kind", onHost.getRole());
                        dataServices.add(details);
                    } else {
                        cleanupKafka = true;
                    }
                }
                deploymentService.deleteClusterFromHost(cluster.getId(), svc.getHostId(),
                        cluster.getKafkaVersion(), cluster.getConfigJson(), dataServices, cleanupKafka);
            }
        }
        return true;
    }

    private void hardDeleteCluster(Cluster cluster, String action, boolean cleanupScheduled) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("clusterName", cluster.getName());
        details.put("kafkaClusterId", cluster.getKafkaClusterId());
        details.put("kafkaVersion", cluster.getKafkaVersion());
        details.put("mode", cluster.getMode());
        details.put("environment", cluster.getEnvironment());
        details.put("bootstrapServers", cluster.getBootstrapServers());
        details.put("createdAt", cluster.getCreatedAt());
        details.put("cleanupScheduled", cleanupScheduled);

        auditService.record("CLUSTER_CHANGE", action, "CLUSTER", cluster.getId().toString(),
                cluster.getId(), "SUCCESS", null, null, null, details);
        clearClusterHostAssignments(cluster);
        clusterRepository.purgeById(cluster.getId());
    }

    private void recordExternalClusterDeletion(ExternalCluster cluster, String action) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("clusterName", cluster.getName());
        details.put("kafkaClusterId", cluster.getKafkaClusterId());
        details.put("kafkaVersion", cluster.getKafkaVersion());
        details.put("mode", cluster.getKafkaMode());
        details.put("environment", cluster.getEnvironment());
        details.put("bootstrapServers", cluster.getBootstrapServers());
        details.put("createdAt", cluster.getCreatedAt());
        details.put("createdBy", cluster.getCreatedBy());

        auditService.record("CLUSTER_CHANGE", action, "CLUSTER", cluster.getId().toString(),
                cluster.getId(), "SUCCESS", null, null, null, details);
    }

    private void clearClusterHostAssignments(Cluster cluster) {
        if (cluster.getServices() == null) {
            return;
        }
        for (ClusterServiceAssignment service : cluster.getServices()) {
            hostRepository.findById(service.getHostId()).ifPresent(host -> {
                if (cluster.getId().equals(host.getClusterId())) {
                    host.setClusterId(null);
                    if ("OCCUPIED".equalsIgnoreCase(host.getStatus())) {
                        host.setStatus("ONLINE");
                    }
                    hostRepository.save(host);
                }
            });
        }
    }
}
