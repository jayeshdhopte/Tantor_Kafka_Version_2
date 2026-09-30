package io.translab.tantor.server.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStatus;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.domain.JobType;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.DiscoveryAgentRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.service.DeploymentService;
import io.translab.tantor.server.service.KafkaAdminService;
import io.translab.tantor.server.service.ActivityAlertService;
import io.translab.tantor.server.service.ConfigurationSanitizer;
import io.translab.tantor.server.service.JobService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import io.translab.tantor.server.domain.JobStepStatus;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.domain.JobStatus;
import io.translab.tantor.server.domain.JobStepStatus;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/config")
@RequiredArgsConstructor
@Slf4j
public class ConfigController {

    private final ClusterRepository clusterRepository;
    private final HostRepository hostRepository;
    private final DeploymentService deploymentService;
    private final KafkaAdminService kafkaAdminService;
    private final ObjectMapper objectMapper;
    private final ActivityAlertService activityAlertService;
    private final io.translab.tantor.server.service.ExternalClusterService externalClusterService;
    private final JobService jobService;
    private final DiscoveryAgentRepository discoveryAgentRepository;
    private final io.translab.tantor.server.repository.ExternalClusterRepository externalClusterRepository;
    private final io.translab.tantor.server.repository.ExternalClusterNodeRepository externalClusterNodeRepository;
    private final RoleAuthenticationUtil roleAuthenticationUtil;
    private final ConfigurationSanitizer configurationSanitizer;

    private ResponseEntity<Map<String, Object>> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.<String, Object>of("error", "Unauthorized"));
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> getBrokerConfigs(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_READ)) {
            return unauthorized();
        }
        Cluster cluster = clusterRepository.findById(clusterId).orElse(null);
        if (cluster != null && !"EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            Map<Integer, Map<String, Object>> dynamicConfigs = kafkaAdminService.getBrokerConfigs(clusterId);
            
            Map<String, Object> response = new java.util.HashMap<>();
            response.put("dynamicConfigs", dynamicConfigs);
            
            Map<String, Object> staticConfigs = new HashMap<>();
            Map<String, Object> deploymentConfig = new HashMap<>();
            
            try {
                if (cluster.getConfigJson() != null && !cluster.getConfigJson().isEmpty()) {
                    deploymentConfig = objectMapper.readValue(cluster.getConfigJson(), Map.class);
                }
            } catch(Exception e) {
                deploymentConfig = new HashMap<>();
            }

            String installDir = activeKafkaInstallDir(deploymentConfig);
            Map<String, Object> activeProperties = buildActiveServerProperties(cluster, deploymentConfig, installDir);
            String activeFilePath = activeServerConfigPath(cluster, installDir);
            
            staticConfigs.put("filePath", activeFilePath);
            staticConfigs.put("properties", activeProperties);
            staticConfigs.put("deploymentParameters", deploymentConfig);
            staticConfigs.put("configFiles", buildConfigFiles(cluster, deploymentConfig, installDir, activeFilePath, activeProperties));
            response.put("serviceTopology", buildServiceTopology(cluster, deploymentConfig, installDir));
            response.put("staticConfigs", staticConfigs);

            return ResponseEntity.ok(configurationSanitizer.sanitize(response));
        }

        io.translab.tantor.server.domain.ExternalCluster externalCluster = externalClusterRepository.findById(clusterId).orElse(null);
        if (externalCluster == null) return ResponseEntity.notFound().build();

        Map<String, Object> response = new HashMap<>();
        response.put("dynamicConfigs", new HashMap<>());
        
        List<Map<String, Object>> topology = new ArrayList<>();
        List<Map<String, Object>> configFiles = new ArrayList<>();
        
        List<io.translab.tantor.server.domain.ExternalClusterNode> nodes = externalClusterNodeRepository.findByClusterId(clusterId);
        List<io.translab.tantor.server.domain.DiscoveryAgent> allAgents = discoveryAgentRepository.findAll();
        
        for (io.translab.tantor.server.domain.ExternalClusterNode node : nodes) {
            Map<String, Object> topoNode = new HashMap<>();
            topoNode.put("hostId", node.getHost());
            topoNode.put("hostAddress", node.getHost());
            
            String role = "Broker";
            if (Boolean.TRUE.equals(node.getIsBroker()) && Boolean.TRUE.equals(node.getIsController())) role = "Broker + Controller";
            else if (Boolean.TRUE.equals(node.getIsController())) role = "Controller";
            
            topoNode.put("role", role);
            topoNode.put("nodeId", node.getNodeId() != null ? node.getNodeId() : Math.abs(node.getHost().hashCode()));
            topoNode.put("isBroker", node.getIsBroker());
            topoNode.put("isController", node.getIsController());
            topoNode.put("serviceName", "kafka.service"); // fallback, actual restart uses agent's discovered service
            String configPath = node.getConfigFile();
            topoNode.put("configPath", configPath);
            topoNode.put("configFilePath", configPath);
            topoNode.put("configFileName", configPath != null ? new java.io.File(configPath).getName() : "server.properties");
            
            boolean canExecute = false;
            for (io.translab.tantor.server.domain.DiscoveryAgent agent : allAgents) {
                if (matchesDiscoveryAgent(agent, node.getHost()) && "ONLINE".equalsIgnoreCase(agent.getStatus())) {
                    canExecute = Boolean.TRUE.equals(agent.getCanExecuteTasks());
                    break;
                }
            }
            topoNode.put("canExecuteTasks", canExecute);
            
            topology.add(topoNode);
            
            Map<String, Object> staticFile = new HashMap<>();
            staticFile.put("id", "ext_" + topoNode.get("nodeId"));
            staticFile.put("nodeId", topoNode.get("nodeId"));
            staticFile.put("serviceId", node.getId().toString());
            staticFile.put("hostId", node.getHost());
            staticFile.put("label", role + " Properties (" + node.getHost() + ")");
            staticFile.put("path", configPath);
            staticFile.put("role", role);
            staticFile.put("properties", new HashMap<>());
            configFiles.add(staticFile);
        }
        
        response.put("serviceTopology", topology);
        Map<String, Object> staticConfigs = new HashMap<>();
        staticConfigs.put("configFiles", configFiles);
        response.put("staticConfigs", staticConfigs);
        
        return ResponseEntity.ok(configurationSanitizer.sanitize(response));


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

    @PostMapping("/rolling-apply")
    public ResponseEntity<Map<String, Object>> rollingApply(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody Map<String, Object> payload) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_CHANGE)) {
            return unauthorized();
        }
        Cluster cluster = clusterRepository.findById(clusterId).orElse(null);
        if (cluster == null) return ResponseEntity.notFound().build();

        Job job = new Job();
        job.setType(JobType.ROLLING_CONFIG_UPDATE);
        job.setStatus(JobStatus.PENDING);
        job.setResourceKey("cluster:" + clusterId);
        job.setRollbackSupported(false);

        try {
            Map<String, Object> jobPayload = new HashMap<>();
            jobPayload.put("clusterId", clusterId.toString());
            jobPayload.put("rollingRestart", payload.get("rollingRestart"));
            jobPayload.put("changes", payload.get("changes"));
            job.setPayload(objectMapper.writeValueAsString(jobPayload));
        } catch (JsonProcessingException e) {
            return ResponseEntity.badRequest().build();
        }

        List<JobStep> steps = new ArrayList<>();
        int order = 1;
        steps.add(createStep("ALL", "PREFLIGHT", order++));
        steps.add(createStep("ALL", "BACKUP_ALL", order++));

        List<Map<String, Object>> changes = (List<Map<String, Object>>) payload.get("changes");
        if (changes != null) {
            for (Map<String, Object> change : changes) {
                String host = (String) change.get("host");
                steps.add(createStep(host, "WRITE_CONFIG: " + host, order++));
                if (Boolean.TRUE.equals(payload.get("rollingRestart"))) {
                    steps.add(createStep(host, "RESTART_SERVICE: " + host, order++));
                    steps.add(createStep(host, "HEALTH_CHECK: " + host, order++));
                }
            }
        }
        steps.add(createStep("ALL", "FINAL_VERIFY", order++));

        Job savedJob = jobService.createJob(job, steps);
        
        activityAlertService.logActivity("INFO", "Requested rolling config update for cluster: " + cluster.getName(), clusterId);
        
        return ResponseEntity.ok(Map.of("jobId", savedJob.getId().toString()));
    }

    private JobStep createStep(String targetId, String name, int order) {
        JobStep step = new JobStep();
        step.setTargetId(targetId);
        step.setName(name);
        step.setStepOrder(order);
        step.setStatus(JobStepStatus.PENDING);
        return step;
    }

    private String activeKafkaInstallDir(Map<String, Object> config) {
        String configured = stringConfig(config, "kafka_install_base_dir", stringConfig(config, "kafka_install_dir", "/opt")).trim();
        if (configured.isBlank()) {
            configured = "/opt";
        }
        configured = trimTrailingSlash(configured);
        if (configured.endsWith("/kafka")) {
            return configured;
        }
        String leaf = configured.substring(configured.lastIndexOf('/') + 1);
        if (leaf.startsWith("kafka_")) {
            int lastSlash = configured.lastIndexOf('/');
            return (lastSlash <= 0 ? "" : configured.substring(0, lastSlash)) + "/kafka";
        }
        return configured + "/kafka";
    }

    private String defaultKafkaDataDir(Map<String, Object> config) {
        String configured = stringConfig(config, "kafka_install_base_dir", stringConfig(config, "kafka_install_dir", "/opt")).trim();
        if (configured.isBlank()) {
            configured = "/opt";
        }
        configured = trimTrailingSlash(configured);
        if ("/opt".equals(configured) || "/".equals(configured)) {
            return "/data/kafka";
        }
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
    private List<Map<String, Object>> buildConfigFiles(
            Cluster cluster,
            Map<String, Object> config,
            String installDir,
            String activeFilePath,
            Map<String, Object> activeProperties
    ) {
        List<Map<String, Object>> files = new ArrayList<>();

        if (!"EXTERNAL".equalsIgnoreCase(cluster.getMode()) && cluster.getServices() != null && !cluster.getServices().isEmpty()) {
            for (ClusterServiceAssignment service : cluster.getServices()) {
                if ("zookeeper".equalsIgnoreCase(service.getRole())) continue;
                Map<String, Object> serviceConfig = serviceConfig(config, service);
                String serviceInstallDir = activeKafkaInstallDir(serviceConfig);
                Map<String, Object> properties = storedProperties(service);
                if (properties.isEmpty()) {
                    properties = "zookeeper".equalsIgnoreCase(cluster.getMode())
                            ? buildZooKeeperServiceProperties(cluster, serviceConfig, serviceInstallDir, service)
                            : buildKraftServiceProperties(cluster, serviceConfig, serviceInstallDir, service);
                }
                files.add(configFile(
                        serviceConfigId(service),
                        serviceConfigLabel(service),
                        serviceConfigDescription(service),
                    serviceConfigPath(service.getRole(), cluster.getMode(), cluster.getKafkaVersion(), serviceInstallDir),
                        service.getRole(),
                        true,
                        properties,
                        service
                ));
            }
            return files;
        }

        files.add(configFile(
                "active-server",
                "Active Server Config",
                "server.properties used by the Kafka service",
                activeFilePath,
                "server",
                true,
                activeProperties,
                null
        ));

        return files;
    }
    private Map<String, Object> configFile(
            String id,
            String label,
            String description,
            String path,
            String role,
            boolean active,
            Map<String, Object> properties,
            ClusterServiceAssignment service
    ) {
        Map<String, Object> file = new LinkedHashMap<>();
        file.put("id", id);
        file.put("label", label);
        file.put("description", description);
        file.put("path", path);
        file.put("role", role);
        file.put("active", active);
        file.put("properties", properties);
        if (service != null) {
            file.put("serviceId", service.getId());
            file.put("hostId", service.getHostId());
            file.put("nodeId", service.getNodeId());
        }
        return file;
    }

    private List<Map<String, Object>> buildServiceTopology(Cluster cluster, Map<String, Object> config, String installDir) {
        List<Map<String, Object>> topology = new ArrayList<>();
        if (cluster.getServices() == null) {
            return topology;
        }
        for (ClusterServiceAssignment service : cluster.getServices()) {
            Map<String, Object> serviceConfig = serviceConfig(config, service);
            String serviceInstallDir = activeKafkaInstallDir(serviceConfig);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("hostId", service.getHostId());
            item.put("hostAddress", hostAddressForService(service));
            item.put("role", service.getRole());
            item.put("nodeId", service.getNodeId());
            item.put("isBroker", isBrokerRole(service.getRole()));
            item.put("isController", isControllerRole(service.getRole()));
            item.put("serviceName", serviceNameForRole(service.getRole()) + ".service");
            item.put("systemdUnit", serviceNameForRole(service.getRole()) + ".service");
            item.put("configPath", serviceConfigPath(service.getRole(), cluster.getMode(), cluster.getKafkaVersion(), serviceInstallDir));
            item.put("listenerPort", isBrokerRole(service.getRole()) ? requiredPort(serviceConfig, "listener_port") : "");
            item.put("controllerPort", isControllerRole(service.getRole()) ? requiredPort(serviceConfig, "controller_port") : "");
            item.put("logDirs", isBrokerRole(service.getRole()) ? brokerLogDirs(serviceConfig, defaultKafkaDataDir(serviceConfig)) : "");
            item.put("metadataLogDir", metadataLogDirForRole(service.getRole(), serviceConfig, defaultKafkaDataDir(serviceConfig)));
            
            if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
                discoveryAgentRepository.findById(service.getHostId()).ifPresentOrElse(agent -> {
                    item.put("agentStatus", agent.getStatus());
                    item.put("managedStatus", "managed");
                    item.put("canExecuteTasks", agent.getCanExecuteTasks() != null && agent.getCanExecuteTasks());
                }, () -> {
                    item.put("agentStatus", "OFFLINE");
                    item.put("managedStatus", "unmanaged");
                    item.put("canExecuteTasks", false);
                });
            } else {
                item.put("agentStatus", "ONLINE");
                item.put("managedStatus", "managed");
                item.put("canExecuteTasks", true);
            }
            
            topology.add(item);
        }
        return topology;
    }

    private String serviceConfigId(ClusterServiceAssignment service) {
        return service.getRole() + "-" + (service.getNodeId() == null ? "unknown" : service.getNodeId());
    }

    private String serviceConfigLabel(ClusterServiceAssignment service) {
        String role = service.getRole() == null ? "Kafka" : service.getRole().replace('_', ' ');
        return capitalizeWords(role) + " Node " + (service.getNodeId() == null ? "" : service.getNodeId());
    }

    private String serviceConfigDescription(ClusterServiceAssignment service) {
        return serviceNameForRole(service.getRole()) + ".service config for host " + service.getHostId();
    }

    private String serviceConfigPath(String role, String mode, String kafkaVersion, String installDir) {
        if ("zookeeper".equalsIgnoreCase(mode)) {
            if ("zookeeper".equals(role)) return installDir + "/config/zookeeper.properties";
            return installDir + "/config/server.properties";
        }
        String configRoot = kafkaMajorVersion(kafkaVersion) >= 4 ? installDir + "/config" : installDir + "/config/kraft";
        if ("controller".equals(role)) return configRoot + "/controller.properties";
        if ("broker".equals(role)) return configRoot + "/broker.properties";
        return configRoot + "/server.properties";
    }

    private int kafkaMajorVersion(String version) {
        if (version == null) return 0;
        try { return Integer.parseInt(version.trim().replaceFirst("^[vV]", "").split("\\.")[0]); }
        catch (Exception e) {
            log.warn("Failed to parse Kafka major version from '{}'", version, e);
            return 0;
        }
    }

    private Map<String, Object> buildKraftServiceProperties(Cluster cluster, Map<String, Object> config, String installDir, ClusterServiceAssignment service) {
        Map<String, Object> props = new LinkedHashMap<>();
        String role = service.getRole();
        String host = hostAddressForService(service);
        String nodeId = service.getNodeId() == null ? "1" : String.valueOf(service.getNodeId());
        String listenerPort = isBrokerRole(role) ? requiredPort(config, "listener_port") : "";
        String controllerPort = isControllerRole(role) ? requiredPort(config, "controller_port") : "";
        String dataDir = defaultKafkaDataDir(config);
        String quorumVoters = requiredConfig(config, "quorum_voters");

        props.put("process.roles", processRoles(role));
        props.put("node.id", nodeId);
        props.put("controller.quorum.voters", quorumVoters);
        if (isBrokerRole(role) && isControllerRole(role)) {
            props.put("listeners", "PLAINTEXT://" + host + ":" + listenerPort + ",CONTROLLER://" + host + ":" + controllerPort);
        } else if (isControllerRole(role)) {
            props.put("listeners", "CONTROLLER://" + host + ":" + controllerPort);
        } else {
            props.put("listeners", "PLAINTEXT://" + host + ":" + listenerPort);
        }
        if (isBrokerRole(role)) {
            props.put("advertised.listeners", "PLAINTEXT://" + host + ":" + listenerPort);
            props.put("inter.broker.listener.name", "PLAINTEXT");
            props.put("log.dirs", brokerLogDirs(config, dataDir));
        }
        props.put("controller.listener.names", "CONTROLLER");
        props.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL");
        props.put("metadata.log.dir", metadataLogDirForRole(role, config, dataDir));
        if (isBrokerRole(role)) {
            props.put("num.partitions", stringConfig(config, "num_partitions", "1"));
            String replicationFactor = stringConfig(config, "replication_factor", "1");
            String minIsr = stringConfig(config, "min_insync_replicas", "1");
            props.put("default.replication.factor", replicationFactor);
            props.put("min.insync.replicas", minIsr);
            props.put("offsets.topic.replication.factor", replicationFactor);
            props.put("transaction.state.log.replication.factor", replicationFactor);
            props.put("transaction.state.log.min.isr", minIsr);
        }
        return props;
    }

    private Map<String, Object> buildZooKeeperServiceProperties(Cluster cluster, Map<String, Object> config, String installDir, ClusterServiceAssignment service) {
        if ("zookeeper".equals(service.getRole())) {
            return buildZooKeeperProperties(config, installDir);
        }
        Map<String, Object> props = new LinkedHashMap<>();
        String host = hostAddressForService(service);
        String nodeId = service.getNodeId() == null ? "1" : String.valueOf(service.getNodeId());
        String listenerPort = requiredPort(config, "listener_port");
        String dataDir = stringConfig(config, "kafka_data_dir", defaultKafkaDataDir(config));
        props.put("broker.id", nodeId);
        props.put("listeners", "PLAINTEXT://" + host + ":" + listenerPort);
        props.put("advertised.listeners", "PLAINTEXT://" + host + ":" + listenerPort);
        props.put("zookeeper.connect", requiredConfig(config, "zookeeper_connect"));
        props.put("zookeeper.connection.timeout.ms", "18000");
        props.put("log.dirs", brokerLogDirs(config, dataDir));
        props.put("num.partitions", stringConfig(config, "num_partitions", "1"));
        String replicationFactor = stringConfig(config, "replication_factor", "1");
        props.put("default.replication.factor", replicationFactor);
        props.put("min.insync.replicas", stringConfig(config, "min_insync_replicas", "1"));
        props.put("offsets.topic.replication.factor", replicationFactor);
        props.put("transaction.state.log.replication.factor", replicationFactor);
        props.put("transaction.state.log.min.isr", stringConfig(config, "min_insync_replicas", "1"));
        return props;
    }

    private String hostAddressForService(ClusterServiceAssignment service) {
        if (service == null || service.getHostId() == null || service.getHostId().isBlank()) {
            throw new IllegalArgumentException("A concrete host assignment is required for service configuration");
        }
        String ip = hostRepository.findById(service.getHostId())
                .map(host -> firstAddressFromJson(host.getIpAddresses()))
                .orElse("");
        return ip.isBlank() ? service.getHostId() : ip;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> serviceConfig(Map<String, Object> clusterConfig, ClusterServiceAssignment service) {
        Map<String, Object> result = new HashMap<>(clusterConfig);
        if (service.getConfigJson() == null || service.getConfigJson().isBlank()) return result;
        try {
            Map<String, Object> stored = objectMapper.readValue(service.getConfigJson(), Map.class);
            if (stored != null) result.putAll(stored);
        } catch (Exception ignored) {
            // Fall back to cluster-level deployment settings for legacy assignments.
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> storedProperties(ClusterServiceAssignment service) {
        if (service.getConfigJson() == null || service.getConfigJson().isBlank()) return new LinkedHashMap<>();
        try {
            Map<String, Object> stored = objectMapper.readValue(service.getConfigJson(), Map.class);
            Object properties = stored == null ? null : stored.get("properties");
            return properties instanceof Map<?, ?> ? new LinkedHashMap<>((Map<String, Object>) properties) : new LinkedHashMap<>();
        } catch (Exception ignored) {
            return new LinkedHashMap<>();
        }
    }

    private String serviceNameForRole(String role) {
        if ("controller".equals(role)) return "controller";
        if ("zookeeper".equals(role)) return "zookeeper";
        if ("broker_controller".equals(role) || "broker_zookeeper".equals(role)) return "kafka";
        return "broker";
    }

    private String brokerLogDirs(Map<String, Object> config, String dataDir) {
        String configured = stringConfig(config, "log_dirs", "");
        return configured.isBlank() ? dataDir + "/broker-data" : configured;
    }

    private String metadataLogDirForRole(String role, Map<String, Object> config, String dataDir) {
        String configured = stringConfig(config, "metadata_log_dir", "");
        if (!configured.isBlank()) return configured;
        if ("controller".equals(role)) return dataDir + "/controller-data/metadata";
        return dataDir + "/broker-metadata";
    }

    private boolean isBrokerRole(String role) {
        return "broker".equals(role) || "broker_controller".equals(role) || "broker_zookeeper".equals(role);
    }

    private boolean isControllerRole(String role) {
        return "controller".equals(role) || "broker_controller".equals(role);
    }

    private String capitalizeWords(String value) {
        StringBuilder result = new StringBuilder();
        for (String part : value.split(" ")) {
            if (part.isBlank()) continue;
            if (result.length() > 0) result.append(' ');
            result.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
        }
        return result.toString();
    }
    private String activeServerConfigPath(Cluster cluster, String installDir) {
        return "zookeeper".equalsIgnoreCase(cluster.getMode())
                ? installDir + "/config/server.properties"
                : installDir + "/config/kraft/server.properties";
    }

    private Map<String, Object> buildActiveServerProperties(Cluster cluster, Map<String, Object> config, String installDir) {
        return "zookeeper".equalsIgnoreCase(cluster.getMode())
                ? buildZooKeeperBackedBrokerProperties(cluster, config, installDir)
                : buildKraftServerProperties(cluster, config, installDir);
    }

    private Map<String, Object> buildKraftServerProperties(Cluster cluster, Map<String, Object> config, String installDir) {
        Map<String, Object> props = new LinkedHashMap<>();
        String host = firstBrokerHost(cluster, config);
        String nodeId = firstNodeId(cluster, config);
        String listenerPort = requiredPort(config, "listener_port");
        String controllerPort = requiredPort(config, "controller_port");
        String dataDir = stringConfig(config, "kafka_data_dir", defaultKafkaDataDir(config));
        String logDirs = stringConfig(config, "log_dirs", dataDir + "/kafka-logs");
        String role = processRoles(stringConfig(config, "role", firstServiceRole(cluster)));
        String quorumVoters = requiredConfig(config, "quorum_voters");
        String listeners = stringConfig(config, "listeners", "PLAINTEXT://" + host + ":" + listenerPort + ",CONTROLLER://" + host + ":" + controllerPort);
        String advertisedListeners = stringConfig(config, "advertised_listeners",
                stringConfig(config, "advertised.listeners", "PLAINTEXT://" + host + ":" + listenerPort));

        props.put("process.roles", role);
        props.put("node.id", nodeId);
        props.put("controller.quorum.voters", quorumVoters);
        props.put("listeners", listeners);
        props.put("inter.broker.listener.name", "PLAINTEXT");
        props.put("advertised.listeners", advertisedListeners);
        props.put("controller.listener.names", "CONTROLLER");
        props.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL");
        props.put("log.dirs", logDirs);
        props.put("num.partitions", stringConfig(config, "num_partitions", "1"));
        String replicationFactor = stringConfig(config, "replication_factor", "1");
        props.put("offsets.topic.replication.factor", replicationFactor);
        props.put("transaction.state.log.replication.factor", replicationFactor);
        props.put("transaction.state.log.min.isr", "1");
        return props;
    }

    private Map<String, Object> buildZooKeeperBackedBrokerProperties(Cluster cluster, Map<String, Object> config, String installDir) {
        Map<String, Object> props = new LinkedHashMap<>();
        String host = firstBrokerHost(cluster, config);
        String nodeId = firstNodeId(cluster, config);
        String listenerPort = requiredPort(config, "listener_port");
        String dataDir = stringConfig(config, "kafka_data_dir", defaultKafkaDataDir(config));
        String logDirs = stringConfig(config, "log_dirs", dataDir + "/kafka-logs");
        String zookeeperConnect = requiredConfig(config, "zookeeper_connect");

        props.put("broker.id", nodeId);
        props.put("listeners", "PLAINTEXT://" + host + ":" + listenerPort);
        props.put("advertised.listeners", "PLAINTEXT://" + host + ":" + listenerPort);
        props.put("zookeeper.connect", zookeeperConnect);
        props.put("zookeeper.connection.timeout.ms", "18000");
        props.put("log.dirs", logDirs);
        props.put("num.partitions", stringConfig(config, "num_partitions", "1"));
        String replicationFactor = stringConfig(config, "replication_factor", "1");
        props.put("offsets.topic.replication.factor", replicationFactor);
        props.put("transaction.state.log.replication.factor", replicationFactor);
        props.put("transaction.state.log.min.isr", "1");
        return props;
    }

    private Map<String, Object> buildZooKeeperProperties(Map<String, Object> config, String installDir) {
        Map<String, Object> props = new LinkedHashMap<>();
        String dataDir = stringConfig(config, "zookeeper_data_dir", defaultKafkaDataDir(config) + "/zookeeper-data");
        props.put("tickTime", "2000");
        props.put("initLimit", "10");
        props.put("syncLimit", "5");
        props.put("dataDir", dataDir);
        props.put("clientPort", requiredPort(config, "zookeeper_port"));
        props.put("maxClientCnxns", "0");
        props.put("admin.enableServer", "false");
        Object servers = config.get("zookeeper_servers");
        if (servers != null && !String.valueOf(servers).isBlank()) {
            props.put("servers", servers);
        }
        return props;
    }

    private String stringConfig(Map<String, Object> config, String key, String defaultValue) {
        Object value = config.get(key);
        if (value == null || String.valueOf(value).isBlank()) {
            return defaultValue;
        }
        return String.valueOf(value);
    }

    private String firstNodeId(Cluster cluster, Map<String, Object> config) {
        Object configured = config.get("node_id");
        if (configured != null && !String.valueOf(configured).isBlank()) {
            return String.valueOf(configured);
        }
        if (cluster.getServices() != null && !cluster.getServices().isEmpty() && cluster.getServices().get(0).getNodeId() != null) {
            return String.valueOf(cluster.getServices().get(0).getNodeId());
        }
        return "1";
    }

    private String firstServiceRole(Cluster cluster) {
        if (cluster.getServices() != null && !cluster.getServices().isEmpty() && cluster.getServices().get(0).getRole() != null) {
            return cluster.getServices().get(0).getRole();
        }
        return "broker_controller";
    }

    private String processRoles(String role) {
        if (role == null || role.isBlank() || "broker_controller".equalsIgnoreCase(role)) {
            return "broker,controller";
        }
        if ("broker_zookeeper".equalsIgnoreCase(role)) {
            return "broker";
        }
        return role.replace('_', ',');
    }

    private String firstBrokerHost(Cluster cluster, Map<String, Object> config) {
        String listeners = stringConfig(config, "advertised_listeners", stringConfig(config, "advertised.listeners", ""));
        if (!listeners.isBlank()) {
            String host = hostFromEndpoint(listeners.split(",")[0]);
            if (!host.isBlank()) return host;
        }
        if (cluster.getBootstrapServers() != null && !cluster.getBootstrapServers().isBlank()) {
            String host = hostFromEndpoint(cluster.getBootstrapServers().split(",")[0]);
            if (!host.isBlank()) return host;
        }
        String assignedHost = assignedHostAddress(cluster);
        if (!assignedHost.isBlank()) return assignedHost;
        throw new IllegalArgumentException("A broker host is required; localhost fallback is forbidden");
    }

    private String requiredConfig(Map<String, Object> config, String key) {
        String value = stringConfig(config, key, "");
        if (value.isBlank()) throw new IllegalArgumentException(key + " is required");
        return value;
    }

    private String requiredPort(Map<String, Object> config, String key) {
        String value = requiredConfig(config, key);
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) throw new NumberFormatException();
            return String.valueOf(port);
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(key + " must be between 1 and 65535", error);
        }
    }

    private String assignedHostAddress(Cluster cluster) {
        if (cluster.getServices() == null || cluster.getServices().isEmpty()) {
            return "";
        }
        for (ClusterServiceAssignment service : cluster.getServices()) {
            String hostId = service.getHostId();
            if (hostId == null || hostId.isBlank()) {
                continue;
            }
            String ip = hostRepository.findById(hostId)
                    .map(host -> firstAddressFromJson(host.getIpAddresses()))
                    .orElse("");
            if (!ip.isBlank()) {
                return ip;
            }
        }
        return "";
    }

    private String firstAddressFromJson(String ipAddressesJson) {
        if (ipAddressesJson == null || ipAddressesJson.isBlank()) {
            return "";
        }
        try {
            List<?> addresses = objectMapper.readValue(ipAddressesJson, List.class);
            for (Object address : addresses) {
                String value = String.valueOf(address);
                if (!value.isBlank() && !value.startsWith("127.") && !"localhost".equalsIgnoreCase(value)) {
                    return value;
                }
            }
        } catch (Exception ignored) {
            String cleaned = ipAddressesJson.replace("[", "").replace("]", "").replace("\"", "");
            for (String part : cleaned.split(",")) {
                String value = part.trim();
                if (!value.isBlank() && !value.startsWith("127.") && !"localhost".equalsIgnoreCase(value)) {
                    return value;
                }
            }
        }
        return "";
    }

    private String hostFromEndpoint(String endpoint) {
        if (endpoint == null) return "";
        String value = endpoint.trim();
        int scheme = value.indexOf("://");
        if (scheme >= 0) {
            value = value.substring(scheme + 3);
        }
        int slash = value.indexOf('/');
        if (slash >= 0) {
            value = value.substring(0, slash);
        }
        int colon = value.lastIndexOf(':');
        if (colon > 0) {
            value = value.substring(0, colon);
        }
        return value.trim();
    }

    @Deprecated
    @PutMapping("/unsafe-legacy/services/{serviceId}")
    public ResponseEntity<?> updateServiceConfig(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID serviceId,
            @RequestBody ServiceConfigUpdateRequest request
    ) throws JsonProcessingException {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_CHANGE)) {
            return unauthorized();
        }
        Cluster cluster = clusterRepository.findById(clusterId).orElse(null);
        if (cluster == null) return ResponseEntity.notFound().build();
        ClusterServiceAssignment service = cluster.getServices() == null ? null : cluster.getServices().stream()
                .filter(item -> serviceId.equals(item.getId()))
                .findFirst()
                .orElse(null);
        if (service == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "Service assignment does not belong to this cluster."));
        }
        if (request.getProperties() == null || request.getProperties().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "At least one configuration property is required."));
        }
        for (String key : request.getProperties().keySet()) {
            if (key == null || !key.matches("[A-Za-z0-9._-]+")) {
                return ResponseEntity.badRequest().body(Map.of("message", "Invalid configuration key: " + key));
            }
        }

        Map<String, Object> deploymentConfig = new HashMap<>();
        if (cluster.getConfigJson() != null && !cluster.getConfigJson().isBlank()) {
            Map<String, Object> parsed = objectMapper.readValue(cluster.getConfigJson(), Map.class);
            if (parsed != null) deploymentConfig.putAll(parsed);
        }
        deploymentConfig.putAll(serviceConfig(deploymentConfig, service));
        deploymentConfig.put("mode", cluster.getMode());
        deploymentConfig.put("version", cluster.getKafkaVersion());

        String previousServiceConfigJson = service.getConfigJson() == null ? "{}" : service.getConfigJson();
        String previousPropertiesTemplate = "";
        try {
            Map<String, Object> previousStored = objectMapper.readValue(previousServiceConfigJson, Map.class);
            Object previousProperties = previousStored.get("properties");
            if (previousProperties instanceof Map<?, ?> propertyMap) {
                Map<String, Object> normalized = new LinkedHashMap<>();
                propertyMap.forEach((key, value) -> normalized.put(String.valueOf(key), value));
                previousPropertiesTemplate = serializeProperties(normalized);
            }
        } catch (Exception ignored) {
            // An empty rollback template is safer than rejecting a valid forward change.
        }

        Map<String, Object> stored = new HashMap<>(deploymentConfig);
        stored.put("properties", new LinkedHashMap<>(request.getProperties()));
        service.setConfigJson(objectMapper.writeValueAsString(stored));
        clusterRepository.save(cluster);

        String propertiesTemplate = serializeProperties(request.getProperties());
        Map<String, Object> stepPayload = new LinkedHashMap<>();
        stepPayload.put("operation", "service");
        stepPayload.put("hostId", service.getHostId());
        stepPayload.put("role", service.getRole());
        stepPayload.put("nodeId", service.getNodeId() == null ? "1" : String.valueOf(service.getNodeId()));
        stepPayload.put("configJson", objectMapper.writeValueAsString(deploymentConfig));
        stepPayload.put("propertiesTemplate", propertiesTemplate);
        stepPayload.put("previousConfigJson", previousServiceConfigJson);
        stepPayload.put("previousPropertiesTemplate", previousPropertiesTemplate);
        stepPayload.put("restart", request.isRestart());

        Job job = new Job();
        job.setType(JobType.CONFIG_CHANGE);
        job.setStatus(JobStatus.PENDING);
        job.setRollbackSupported(true);
        job.setResourceKey("cluster:" + clusterId);
        job.setPayload(objectMapper.writeValueAsString(Map.of("clusterId", clusterId.toString())));
        JobStep step = new JobStep();
        step.setStepOrder(1);
        step.setTargetId(service.getHostId());
        step.setName("Update " + serviceConfigPath(service.getRole(), cluster.getMode(), cluster.getKafkaVersion(), activeKafkaInstallDir(deploymentConfig))
                + " on " + service.getHostId());
        step.setPayload(objectMapper.writeValueAsString(stepPayload));
        Job savedJob = jobService.createJob(job, List.of(step));
        activityAlertService.logAudit("INFO", "CONFIGURATION", "UPDATE",
                "Updated " + serviceConfigPath(service.getRole(), cluster.getMode(), cluster.getKafkaVersion(), activeKafkaInstallDir(deploymentConfig))
                        + " on " + service.getHostId() + (request.isRestart() ? " and queued service restart" : ""),
                "SERVICE", serviceId.toString(), clusterId,
                auditProperties(previousPropertiesTemplate), auditProperties(propertiesTemplate), "QUEUED", null,
                "jobId=" + savedJob.getId());
        return ResponseEntity.ok(Map.of("jobId", savedJob.getId().toString(), "status", "scheduled"));
    }

    private String serializeProperties(Map<String, Object> properties) {
        StringBuilder result = new StringBuilder();
        properties.forEach((key, value) -> {
            if ("servers".equals(key)) return;
            result.append(key).append('=').append(value == null ? "" : String.valueOf(value).replace("\r", "").replace("\n", " ")).append('\n');
        });
        return result.toString();
    }

    @PutMapping("/bulk")
    public ResponseEntity<?> updateConfigBulk(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody BulkConfigRequest request) throws JsonProcessingException {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_CHANGE)) {
            return unauthorized();
        }
        Cluster cluster = clusterRepository.findById(clusterId).orElse(null);
        if (cluster == null) return ResponseEntity.notFound().build();
        if (request.getConfigKey() == null || !request.getConfigKey().matches("[A-Za-z0-9._-]+")) {
            return ResponseEntity.badRequest().body(Map.of("message", "A valid configuration key is required."));
        }

        List<JobStep> steps = new ArrayList<>();
        Map<Integer, Map<String, Object>> currentConfigs = kafkaAdminService.getBrokerConfigs(clusterId);
        Map<String, Object> previousByBroker = new LinkedHashMap<>();
        currentConfigs.forEach((brokerId, config) -> previousByBroker.put(
                String.valueOf(brokerId), String.valueOf(config.getOrDefault(request.getConfigKey(), ""))));

        JobStep dynamicStep = new JobStep();
        dynamicStep.setStepOrder(1);
        dynamicStep.setName("Apply dynamic broker configuration " + request.getConfigKey());
        dynamicStep.setTargetId(clusterId.toString());
        dynamicStep.setPayload(objectMapper.writeValueAsString(Map.of(
                "operation", "dynamic",
                "configKey", request.getConfigKey(),
                "configValue", request.getConfigValue() == null ? "" : request.getConfigValue(),
                "previousByBroker", previousByBroker
        )));
        steps.add(dynamicStep);

        if (request.isApplyToAgents() && "EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            JobStep externalStep = new JobStep();
            externalStep.setStepOrder(2);
            externalStep.setName("Persist configuration through discovery agents");
            externalStep.setTargetId(clusterId.toString());
            externalStep.setPayload(objectMapper.writeValueAsString(Map.of(
                    "operation", "external",
                    "configKey", request.getConfigKey(),
                    "configValue", request.getConfigValue() == null ? "" : request.getConfigValue(),
                    "previousValue", previousByBroker.values().stream().findFirst().orElse(""),
                    "restart", request.isRestart()
            )));
            steps.add(externalStep);
        }

        if (request.isApplyToAgents() && !"EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            Map<String, Object> oldConfig = cluster.getConfigJson() == null || cluster.getConfigJson().isBlank()
                    ? new HashMap<>() : objectMapper.readValue(cluster.getConfigJson(), Map.class);
            Map<String, Object> newConfig = new HashMap<>(oldConfig);
            newConfig.put(request.getConfigKey(), request.getConfigValue());
            String oldConfigJson = objectMapper.writeValueAsString(oldConfig);
            String newConfigJson = objectMapper.writeValueAsString(newConfig);
            int order = 2;
            for (ClusterServiceAssignment service : cluster.getServices()) {
                if (!List.of("broker", "broker_controller", "broker_zookeeper").contains(service.getRole())) continue;
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("operation", "service");
                payload.put("hostId", service.getHostId());
                payload.put("role", service.getRole());
                payload.put("nodeId", service.getNodeId() == null ? "1" : String.valueOf(service.getNodeId()));
                payload.put("configJson", newConfigJson);
                payload.put("clusterConfigJson", newConfigJson);
                payload.put("propertiesTemplate", request.getConfigKey() + "=" + request.getConfigValue() + "\n");
                payload.put("previousConfigJson", oldConfigJson);
                payload.put("previousPropertiesTemplate", request.getConfigKey() + "=" + previousByBroker.getOrDefault(String.valueOf(service.getNodeId()), "") + "\n");
                payload.put("restart", request.isRestart());
                JobStep step = new JobStep();
                step.setStepOrder(order++);
                step.setName("Persist configuration on " + service.getHostId());
                step.setTargetId(service.getHostId());
                step.setPayload(objectMapper.writeValueAsString(payload));
                steps.add(step);
            }
        }

        Job job = new Job();
        job.setType(JobType.CONFIG_CHANGE);
        job.setStatus(JobStatus.PENDING);
        job.setRollbackSupported(true);
        job.setResourceKey("cluster:" + clusterId);
        job.setPayload(objectMapper.writeValueAsString(Map.of("clusterId", clusterId.toString())));
        Job saved = jobService.createJob(job, steps);
        activityAlertService.logAudit("INFO", "CONFIGURATION", "UPDATE",
                "Created configuration change job for " + request.getConfigKey(), "CLUSTER", clusterId.toString(), clusterId,
                auditConfigValue(request.getConfigKey(), previousByBroker.toString()),
                auditConfigValue(request.getConfigKey(), request.getConfigValue()), "QUEUED", null,
                "jobId=" + saved.getId() + ";restart=" + request.isRestart());
        return ResponseEntity.ok(Map.of("jobId", saved.getId().toString(), "status", saved.getStatus().name()));
    }

    private String auditProperties(String properties) {
        if (properties == null || properties.isBlank()) return properties;
        return properties.lines().map(line -> {
            int separator = line.indexOf('=');
            if (separator < 0) return line;
            String key = line.substring(0, separator);
            return key + "=" + auditConfigValue(key, line.substring(separator + 1));
        }).collect(java.util.stream.Collectors.joining("\n"));
    }

    private String auditConfigValue(String key, String value) {
        String normalized = key == null ? "" : key.toLowerCase(java.util.Locale.ROOT);
        if (normalized.contains("password") || normalized.contains("secret")
                || normalized.contains("token") || normalized.contains("credential")
                || normalized.contains("jaas")) {
            return "<redacted>";
        }
        return value;
    }

    @PostMapping("/read")
    public ResponseEntity<Map<String, Object>> readConfig(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody Map<String, Object> request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.CONFIGURATION_CHANGE)) {
            return unauthorized();
        }
        String nodeIdStr = String.valueOf(request.get("nodeId"));
        Integer targetNodeId = null;
        try { targetNodeId = Integer.parseInt(nodeIdStr); } catch (Exception e) {}

        List<io.translab.tantor.server.domain.ExternalClusterNode> nodes = externalClusterNodeRepository.findByClusterId(clusterId);
        io.translab.tantor.server.domain.ExternalClusterNode targetNode = null;
        for (io.translab.tantor.server.domain.ExternalClusterNode n : nodes) {
            if (n.getNodeId() != null && n.getNodeId().equals(targetNodeId)) {
                targetNode = n;
                break;
            } else if (n.getNodeId() == null && Math.abs(n.getHost().hashCode()) == targetNodeId) {
                targetNode = n;
                break;
            }
        }

        if (targetNode == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "Node not found"));
        }

        Map<String, Object> payload = new HashMap<>();
        payload.put("configFilePath", targetNode.getConfigFile());

        Map<String, Object> taskResponse = externalClusterService.queueTask(clusterId, targetNode.getHost(), "read_config", payload);
        return ResponseEntity.ok(taskResponse);
    }

    @Data
    public static class BulkConfigRequest {
        private String configKey;
        private String configValue;
        private boolean applyToAgents = false;
        private boolean restart = false;
    }

    @Data
    public static class ServiceConfigUpdateRequest {
        private Map<String, Object> properties;
        private boolean restart = true;
    }
}
