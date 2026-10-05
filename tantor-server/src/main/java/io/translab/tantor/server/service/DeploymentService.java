package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import io.translab.tantor.server.config.KafkaDeploymentProperties;
import jakarta.annotation.PostConstruct;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeploymentService {

    private final TaskRepository taskRepository;
    private final HostParcelRepository hostParcelRepository;
    private final ClusterRepository clusterRepository;
    private final HostRepository hostRepository;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final ArtifactRepositoryProperties artifactRepositoryProperties;
    private final KafkaDeploymentProperties kafkaDeploymentProperties;

    private String defaultRuntimeUser;

    private String defaultRuntimeGroup;

    private String defaultJavaHome;

    private String defaultLimitNoFile;

    private String defaultSecurityMode;

    private String defaultServicePrefix;

    @PostConstruct
    void bindTypedConfiguration() {
        defaultRuntimeUser = kafkaDeploymentProperties.getRuntimeUser();
        defaultRuntimeGroup = kafkaDeploymentProperties.getRuntimeGroup();
        defaultJavaHome = kafkaDeploymentProperties.getJavaHome();
        defaultLimitNoFile = Integer.toString(kafkaDeploymentProperties.getLimitNofile());
        defaultSecurityMode = kafkaDeploymentProperties.getSecurityMode();
        defaultServicePrefix = kafkaDeploymentProperties.getServicePrefix();
    }

    private static final Pattern ARTIFACT_DOWNLOAD_PATTERN = Pattern.compile("/api/v1/artifacts/([^/]+)/download");

    @Transactional
    public UUID deployKafkaToHost(UUID clusterId, String hostId, String version, String artifactUrl, String checksum, String nodeId, String quorumVoters, String role, String configJsonStr) {
        log.info("Scheduling Kafka {} deployment on host {}", version, hostId);

        Task task = createTask(clusterId, hostId, "INSTALL_KAFKA");
        String agentArtifactUrl = normalizeArtifactRepoUrl(artifactUrl);
        task.setArtifactUrl(agentArtifactUrl);
        
        try {
            String resolvedChecksum = firstNonBlank(checksum, resolveArtifactChecksum(agentArtifactUrl).orElse(""));
            if (!hasText(resolvedChecksum)) {
                throw new IllegalArgumentException("Kafka artifact checksum is required for V9 agent deployment. Upload/select an artifact with SHA-256 metadata.");
            }
            task.setChecksum(resolvedChecksum);

            Map<String, Object> params = new LinkedHashMap<>();
            params.put("version", version);
            params.put("node_id", nodeId != null ? nodeId : "1");
            params.put("quorum_voters", required(quorumVoters, "quorum_voters"));
            String normalizedRole = role != null && !role.isBlank() ? role : "broker_controller";
            params.put("role", normalizedRole);
            params.put("service_role", normalizedRole);
            params.put("service_name", systemdServiceName(normalizedRole));
            params.put("systemd_service", systemdServiceName(normalizedRole));
            params.put("jmx_port", defaultJmxPortForRole(normalizedRole));
            if (clusterId != null) {
                params.put("db_cluster_id", clusterId.toString());
            }
            addHostIdentity(params, hostId);

            mergeConfigParams(params, configJsonStr);
            params.put("config_file", configFileForRole(normalizedRole, String.valueOf(params.getOrDefault("mode", "kraft"))));
            
            applyDefaultKafkaPaths(params);
            applyActiveParcelParams(params, hostId, version);

            injectJmxArtifactUrl(params, agentArtifactUrl);
            injectKafkaExporterArtifactUrl(params);
            applyAgentKafkaDeploymentParams(params, version, normalizedRole, resolvedChecksum, agentArtifactUrl);

            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize parameters", e);
        }

        taskRepository.save(task);
        log.info("Task {} created successfully", task.getId());
        return task.getId();
    }

    @Transactional
    public UUID dispatchSchemaTask(UUID clusterId, String hostId, String command,
                                   String artifactUrl, String checksum, Map<String, Object> parameters) {
        Task task = createTask(clusterId, hostId, command);
        String agentArtifactUrl = normalizeArtifactRepoUrl(artifactUrl);
        if (hasText(agentArtifactUrl)) {
            task.setArtifactUrl(agentArtifactUrl);
            String resolvedChecksum = firstNonBlank(checksum, resolveArtifactChecksum(agentArtifactUrl).orElse(""));
            if (("INSTALL_SCHEMA".equals(command) || "PRECHECK_SCHEMA".equals(command)
                    || "INSTALL_CONNECT".equals(command) || "PRECHECK_CONNECT".equals(command))
                    && !hasText(resolvedChecksum)) {
                throw new IllegalArgumentException("Data-service artifact checksum is required.");
            }
            task.setChecksum(resolvedChecksum);
        }
        Map<String, Object> params = new LinkedHashMap<>();
        if (parameters != null) {
            params.putAll(parameters);
        }
        addHostIdentity(params, hostId);
        try {
            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize Schema Registry task parameters", e);
        }
        taskRepository.save(task);
        return task.getId();
    }

    private void addHostIdentity(Map<String, Object> params, String hostId) {
        params.put("host_id", hostId);
        hostRepository.findById(hostId).ifPresent(host -> {
            params.put("host_hostname", host.getHostname() == null ? "" : host.getHostname());
            params.put("host_ip", firstHostIp(host.getIpAddresses()));
        });
    }

    private String firstHostIp(String ipAddresses) {
        if (ipAddresses == null || ipAddresses.isBlank() || "[]".equals(ipAddresses)) return "";
        try {
            java.util.List<?> values = objectMapper.readValue(ipAddresses, java.util.List.class);
            return values.isEmpty() ? "" : String.valueOf(values.get(0));
        } catch (Exception ignored) {
            return ipAddresses.replaceAll("\\[|\\]|\\\"", "").split(",")[0].trim();
        }
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private String required(String value, String name) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    private String normalizeArtifactRepoUrl(String artifactUrl) {
        if (!hasText(artifactUrl) || !artifactUrl.contains("/api/v1/artifacts/")) {
            return artifactUrl;
        }
        int pathStart = artifactUrl.indexOf("/api/v1/artifacts/");
        return joinArtifactRepoBase(artifactUrl.substring(pathStart));
    }

    private String joinArtifactRepoBase(String pathAndQuery) {
        String base = artifactRepositoryProperties.getPublicUrl().toString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        String path = pathAndQuery == null ? "" : pathAndQuery.trim();
        if (path.isBlank()) return base;
        return base + (path.startsWith("/") ? path : "/" + path);
    }

    private void injectJmxArtifactUrl(Map<String, Object> params, String artifactUrl) {
        if (!hasText(artifactUrl) || !artifactUrl.contains("/api/v1/artifacts/")) {
            return;
        }

        Optional<Map<String, String>> artifact = resolveJmxExporterArtifact();
        if (artifact.isEmpty()) {
            log.info("No available JMX exporter artifact found. Agent will use fallback behavior.");
            return;
        }

        String artifactId = artifact.get().get("id");
        params.put("jmx_artifact_id", artifactId);
        params.put("jmx_artifact_url", joinArtifactRepoBase("/api/v1/artifacts/" + artifactId + "/download"));
        if (hasText(artifact.get().get("checksum"))) {
            params.put("jmx_checksum", artifact.get().get("checksum"));
        }
    }

    private Optional<String> resolveJmxExporterArtifactId() {
        return resolveJmxExporterArtifact().map(artifact -> artifact.get("id"));
    }

    private void injectKafkaExporterArtifactUrl(Map<String, Object> params) {
        Optional<Map<String, String>> artifact = resolveKafkaExporterArtifact();
        if (artifact.isEmpty()) {
            log.info("No available KAFKA_EXPORTER artifact found.");
            return;
        }

        String artifactId = artifact.get().get("id");
        params.put("kafka_exporter_artifact_id", artifactId);
        params.put("kafka_exporter_download_url", joinArtifactRepoBase("/api/v1/artifacts/" + artifactId + "/download"));
        if (hasText(artifact.get().get("checksum"))) {
            params.put("kafka_exporter_checksum", artifact.get().get("checksum"));
        }
    }

    private Optional<Map<String, String>> resolveKafkaExporterArtifact() {
        try {
            return jdbcTemplate.query("""
                    SELECT id::text, COALESCE(checksum, '')
                    FROM kf_artifact
                    WHERE service_type = 'KAFKA_EXPORTER'
                      AND status = 'AVAILABLE'
                    ORDER BY created_time DESC
                    LIMIT 1
                    """, rs -> rs.next()
                            ? Optional.of(Map.of("id", rs.getString(1), "checksum", rs.getString(2)))
                            : Optional.empty());
        } catch (Exception e) {
            log.warn("Could not auto-resolve KAFKA_EXPORTER artifact from kf_artifact", e);
            return Optional.empty();
        }
    }

    private Optional<Map<String, String>> resolveJmxExporterArtifact() {
        if (hasText(artifactRepositoryProperties.getJmxExporterArtifactId())) {
            String id = artifactRepositoryProperties.getJmxExporterArtifactId().trim();
            return Optional.of(Map.of(
                    "id", id,
                    "checksum", resolveArtifactChecksumById(id).orElse("")
            ));
        }

        try {
            return jdbcTemplate.query("""
                    SELECT id::text, COALESCE(checksum, '')
                    FROM kf_artifact
                    WHERE service_type = 'JMX_EXPORTER'
                      AND status = 'AVAILABLE'
                    ORDER BY created_time DESC
                    LIMIT 1
                    """, rs -> rs.next()
                            ? Optional.of(Map.of("id", rs.getString(1), "checksum", rs.getString(2)))
                            : Optional.empty());
        } catch (Exception e) {
            log.warn("Could not auto-resolve JMX exporter artifact from kf_artifact", e);
            return Optional.empty();
        }
    }

    private Optional<String> resolveArtifactChecksum(String artifactUrl) {
        return extractArtifactId(artifactUrl).flatMap(this::resolveArtifactChecksumById);
    }

    private Optional<String> resolveArtifactChecksumById(String artifactId) {
        if (!hasText(artifactId)) {
            return Optional.empty();
        }
        try {
            return jdbcTemplate.query("""
                    SELECT COALESCE(checksum, '')
                    FROM kf_artifact
                    WHERE id::text = ?
                    LIMIT 1
                    """, rs -> rs.next() && hasText(rs.getString(1)) ? Optional.of(rs.getString(1)) : Optional.empty(), artifactId);
        } catch (Exception e) {
            log.warn("Could not resolve artifact checksum for {}", artifactId, e);
            return Optional.empty();
        }
    }

    private Optional<String> extractArtifactId(String artifactUrl) {
        if (!hasText(artifactUrl)) {
            return Optional.empty();
        }
        Matcher matcher = ARTIFACT_DOWNLOAD_PATTERN.matcher(artifactUrl);
        return matcher.find() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    @Transactional
    public void upgradeKafkaOnHost(UUID clusterId, String hostId, String currentVersion, String targetVersion, String nodeId, String role, String configJsonStr) {
        log.info("Scheduling Kafka upgrade on host {} from {} to {}", hostId, currentVersion, targetVersion);

        Task task = createTask(clusterId, hostId, "UPGRADE_KAFKA");

        try {
            Map<String, Object> params = new java.util.HashMap<>();
            params.put("version", targetVersion);
            params.put("target_version", targetVersion);
            params.put("previous_version", currentVersion);
            params.put("node_id", nodeId != null ? nodeId : "1");
            String normalizedRole = role != null && !role.isBlank() ? role : "broker_controller";
            params.put("role", normalizedRole);
            params.put("service_role", normalizedRole);
            params.put("service_name", systemdServiceName(normalizedRole));
            params.put("systemd_service", systemdServiceName(normalizedRole));
            params.put("jmx_port", defaultJmxPortForRole(normalizedRole));
            if (clusterId != null) {
                params.put("cluster_id", clusterId.toString());
            }

            mergeConfigParams(params, configJsonStr);
            params.put("config_file", configFileForRole(normalizedRole, String.valueOf(params.getOrDefault("mode", "kraft"))));
            applyDefaultKafkaPaths(params);
            if (!applyActiveParcelParams(params, hostId, targetVersion)) {
                throw new IllegalStateException("Kafka " + targetVersion + " is not active on host " + hostId + ".");
            }
            injectJmxArtifactUrl(params, String.valueOf(params.get("artifact_url")));
            injectKafkaExporterArtifactUrl(params);

            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize upgrade parameters", e);
            task.setParameters("{}");
        }

        taskRepository.save(task);
        log.info("Upgrade task {} created successfully", task.getId());
    }

    @Transactional
    public void startService(UUID clusterId, String hostId, String serviceName) {
        Task task = createTask(clusterId, hostId, "START_SERVICE");
        
        try {
            task.setParameters(objectMapper.writeValueAsString(Map.of(
                "service_name", serviceName
            )));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize parameters", e);
        }

        taskRepository.save(task);
    }

    @Transactional
    public UUID restartService(UUID clusterId, String hostId, String serviceName) {
        Task task = createTask(clusterId, hostId, "RESTART_SERVICE");
        
        try {
            task.setParameters(objectMapper.writeValueAsString(Map.of(
                "service_name", serviceName
            )));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize parameters", e);
        }

        taskRepository.save(task);
        return task.getId();
    }

    @Transactional
    public void updateKafkaConfig(UUID clusterId, String hostId, String configJsonStr, boolean restart) {
        updateKafkaConfig(clusterId, hostId, "broker", "1", configJsonStr, "", restart, "unversioned", null);
    }

    @Transactional
    public UUID updateKafkaConfig(
            UUID clusterId,
            String hostId,
            String role,
            String nodeId,
            String configJsonStr,
            String propertiesTemplate,
            boolean restart,
            String configVersion,
            String configPath
    ) {
        Task task = createTask(clusterId, hostId, "UPDATE_KAFKA_CONFIG");
        
        try {
            Map<String, Object> params = new java.util.HashMap<>();
            params.put("restart", String.valueOf(restart));
            String normalizedRole = role == null || role.isBlank() ? "broker" : role;
            params.put("role", normalizedRole);
            params.put("service_role", normalizedRole);
            params.put("node_id", nodeId == null || nodeId.isBlank() ? "1" : nodeId);
            params.put("service_name", systemdServiceName(normalizedRole));
            params.put("systemd_service", systemdServiceName(normalizedRole));
            params.put("config_version", configVersion == null || configVersion.isBlank() ? "unversioned" : configVersion);
            if (clusterId != null) {
                params.put("cluster_id", clusterId.toString());
            }

            mergeConfigParams(params, configJsonStr);
            params.put("config_file", configFileForRole(normalizedRole, String.valueOf(params.getOrDefault("mode", "kraft"))));
            if (configPath != null && !configPath.isBlank()) {
                params.put("config_path", configPath.trim());
            }
            if (propertiesTemplate != null && !propertiesTemplate.isBlank()) {
                if ("controller".equals(normalizedRole)) {
                    params.put("controller_properties_template", propertiesTemplate);
                } else if ("zookeeper".equals(normalizedRole)) {
                    params.put("zookeeper_properties_template", propertiesTemplate);
                } else if ("broker_controller".equals(normalizedRole)) {
                    params.put("server_properties_template", propertiesTemplate);
                } else {
                    params.put("broker_properties_template", propertiesTemplate);
                }
            }
            applyDefaultKafkaPaths(params);
            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize parameters", e);
        }

        taskRepository.save(task);
        return task.getId();
    }

    @Transactional
    public UUID deleteClusterFromHost(UUID clusterId, String hostId, String version, String configJsonStr) {
        return deleteClusterFromHost(clusterId, hostId, version, configJsonStr, List.of(), true);
    }

    @Transactional
    public UUID deleteClusterFromHost(UUID clusterId, String hostId, String version, String configJsonStr,
                                      List<Map<String, Object>> dataServices, boolean cleanupKafka) {
        Task task = createTask(clusterId, hostId, "DELETE_CLUSTER");
        try {
            Map<String, Object> params = new java.util.HashMap<>();
            if (version != null && !version.isBlank()) {
                params.put("version", version);
            }
            if (clusterId != null) {
                params.put("cluster_id", clusterId.toString());
            }
            mergeConfigParams(params, configJsonStr);
            applyDefaultKafkaPaths(params);
            params.put("data_services", objectMapper.writeValueAsString(dataServices));
            params.put("cleanup_kafka", Boolean.toString(cleanupKafka));
            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize cluster cleanup parameters", e);
        }
        taskRepository.save(task);
        log.info("Dispatched DELETE_CLUSTER task for host {} in cluster {}", hostId, clusterId);
        return task.getId();
    }

    @Transactional
    public UUID installMonitoring(
            UUID clusterId,
            String hostId,
            String installDir,
            String prometheusUrl,
            String grafanaUrl
    ) {
        Task task = createTask(clusterId, hostId, "INSTALL_MONITORING");
        try {
            task.setParameters(objectMapper.writeValueAsString(Map.of(
                    "install_dir", installDir == null || installDir.isBlank() ? "/opt/tantor/monitoring" : installDir,
                    "prometheus_url", prometheusUrl == null ? "" : prometheusUrl,
                    "grafana_url", grafanaUrl == null ? "" : grafanaUrl
            )));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid monitoring deployment parameters", e);
        }
        taskRepository.save(task);
        return task.getId();
    }

    @Transactional
    public UUID removeMonitoring(UUID clusterId, String hostId, String installDir) {
        Task task = createTask(clusterId, hostId, "DELETE_MONITORING");
        try {
            task.setParameters(objectMapper.writeValueAsString(Map.of(
                    "install_dir", installDir == null || installDir.isBlank() ? "/opt/tantor/monitoring" : installDir
            )));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid monitoring rollback parameters", e);
        }
        taskRepository.save(task);
        return task.getId();
    }

    @Transactional
    public UUID checkKRaftConnectivity(UUID clusterId, String hostId, String controllerEndpoints) {
        Task task = createTask(clusterId, hostId, "CHECK_KRAFT_CONNECTIVITY");
        try {
            task.setParameters(objectMapper.writeValueAsString(Map.of(
                    "controller_endpoints", controllerEndpoints == null ? "" : controllerEndpoints
            )));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid KRaft connectivity parameters", e);
        }
        taskRepository.save(task);
        return task.getId();
    }

    @Transactional
    public UUID verifyKRaftQuorum(
            UUID clusterId,
            String hostId,
            String controllerEndpoints,
            String clusterUuid,
            String expectedControllerCount,
            String configJsonStr
    ) {
        Task task = createTask(clusterId, hostId, "VERIFY_KRAFT_QUORUM");
        try {
            Map<String, Object> params = new java.util.HashMap<>();
            params.put("controller_endpoints", controllerEndpoints == null ? "" : controllerEndpoints);
            params.put("cluster_uuid", clusterUuid == null ? "" : clusterUuid);
            params.put("expected_controller_count", expectedControllerCount == null ? "" : expectedControllerCount);
            mergeConfigParams(params, configJsonStr);
            applyDefaultKafkaPaths(params);
            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid KRaft quorum verification parameters", e);
        }
        taskRepository.save(task);
        return task.getId();
    }

    @Transactional
    public UUID verifyZooKeeperQuorum(
            UUID clusterId,
            String hostId,
            String zookeeperConnect,
            String configJsonStr
    ) {
        Task task = createTask(clusterId, hostId, "VERIFY_ZK_QUORUM");
        try {
            Map<String, Object> params = new java.util.HashMap<>();
            params.put("zookeeper_connect", zookeeperConnect == null ? "" : zookeeperConnect);
            mergeConfigParams(params, configJsonStr);
            applyDefaultKafkaPaths(params);
            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid ZooKeeper quorum verification parameters", e);
        }
        taskRepository.save(task);
        return task.getId();
    }

    @Transactional
    public boolean retryTask(UUID taskId) {
        return taskRepository.findById(taskId).map(task -> {
            if ("FAILED".equals(task.getStatus()) || "CANCELLED".equals(task.getStatus())) {
                task.setStatus("PENDING");
                task.setErrorMsg(null);

                task.setLogOutput(null);
                task.setStepLogs(null);
                task.setCurrentStep(null);
                taskRepository.save(task);
                
                if (task.getClusterId() != null) {
                    clusterRepository.findById(task.getClusterId()).ifPresent(c -> {
                        c.setStatus("RUNNING");
                        clusterRepository.save(c);
                    });
                }
                
                return true;
            }
            return false;
        }).orElse(false);
    }

    @Transactional
    public boolean resumeTask(UUID taskId) {
        return taskRepository.findById(taskId).map(task -> {
            if ("FAILED".equals(task.getStatus()) || "CANCELLED".equals(task.getStatus())) {
                task.setStatus("PENDING");
                task.setErrorMsg(null);

                try {
                    Map<String, Object> params = objectMapper.readValue(task.getParameters(), Map.class);
                    if (task.getCurrentStep() != null) {
                        params.put("resume_step", task.getCurrentStep());
                    }
                    task.setParameters(objectMapper.writeValueAsString(params));
                } catch (Exception e) {
                    log.error("Failed to inject resume_step into parameters", e);
                }
                taskRepository.save(task);
                
                if (task.getClusterId() != null) {
                    clusterRepository.findById(task.getClusterId()).ifPresent(c -> {
                        c.setStatus("RUNNING");
                        clusterRepository.save(c);
                    });
                }
                return true;
            }
            return false;
        }).orElse(false);
    }

    @Transactional
    public boolean rollbackTask(UUID clusterId, UUID taskId) {
        return taskRepository.findById(taskId).map(task -> {
            if ("FAILED".equals(task.getStatus())) {
                task.setStatus("ROLLBACK_PENDING");
                taskRepository.save(task);
                
                Task rollbackTask = createTask(clusterId, task.getHostId(), "ROLLBACK_DEPLOYMENT");
                try {
                    Map<String, Object> params = objectMapper.readValue(task.getParameters(), Map.class);
                    params.put("original_task_id", task.getId().toString());
                    rollbackTask.setParameters(objectMapper.writeValueAsString(params));
                } catch (Exception e) {
                    log.error("Failed to serialize parameters for rollback", e);
                    rollbackTask.setParameters("{\"original_task_id\":\"" + task.getId().toString() + "\"}");
                }
                taskRepository.save(rollbackTask);
                return true;
            }
            return false;
        }).orElse(false);
    }

    @Transactional
    public boolean cleanupTask(UUID clusterId, UUID taskId) {
        return taskRepository.findById(taskId).map(task -> {
            if ("FAILED".equals(task.getStatus()) || "ROLLBACK_DONE".equals(task.getStatus())) {
                task.setStatus("CLEANUP_PENDING");
                taskRepository.save(task);
                
                Task cleanupTask = createTask(clusterId, task.getHostId(), "DELETE_CLUSTER");
                try {
                    Map<String, Object> params = objectMapper.readValue(task.getParameters(), Map.class);
                    params.put("original_task_id", task.getId().toString());
                    cleanupTask.setParameters(objectMapper.writeValueAsString(params));
                } catch (Exception e) {
                    cleanupTask.setParameters("{\"original_task_id\":\"" + task.getId().toString() + "\"}");
                }
                taskRepository.save(cleanupTask);
                return true;
            }
            return false;
        }).orElse(false);
    }



    private String systemdServiceName(String role) {
        if ("controller".equals(role)) return "controller";
        if ("zookeeper".equals(role)) return "zookeeper";
        if ("broker_controller".equals(role) || "broker_zookeeper".equals(role)) return "kafka";
        return "broker";
    }

    private String configFileForRole(String role, String mode) {
        if ("zookeeper".equalsIgnoreCase(mode)) {
            return "zookeeper".equals(role) ? "zookeeper.properties" : "server.properties";
        }
        if ("controller".equals(role)) return "controller.properties";
        if ("zookeeper".equals(role)) return "zookeeper.properties";
        if ("broker_controller".equals(role)) return "server.properties";
        return "broker.properties";
    }
    private Task createTask(UUID clusterId, String hostId, String command) {
        Task task = new Task();
        task.setClusterId(clusterId);
        task.setHostId(hostId);
        task.setCommand(command);
        task.setStatus("PENDING");
        return task;
    }

    @SuppressWarnings("unchecked")
    private void mergeConfigParams(Map<String, Object> params, String configJsonStr) throws JsonProcessingException {
        if (configJsonStr == null || configJsonStr.isBlank() || "{}".equals(configJsonStr)) {
            return;
        }

        Map<String, Object> configMap = objectMapper.readValue(configJsonStr, Map.class);
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            if (entry.getValue() != null) {
                params.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
    }

    private void applyDefaultKafkaPaths(Map<String, Object> params) {
        Object installDir = params.get("kafka_install_dir");
        if (installDir == null || String.valueOf(installDir).isBlank()) {
            params.put("kafka_install_dir", "/opt");
        }
    }

    private void applyAgentKafkaDeploymentParams(
            Map<String, Object> params,
            String version,
            String role,
            String artifactChecksum,
            String artifactUrl
    ) {
        String runtimeUser = firstParam(params, "runtime_user", "runtimeUser");
        if (!hasText(runtimeUser)) runtimeUser = defaultRuntimeUser;
        String runtimeGroup = firstParam(params, "runtime_group", "runtimeGroup");
        if (!hasText(runtimeGroup)) runtimeGroup = defaultRuntimeGroup;
        String javaHome = firstParam(params, "java_home", "javaHome");
        if (!hasText(javaHome)) javaHome = defaultJavaHome;

        String kafkaVersion = firstNonBlank(version, firstParam(params, "kafka_version", "version", "target_version"));
        String scalaVersion = firstNonBlank(firstParam(params, "scala_version"), "2.13");
        String installBasePath = firstParam(params, "install_base_path", "kafka_install_base_dir", "kafka_install_dir");
        String activeSymlinkPath = firstNonBlank(firstParam(params, "active_symlink_path"), activeSymlinkPath(installBasePath));
        String dataPaths = firstNonBlank(firstParam(params, "data_paths", "kafka_log_dirs", "log_dirs", "kafka_data_dir"), "");
        String metadataPath = firstNonBlank(firstParam(params, "metadata_path", "kafka_metadata_dir", "metadata_log_dir"), metadataPathForRole(role, dataPaths));
        String logPath = firstParam(params, "log_path", "kafka_app_log_dir", "service_log_dir");
        String kafkaClusterId = firstParam(params, "cluster_uuid", "kafka_cluster_id", "cluster_id");
        String hostIp = firstParam(params, "host_ip", "bind_address", "listen_address");
        String brokerPort = firstParam(params, "broker_port", "listener_port");
        String controllerPort = firstParam(params, "controller_port");
        String heapSize = firstParam(params, "heap_size");
        String heapXms = firstNonBlank(firstParam(params, "heap_xms"), heapSize);
        String heapXmx = firstNonBlank(firstParam(params, "heap_xmx"), heapSize);
        String quorumMode = firstParam(params, "kraft_quorum_mode");
        String quorumVoters = firstParam(params, "controller_quorum_voters", "quorum_voters");
        String quorumBootstrapServers = firstParam(params, "controller_quorum_bootstrap_servers", "quorum_bootstrap_servers", "controller_endpoints");
        String kraftFormatMode = firstParam(params, "kraft_format_mode");
        String initialControllers = firstParam(params, "initial_controllers");

        putParamIfText(params, "runtime_user", runtimeUser);
        putParamIfText(params, "runtime_group", runtimeGroup);
        putParamIfText(params, "java_home", javaHome);
        putParam(params, "kafka_version", kafkaVersion);
        putParam(params, "scala_version", scalaVersion);
        putParam(params, "install_base_path", installBasePath);
        putParam(params, "active_symlink_path", activeSymlinkPath);
        putParam(params, "data_paths", dataPaths);
        putParam(params, "metadata_path", metadataPath);
        putParam(params, "log_path", logPath);
        putParam(params, "cluster_id", kafkaClusterId);
        putParam(params, "bind_address", hostIp);
        putParam(params, "advertised_address", firstNonBlank(firstParam(params, "advertised_address", "advertised_host", "advertised_ip"), hostIp));
        putParam(params, "role", role);
        putParam(params, "node_id", firstParam(params, "node_id"));
        putParam(params, "service_name", v9ServiceName(params));
        putParam(params, "broker_port", brokerPort);
        putParam(params, "controller_port", controllerPort);
        putParam(params, "jmx_enabled", firstNonBlank(firstParam(params, "jmx_enabled"), "true"));
        putParam(params, "jmx_port", firstNonBlank(firstParam(params, "jmx_port"), defaultJmxPortForRole(role)));
        if ((isBrokerRole(role) || isControllerRole(role)) && !"false".equalsIgnoreCase(firstParam(params, "jmx_enabled"))) {
            putParam(params, "jmx_required", "true");
        }
        putParam(params, "heap_xms", heapXms);
        putParam(params, "heap_xmx", heapXmx);
        putParam(params, "limit_nofile", firstNonBlank(firstParam(params, "limit_nofile"), defaultLimitNoFile));
        putParam(params, "security_mode", firstNonBlank(firstParam(params, "security_mode", "listener_security_protocol"), defaultSecurityMode));
        putParam(params, "kraft_quorum_mode", quorumMode);
        putParam(params, "controller_quorum_voters", quorumVoters);
        putParam(params, "controller_quorum_bootstrap_servers", quorumBootstrapServers);
        putParam(params, "kraft_format_mode", firstNonBlank(kraftFormatMode, defaultKraftFormatMode(role, quorumMode, initialControllers)));
        putParam(params, "initial_controllers", initialControllers);
        putParam(params, "num_partitions", firstParam(params, "num_partitions"));
        putParam(params, "replication_factor", firstParam(params, "replication_factor", "rep_factor"));
        putParam(params, "min_insync_replicas", firstParam(params, "min_insync_replicas"));
        putParam(params, "artifact_url", artifactUrl);
        putParam(params, "artifact_checksum", artifactChecksum);
    }

    private String v9ServiceName(Map<String, Object> params) {
        String configured = firstParam(params, "v9_service_name", "kafka_service_name");
        if (hasText(configured)) return configured;
        String existing = firstParam(params, "service_name");
        if (hasText(existing) && (existing.startsWith("tantor-kafka-") || existing.startsWith("kafka-"))) {
            return existing;
        }
        return firstNonBlank(defaultServicePrefix, "tantor-kafka-") + firstNonBlank(firstParam(params, "node_id"), "node");
    }

    private String defaultKraftFormatMode(String role, String quorumMode, String initialControllers) {
        if (!"dynamic".equalsIgnoreCase(quorumMode)) return "";
        if ((role.contains("controller")) && hasText(initialControllers)) return "initial_controllers";
        return "existing_cluster";
    }

    private boolean isBrokerRole(String role) {
        String normalized = role == null ? "" : role.toLowerCase();
        return normalized.contains("broker");
    }

    private boolean isControllerRole(String role) {
        String normalized = role == null ? "" : role.toLowerCase();
        return normalized.contains("controller");
    }

    private String defaultJmxPortForRole(String role) {
        return "controller".equalsIgnoreCase(role) ? "7072" : "7071";
    }

    private String activeSymlinkPath(String installBasePath) {
        if (!hasText(installBasePath)) return "";
        String base = trimTrailingSlash(installBasePath.trim());
        if (base.endsWith("/kafka")) return base;
        return base + "/kafka";
    }

    private String metadataPathForRole(String role, String dataPaths) {
        if (!hasText(dataPaths)) return "";
        String firstDataPath = dataPaths.split(",")[0].trim();
        if (!hasText(firstDataPath)) return "";
        if (role != null && role.contains("controller") && !role.contains("broker")) {
            return trimTrailingSlash(firstDataPath) + "/metadata";
        }
        return firstDataPath;
    }

    private String firstParam(Map<String, Object> params, String... keys) {
        for (String key : keys) {
            Object value = params.get(key);
            if (value != null && hasText(String.valueOf(value))) {
                return String.valueOf(value).trim();
            }
        }
        return "";
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (hasText(value)) return value.trim();
        }
        return "";
    }

    private void putParam(Map<String, Object> params, String key, String value) {
        if (value != null) {
            params.put(key, value);
        }
    }

    private void putParamIfText(Map<String, Object> params, String key, String value) {
        if (hasText(value)) {
            params.put(key, value.trim());
        }
    }

    private String trimTrailingSlash(String value) {
        String result = value;
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private boolean applyActiveParcelParams(Map<String, Object> params, String hostId, String version) {
        var activeParcel = hostParcelRepository.findLatestActive(hostId, "KAFKA").stream()
                .filter(parcel -> version != null && version.equals(parcel.getVersion()))
                .findFirst();
        activeParcel.ifPresent(parcel -> {
            params.put("use_active_parcel", "true");
            params.put("parcel_dir", parcel.getParcelDir());
        });
        return activeParcel.isPresent();
    }
}
