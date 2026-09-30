package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.security.TruststoreStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaAdminService {

    private final ClusterRepository clusterRepository;
    private final ExternalClusterRepository externalClusterRepository;
    private final HostRepository hostRepository;
    private final ObjectMapper objectMapper;
    private final EncryptionService encryptionService;
    private final TruststoreStorageService truststoreStorageService;

    private final Map<UUID, AdminClient> adminClients = new ConcurrentHashMap<>();

    public AdminClient getAdminClient(UUID clusterId) {
        return adminClients.computeIfAbsent(clusterId, this::createAdminClient);
    }

    private AdminClient createAdminClient(UUID clusterId) {
        Properties props = getKafkaClientProperties(clusterId);
        log.info("Creating AdminClient for cluster {} with bootstrap {}", clusterId, props.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        return AdminClient.create(props);
    }

    /**
     * Builds the common connection properties used by Admin, Consumer and Producer clients.
     * Keeping this in one place prevents topic message operations from accidentally targeting
     * a different listener than the rest of the cluster-management API.
     */
    public Properties getKafkaClientProperties(UUID clusterId) {
        Optional<io.translab.tantor.server.domain.Cluster> clusterOpt = clusterRepository.findById(clusterId);
        Optional<ExternalCluster> extOpt = externalClusterRepository.findById(clusterId);
        
        if (clusterOpt.isEmpty() && extOpt.isEmpty()) {
            throw new IllegalArgumentException("Cluster not found");
        }
        
        Properties props = new Properties();

        if (extOpt.isPresent()) {
            ExternalCluster ext = extOpt.get();
            String servers = ext.getBootstrapServers();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            log.info("Using external bootstrap servers for cluster {}: {}", clusterId, servers);
            
            applySecurityProperties(props, ext, true);
            
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
            props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
            return props;
        }

        io.translab.tantor.server.domain.Cluster cluster = clusterOpt.get();

        if ("EXTERNAL".equals(cluster.getMode()) && cluster.getBootstrapServers() != null) {
            String servers = cluster.getBootstrapServers();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            log.info("Using external bootstrap servers for cluster {}: {}", clusterId, servers);
        } else {
            List<String> bootstrapServers = new ArrayList<>();
            for (io.translab.tantor.server.domain.ClusterServiceAssignment svc : cluster.getServices()) {
                if ("broker".equals(svc.getRole()) || "broker_controller".equals(svc.getRole()) || "broker_zookeeper".equals(svc.getRole())) {
                    Host host = hostRepository.findById(svc.getHostId()).orElse(null);
                    if (host != null) {
                        try {
                            String configJson = cluster.getConfigJson();
                            Integer port = null;
                            if (configJson != null && !configJson.isEmpty()) {
                                Map<String, Object> config = objectMapper.readValue(configJson, Map.class);
                                if (config.containsKey("listeners")) {
                                    String listeners = (String) config.get("listeners");
                                    String[] parts = listeners.split(":");
                                    port = Integer.parseInt(parts[parts.length - 1]);
                                } else if (config.containsKey("listener_port")) {
                                    Object portObj = config.get("listener_port");
                                    if (portObj instanceof Number) {
                                        port = ((Number) portObj).intValue();
                                    } else if (portObj instanceof String) {
                                        port = Integer.parseInt((String) portObj);
                                    }
                                }
                            }
                            if (port == null || port < 1 || port > 65535) {
                                throw new IllegalArgumentException("listener_port is required and must be between 1 and 65535");
                            }
                            List<String> ips = objectMapper.readValue(host.getIpAddresses(), new TypeReference<List<String>>() {});
                            if (!ips.isEmpty()) {
                                bootstrapServers.add(ips.get(0) + ":" + port);
                            }
                        } catch (Exception e) {
                            throw new IllegalStateException("Invalid Kafka bootstrap configuration for cluster " + clusterId, e);
                        }
                    }
                }
            }

            if (bootstrapServers.isEmpty()) {
                throw new RuntimeException("No brokers found for cluster " + clusterId);
            }

            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapServers));
        }
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");

        return props;
    }

    public Map<String, Object> inspectBootstrapServers(String bootstrapServers) {
        ExternalCluster temp = new ExternalCluster();
        temp.setBootstrapServers(bootstrapServers);
        return inspectBootstrapServers(temp, false);
    }
    
    public Map<String, Object> inspectBootstrapServers(ExternalCluster cluster, boolean decryptPasswords) {
        if (cluster.getBootstrapServers() == null || cluster.getBootstrapServers().isBlank()) {
            throw new IllegalArgumentException("Bootstrap servers are required.");
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers().trim());
        
        applySecurityProperties(props, cluster, decryptPasswords);
        
        // Use a short timeout (5-10s) so the UI doesn't hang indefinitely for unreachable brokers
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");

        Exception lastException = null;
        for (int attempt = 1; attempt <= 1; attempt++) {
            try (AdminClient client = AdminClient.create(props)) {
                DescribeClusterResult clusterResult = client.describeCluster();
                Collection<org.apache.kafka.common.Node> nodes = clusterResult.nodes().get();
                org.apache.kafka.common.Node controller = clusterResult.controller().get();
                String clusterId = clusterResult.clusterId().get();

                int topicCount = 0;
                try {
                    topicCount = client.listTopics(new ListTopicsOptions().listInternal(false)).names().get().size();
                } catch (Exception e) {
                    log.warn("Connected to bootstrap {}, but failed to count topics: {}", cluster.getBootstrapServers(), e.getMessage());
                }

                List<Map<String, Object>> finalNodes = new ArrayList<>();
                Map<Integer, Map<String, Object>> nodeMap = new HashMap<>();

                // Get configurations for all nodes to determine roles
                List<org.apache.kafka.common.config.ConfigResource> resources = new ArrayList<>();
                for (org.apache.kafka.common.Node node : nodes) {
                    resources.add(new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.BROKER, String.valueOf(node.id())));
                }
                
                Map<Integer, String> processRolesMap = new HashMap<>();
                Map<Integer, String> voterEndpoints = new HashMap<>();
                String detectedKafkaMode = null;
                
                if (!resources.isEmpty()) {
                    try {
                        org.apache.kafka.clients.admin.DescribeConfigsResult configResult = client.describeConfigs(resources);
                        java.util.Map<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config> configs = configResult.all().get();
                        
                        for (Map.Entry<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config> entry : configs.entrySet()) {
                            int nodeId = Integer.parseInt(entry.getKey().name());
                            org.apache.kafka.clients.admin.Config brokerConfig = entry.getValue();

                            String configuredMode = configuredKafkaMode(brokerConfig);
                            if ("KRaft".equals(configuredMode)
                                    || (detectedKafkaMode == null && "ZooKeeper".equals(configuredMode))) {
                                detectedKafkaMode = configuredMode;
                            }
                            
                            org.apache.kafka.clients.admin.ConfigEntry processRolesEntry = brokerConfig.get("process.roles");
                            if (processRolesEntry != null && processRolesEntry.value() != null) {
                                processRolesMap.put(nodeId, processRolesEntry.value());
                            }
                            
                            org.apache.kafka.clients.admin.ConfigEntry quorumVoters = brokerConfig.get("controller.quorum.voters");
                            if (quorumVoters != null && quorumVoters.value() != null && voterEndpoints.isEmpty()) {
                                for (String voterStr : quorumVoters.value().split(",")) {
                                    String[] parts = voterStr.split("@");
                                    if (parts.length == 2) {
                                        try {
                                            voterEndpoints.put(Integer.parseInt(parts[0]), parts[1]);
                                        } catch (NumberFormatException ignored) {}
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to fetch broker config: {}", e.getMessage());
                    }
                }

                for (org.apache.kafka.common.Node node : nodes) {
                    Map<String, Object> nodeData = new HashMap<>();
                    nodeData.put("id", node.id());
                    nodeData.put("broker_id", String.valueOf(node.id()));
                    nodeData.put("host", node.host());
                    nodeData.put("port", node.port());
                    nodeData.put("endpoint", node.host() + ":" + node.port());
                    nodeData.put("rack", node.rack() == null ? "" : node.rack());
                    
                    String roles = processRolesMap.get(node.id());
                    boolean isBroker = true;
                    boolean isController = controller != null && controller.id() == node.id();
                    
                    if (roles != null && !roles.isBlank()) {
                        isBroker = roles.contains("broker");
                        isController = roles.contains("controller");
                    }
                    
                    nodeData.put("isBroker", isBroker);
                    nodeData.put("isController", isController);
                    nodeMap.put(node.id(), nodeData);
                }

                // voterEndpoints are already resolved from the first successful broker config

                // 2. Query KRaft quorum to find all controllers
                try {
                    org.apache.kafka.clients.admin.QuorumInfo quorumInfo = client.describeMetadataQuorum().quorumInfo().get();
                    detectedKafkaMode = "KRaft";
                    for (org.apache.kafka.clients.admin.QuorumInfo.ReplicaState voter : quorumInfo.voters()) {
                        int voterId = voter.replicaId();
                        if (nodeMap.containsKey(voterId)) {
                            // It's a combined broker + controller
                            nodeMap.get(voterId).put("isController", true);
                        } else {
                            // Dedicated controller not returned in describeCluster().nodes()
                            String host = "unknown";
                            int port = 0;
                            
                            // Map from controller.quorum.voters if available
                            if (voterEndpoints.containsKey(voterId)) {
                                String endpoint = voterEndpoints.get(voterId);
                                String[] epParts = endpoint.split(":");
                                if (epParts.length == 2) {
                                    host = epParts[0];
                                    try {
                                        port = Integer.parseInt(epParts[1]);
                                    } catch (NumberFormatException ignored) {}
                                }
                            }
                            
                            Map<String, Object> nodeData = new HashMap<>();
                            nodeData.put("id", voterId);
                            nodeData.put("broker_id", String.valueOf(voterId));
                            nodeData.put("host", host);
                            nodeData.put("port", port);
                            nodeData.put("endpoint", host + ":" + port);
                            nodeData.put("rack", "");
                            nodeData.put("isBroker", false);
                            nodeData.put("isController", true);
                            nodeMap.put(voterId, nodeData);
                        }
                    }
                } catch (Exception e) {
                    // An unsupported metadata-quorum API proves that this broker is
                    // from the pre-KRaft/ZooKeeper generation. Authorization and
                    // timeout failures are not proof of either mode, so retain the
                    // config-derived result (or Unknown) for those cases.
                    if (detectedKafkaMode == null && isUnsupportedMetadataQuorum(e)) {
                        detectedKafkaMode = "ZooKeeper";
                    }
                    log.warn("Failed to fetch KRaft quorum info (likely Zookeeper mode): {}", e.getMessage());
                }

                normalizeNodeRolesForMode(nodeMap.values(), detectedKafkaMode);
                finalNodes.addAll(nodeMap.values());
                
                // Deterministic sorting by node ID
                finalNodes.sort((a, b) -> Integer.compare((Integer) a.get("id"), (Integer) b.get("id")));

                long actualBrokerCount = finalNodes.stream().filter(n -> Boolean.TRUE.equals(n.get("isBroker"))).count();
                if (actualBrokerCount == 0 || clusterId == null || clusterId.isBlank()) {
                    throw new RuntimeException("Controller listener detected or invalid cluster data. Please use a broker bootstrap listener.");
                }

                Map<String, Object> result = new HashMap<>();
                result.put("success", true);
                result.put("connected", true);
                result.put("status", "CONNECTED");
                result.put("bootstrapServers", cluster.getBootstrapServers().trim());
                result.put("bootstrap_servers", cluster.getBootstrapServers().trim());
                result.put("security_protocol", cluster.getSecurityProtocol() == null || cluster.getSecurityProtocol().isBlank()
                        ? "UNKNOWN"
                        : cluster.getSecurityProtocol());
                String reportedKafkaMode = detectedKafkaMode == null ? "Unknown" : detectedKafkaMode;
                result.put("mode", reportedKafkaMode);
                result.put("kafkaMode", reportedKafkaMode);
                result.put("clusterId", clusterId);
                result.put("kafka_cluster_id", clusterId == null ? "" : clusterId);
                result.put("brokerCount", finalNodes.stream().filter(n -> Boolean.TRUE.equals(n.get("isBroker"))).count());
                result.put("brokers", finalNodes);
                result.put("topicCount", topicCount);
                result.put("topic_count", topicCount);
                result.put("topics", Collections.emptyList());
                
                Integer activeControllerId = controller == null ? null : controller.id();
                result.put("controllerId", activeControllerId);
                result.put("controller_id", activeControllerId);
                result.put("activeControllerId", activeControllerId);
                
                result.put("kafka_version", "auto-detected by Kafka client");
                result.put("socket_results", socketResults(cluster.getBootstrapServers().trim()));
                result.put("message", "Bootstrap connection successful.");
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while testing bootstrap connection.");
            } catch (Exception e) {
                lastException = e;
                log.warn("Attempt {} to inspect bootstrap {} failed: {}", attempt, cluster.getBootstrapServers(), e.getMessage());
                
                String errorMsg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
                if (errorMsg.contains("sasl authentication failed") || errorMsg.contains("invalid credentials")) {
                    throw new RuntimeException("Authentication failed. Invalid SASL username or password.", e);
                }
                if (errorMsg.contains("unsupported sasl mechanism")) {
                    throw new RuntimeException("Unsupported SASL mechanism. Verify broker-supported mechanism.", e);
                }
                if (errorMsg.contains("sslhandshakeexception") || errorMsg.contains("sun.security.validator.validatorexception") || errorMsg.contains("pkix path building failed")) {
                    throw new RuntimeException("SSL trust validation failed. The uploaded truststore does not contain the CA that signed the broker certificate: CN=lab-root-ca.", e);
                }
                if (errorMsg.contains("connection reset") || errorMsg.contains("eof") || errorMsg.contains("disconnected")) {
                    throw new RuntimeException("Security protocol mismatch. Broker closed the connection. Verify selected protocol and listener port.", e);
                }

                if (attempt < 1) {
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }
        
        throw new RuntimeException("Failed to connect to bootstrap servers: " + (lastException != null ? lastException.getMessage() : "Unknown error"));
    }

    void normalizeNodeRolesForMode(Collection<Map<String, Object>> nodes, String kafkaMode) {
        if (!"ZooKeeper".equalsIgnoreCase(kafkaMode)) {
            return;
        }

        // describeCluster().controller() identifies the active broker controller
        // in ZooKeeper clusters. It is not a KRaft controller process role.
        // Every node returned by describeCluster().nodes() is therefore a broker.
        for (Map<String, Object> node : nodes) {
            node.put("isBroker", true);
            node.put("isController", false);
        }
    }

    private static String configuredKafkaMode(org.apache.kafka.clients.admin.Config brokerConfig) {
        if (brokerConfig == null) {
            return null;
        }
        org.apache.kafka.clients.admin.ConfigEntry processRoles = brokerConfig.get("process.roles");
        if (processRoles != null && processRoles.value() != null && !processRoles.value().isBlank()) {
            return "KRaft";
        }
        org.apache.kafka.clients.admin.ConfigEntry zookeeperConnect = brokerConfig.get("zookeeper.connect");
        if (zookeeperConnect != null
                && zookeeperConnect.value() != null
                && !zookeeperConnect.value().isBlank()) {
            return "ZooKeeper";
        }
        return null;
    }

    private static boolean isUnsupportedMetadataQuorum(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof org.apache.kafka.common.errors.UnsupportedVersionException
                    || current instanceof UnsupportedOperationException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private List<Map<String, Object>> socketResults(String bootstrapServers) {
        List<Map<String, Object>> results = new ArrayList<>();
        for (String server : bootstrapServers.split(",")) {
            String endpoint = server.trim();
            if (endpoint.isEmpty()) {
                continue;
            }
            String host = endpoint;
            int port = -1;
            int index = endpoint.lastIndexOf(":");
            if (index > 0 && index < endpoint.length() - 1) {
                host = endpoint.substring(0, index);
                try {
                    port = Integer.parseInt(endpoint.substring(index + 1));
                } catch (NumberFormatException ignored) {
                    port = -1;
                }
            }
            Map<String, Object> socket = new HashMap<>();
            socket.put("host", host);
            socket.put("port", port);
            socket.put("success", true);
            socket.put("latency_ms", 0);
            results.add(socket);
        }
        return results;
    }

    public List<org.apache.kafka.common.Node> describeClusterNodes(UUID clusterId) {
        AdminClient client = getAdminClient(clusterId);
        try {
            return new ArrayList<>(client.describeCluster().nodes().get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to describe brokers: interrupted");
        } catch (ExecutionException e) {
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to describe brokers: " + e.getMessage());
        }
    }

    public Integer getControllerId(UUID clusterId) {
        try {
            // In KRaft, describeCluster().controller() can identify a broker when the
            // client was created with bootstrap.servers. The metadata quorum is the
            // authoritative source for the elected controller leader.
            return getAdminClient(clusterId)
                    .describeMetadataQuorum()
                    .quorumInfo()
                    .get()
                    .leaderId();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            refreshAdminClient(clusterId);
            log.warn("Failed to resolve active KRaft controller for cluster {}: {}", clusterId, e.getMessage());
            return null;
        }
    }

    public String getKafkaClusterId(UUID clusterId) {
        AdminClient client = getAdminClient(clusterId);
        try {
            String id = client.describeCluster().clusterId().get();
            return id == null ? "" : id;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            refreshAdminClient(clusterId);
            return "";
        } catch (ExecutionException e) {
            refreshAdminClient(clusterId);
            return "";
        }
    }

    public boolean isClusterReachable(UUID clusterId, long timeoutSeconds) {
        AdminClient client = getAdminClient(clusterId);
        long boundedTimeout = Math.max(1, timeoutSeconds);
        try {
            String id = client.describeCluster().clusterId().get(boundedTimeout, TimeUnit.SECONDS);
            return id != null && !id.isBlank();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            refreshAdminClient(clusterId);
            return false;
        } catch (Exception e) {
            refreshAdminClient(clusterId);
            log.warn("Kafka health check failed for cluster {}: {}", clusterId, e.getMessage());
            return false;
        }
    }

    public void refreshAdminClient(UUID clusterId) {
        AdminClient oldClient = adminClients.remove(clusterId);
        if (oldClient != null) {
            oldClient.close();
        }
    }

    @PreDestroy
    public void closeAll() {
        adminClients.values().forEach(AdminClient::close);
        adminClients.clear();
    }

    public List<Map<String, Object>> listTopics(UUID clusterId) {
        AdminClient client = getAdminClient(clusterId);
        Set<String> topicNames = null;
        Exception lastException = null;

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                ListTopicsOptions options = new ListTopicsOptions().listInternal(false);
                topicNames = client.listTopics(options).names().get();
                break;
            } catch (Exception e) {
                lastException = e;
                log.warn("Attempt {} failed to list topics for cluster {}: {}", attempt, clusterId, e.getMessage());
                refreshAdminClient(clusterId);
                client = getAdminClient(clusterId);
                try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }

        if (topicNames == null) {
            log.error("Failed to list topics after 3 attempts", lastException);
            throw new RuntimeException("Failed to list topics: " + (lastException != null ? lastException.getMessage() : "timeout"));
        }

        try {
            return topicNames.stream().map(name -> {
                Map<String, Object> topic = new HashMap<>();
                topic.put("name", name);
                topic.put("isInternal", false);
                return topic;
            }).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to map topics", e);
            throw new RuntimeException("Failed to map topics: " + e.getMessage());
        }
    }

    public io.translab.tantor.server.dto.PaginatedResponse<io.translab.tantor.server.dto.TopicSummaryDto> listTopicsPaginated(UUID clusterId, int page, int size, String search, String sortBy, boolean includeInternal) {
        AdminClient client = getAdminClient(clusterId);
        Set<String> allTopicNames = null;
        Exception lastException = null;

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                ListTopicsOptions options = new ListTopicsOptions().listInternal(includeInternal);
                allTopicNames = client.listTopics(options).names().get();
                break;
            } catch (Exception e) {
                lastException = e;
                log.warn("Attempt {} failed to list topics for cluster {}: {}", attempt, clusterId, e.getMessage());
                refreshAdminClient(clusterId);
                client = getAdminClient(clusterId);
                try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }

        if (allTopicNames == null) {
            log.error("Failed to list topics after 3 attempts", lastException);
            throw new RuntimeException("Failed to list topics: " + (lastException != null ? lastException.getMessage() : "timeout"));
        }

        try {

            // Filter and Sort in memory
            List<String> filteredNames = allTopicNames.stream()
                    .filter(name -> includeInternal || !isManagedInternalTopic(name))
                    .filter(name -> search == null || search.isEmpty() || name.toLowerCase().contains(search.toLowerCase()))
                    .sorted((a, b) -> {
                        if ("name".equalsIgnoreCase(sortBy)) return a.compareToIgnoreCase(b);
                        return a.compareToIgnoreCase(b); // Default fallback
                    })
                    .collect(Collectors.toList());

            // Pagination calculation
            int totalElements = filteredNames.size();
            int totalPages = (int) Math.ceil((double) totalElements / size);
            
            // Validate page bounds
            if (page < 0) page = 0;
            if (page >= totalPages && totalPages > 0) page = totalPages - 1;
            
            int start = page * size;
            int end = Math.min(start + size, totalElements);
            
            List<String> pagedNames = filteredNames.subList(start, end);

            // Fetch metadata ONLY for the current page
            List<io.translab.tantor.server.dto.TopicSummaryDto> content = new ArrayList<>();
            if (!pagedNames.isEmpty()) {
                DescribeTopicsResult describeTopicsResult = client.describeTopics(pagedNames);
                Map<String, TopicDescription> descriptions = describeTopicsResult.allTopicNames().get();
                List<TopicPartition> pagePartitions = descriptions.values().stream()
                        .flatMap(desc -> desc.partitions().stream()
                                .map(partition -> new TopicPartition(desc.name(), partition.partition())))
                        .collect(Collectors.toList());
                Map<TopicPartition, OffsetSpec> earliestRequest = pagePartitions.stream()
                        .collect(Collectors.toMap(tp -> tp, ignored -> OffsetSpec.earliest()));
                Map<TopicPartition, OffsetSpec> latestRequest = pagePartitions.stream()
                        .collect(Collectors.toMap(tp -> tp, ignored -> OffsetSpec.latest()));
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                        client.listOffsets(earliestRequest).all().get();
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                        client.listOffsets(latestRequest).all().get();

                for (String name : pagedNames) {
                    TopicDescription desc = descriptions.get(name);
                    if (desc != null) {
                        int replicationFactor = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
                        long underReplicated = desc.partitions().stream()
                                .filter(p -> p.replicas().size() > p.isr().size())
                                .count();
                        List<TopicPartition> partitions = desc.partitions().stream()
                                .map(p -> new TopicPartition(name, p.partition()))
                                .collect(Collectors.toList());
                        long messageCount = partitions.stream().mapToLong(tp ->
                                Math.max(0, latest.get(tp).offset() - earliest.get(tp).offset())).sum();

                        content.add(io.translab.tantor.server.dto.TopicSummaryDto.builder()
                                .name(desc.name())
                                .partitionCount(desc.partitions().size())
                                .replicationFactor(replicationFactor)
                                .underReplicated(underReplicated)
                                .messageCount(messageCount)
                                .internal(desc.isInternal())
                                .build());
                    }
                }
            }

            return io.translab.tantor.server.dto.PaginatedResponse.<io.translab.tantor.server.dto.TopicSummaryDto>builder()
                    .content(content)
                    .page(page)
                    .size(size)
                    .totalElements(totalElements)
                    .totalPages(totalPages)
                    .hasNext(page < totalPages - 1)
                    .build();

        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to list topics paginated", e);
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to list topics: " + e.getMessage());
        }
    }

    private boolean isManagedInternalTopic(String name) {
        return "__consumer_offsets".equals(name)
                || "_schemas".equals(name)
                || "connect-configs".equals(name)
                || "connect-offsets".equals(name)
                || "connect-status".equals(name);
    }

    public void createTopic(UUID clusterId, String name, int partitions, short replicationFactor, Map<String, String> configs) {
        AdminClient client = getAdminClient(clusterId);
        try {
            NewTopic newTopic = new NewTopic(name, partitions, replicationFactor).configs(configs != null ? configs : Collections.emptyMap());
            client.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to create topic", e);
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to create topic: " + e.getMessage());
        }
    }

    public void deleteTopic(UUID clusterId, String name) {
        AdminClient client = getAdminClient(clusterId);
        try {
            client.deleteTopics(Collections.singletonList(name)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to delete topic", e);
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to delete topic: " + e.getMessage());
        }
    }

    // --- Consumer Group Operations ---

    public List<Map<String, Object>> listConsumerGroups(UUID clusterId) {
        AdminClient client = getAdminClient(clusterId);
        try {
            Collection<ConsumerGroupListing> listings = client.listConsumerGroups().all().get();
            List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
            
            if (groupIds.isEmpty()) return Collections.emptyList();

            Map<String, ConsumerGroupDescription> descriptions = client.describeConsumerGroups(groupIds).all().get();

            return descriptions.values().stream().map(desc -> {
                Map<String, Object> map = new HashMap<>();
                map.put("groupId", desc.groupId());
                map.put("state", desc.state().toString());
                map.put("coordinator", desc.coordinator().host() + ":" + desc.coordinator().port());
                map.put("members", desc.members().size());
                return map;
            }).collect(Collectors.toList());

        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to list consumer groups", e);
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to list consumer groups: " + e.getMessage());
        }
    }

    // --- Configuration Operations ---

    public Map<Integer, Map<String, Object>> getBrokerConfigs(UUID clusterId) {
        AdminClient client = getAdminClient(clusterId);
        try {
            Collection<org.apache.kafka.common.Node> nodes = client.describeCluster().nodes().get();
            List<org.apache.kafka.common.config.ConfigResource> resources = nodes.stream()
                    .map(node -> new org.apache.kafka.common.config.ConfigResource(
                            org.apache.kafka.common.config.ConfigResource.Type.BROKER, String.valueOf(node.id())))
                    .collect(Collectors.toList());

            Map<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config> configs = 
                    client.describeConfigs(resources).all().get();

            Map<Integer, Map<String, Object>> result = new HashMap<>();
            configs.forEach((res, conf) -> {
                Map<String, Object> brokerConf = new HashMap<>();
                conf.entries().forEach(entry -> {
                    Map<String, Object> details = new HashMap<>();
                    details.put("value", entry.value());
                    details.put("isReadOnly", entry.isReadOnly());
                    brokerConf.put(entry.name(), details);
                });
                result.put(Integer.parseInt(res.name()), brokerConf);
            });
            return result;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to get broker configs", e);
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to get broker configs: " + e.getMessage());
        }
    }

    public void alterBrokerConfig(UUID clusterId, int brokerId, String key, String value) {
        AdminClient client = getAdminClient(clusterId);
        try {
            org.apache.kafka.common.config.ConfigResource resource = new org.apache.kafka.common.config.ConfigResource(
                    org.apache.kafka.common.config.ConfigResource.Type.BROKER, String.valueOf(brokerId));

            org.apache.kafka.clients.admin.AlterConfigOp op = new org.apache.kafka.clients.admin.AlterConfigOp(
                    new org.apache.kafka.clients.admin.ConfigEntry(key, value), 
                    org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET);

            client.incrementalAlterConfigs(Map.of(resource, Collections.singletonList(op))).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to alter broker config", e);
            refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to alter broker config: " + e.getMessage());
        }
    }

    private void applySecurityProperties(Properties props, ExternalCluster cluster, boolean decryptPasswords) {
        String protocol = cluster.getSecurityProtocol();
        if (protocol == null || protocol.isBlank()) {
            protocol = "PLAINTEXT";
        }
        if ("PLAINTEXT".equalsIgnoreCase(protocol)) {
            props.put("security.protocol", "PLAINTEXT");
            return;
        }

        props.put("security.protocol", protocol.toUpperCase());

        // SSL Configuration
        if ("SSL".equalsIgnoreCase(protocol) || "SASL_SSL".equalsIgnoreCase(protocol)) {
            if (Boolean.TRUE.equals(cluster.getDisableHostnameVerification())) {
                props.put("ssl.endpoint.identification.algorithm", "");
            }
            String truststorePath = ensureSecurityFile(
                    cluster,
                    cluster.getTruststoreType(),
                    cluster.getTruststoreContentEncrypted(),
                    cluster.getTruststorePath(),
                    decryptPasswords,
                    false
            );
            if (truststorePath != null && !truststorePath.isBlank()) {
                if ("PEM".equalsIgnoreCase(cluster.getTruststoreType())) {
                    props.put("ssl.truststore.type", "PEM");
                    try {
                        String pemContent = java.nio.file.Files.readString(java.nio.file.Paths.get(truststorePath));
                        props.put("ssl.truststore.certificates", pemContent);
                    } catch (Exception e) {
                        log.error("Failed to read PEM truststore from {}", truststorePath, e);
                        throw new RuntimeException("Failed to read PEM truststore", e);
                    }
                } else {
                    props.put("ssl.truststore.location", truststorePath);
                    if (cluster.getTruststorePasswordEncrypted() != null && !cluster.getTruststorePasswordEncrypted().isBlank()) {
                        String pw = decryptPasswords ? encryptionService.decrypt(cluster.getTruststorePasswordEncrypted()) : cluster.getTruststorePasswordEncrypted();
                        props.put("ssl.truststore.password", pw);
                    }
                    if (cluster.getTruststoreType() != null && !cluster.getTruststoreType().isBlank()) {
                        props.put("ssl.truststore.type", cluster.getTruststoreType().toUpperCase());
                    }
                }
            }

            String keystorePath = ensureSecurityFile(
                    cluster,
                    "keystore_" + cluster.getKeystoreType(),
                    cluster.getKeystoreContentEncrypted(),
                    cluster.getKeystorePath(),
                    decryptPasswords,
                    true
            );
            if (keystorePath != null && !keystorePath.isBlank()) {
                props.put("ssl.keystore.location", keystorePath);
                if (cluster.getKeystorePasswordEncrypted() != null && !cluster.getKeystorePasswordEncrypted().isBlank()) {
                    String pw = decryptPasswords ? encryptionService.decrypt(cluster.getKeystorePasswordEncrypted()) : cluster.getKeystorePasswordEncrypted();
                    props.put("ssl.keystore.password", pw);
                }
                if (cluster.getKeyPasswordEncrypted() != null && !cluster.getKeyPasswordEncrypted().isBlank()) {
                    String pw = decryptPasswords ? encryptionService.decrypt(cluster.getKeyPasswordEncrypted()) : cluster.getKeyPasswordEncrypted();
                    props.put("ssl.key.password", pw);
                }
                if (cluster.getKeystoreType() != null && !cluster.getKeystoreType().isBlank()) {
                    props.put("ssl.keystore.type", cluster.getKeystoreType().toUpperCase());
                }
            }
        }

        // SASL Configuration
        if ("SASL_PLAINTEXT".equalsIgnoreCase(protocol) || "SASL_SSL".equalsIgnoreCase(protocol)) {
            String mechanism = cluster.getSaslMechanism();
            if (mechanism == null || mechanism.isBlank()) {
                throw new IllegalArgumentException("SASL mechanism is required for " + protocol);
            }
            props.put("sasl.mechanism", mechanism.toUpperCase());
            
            String username = cluster.getSaslUsername();
            String passwordEnc = cluster.getSaslPasswordEncrypted();
            if (username != null && !username.isBlank() && passwordEnc != null && !passwordEnc.isBlank()) {
                String password = decryptPasswords ? encryptionService.decrypt(passwordEnc) : passwordEnc;
                // Escape quotes and backslashes for JAAS
                String escapedUsername = username.replace("\\", "\\\\").replace("\"", "\\\"");
                String escapedPassword = password.replace("\\", "\\\\").replace("\"", "\\\"");
                
                String module = "org.apache.kafka.common.security.plain.PlainLoginModule";
                if (mechanism.toUpperCase().startsWith("SCRAM")) {
                    module = "org.apache.kafka.common.security.scram.ScramLoginModule";
                }
                String jaas = String.format("%s required username=\"%s\" password=\"%s\";", module, escapedUsername, escapedPassword);
                props.put("sasl.jaas.config", jaas);
            }
        }
    }

    private String ensureSecurityFile(
            ExternalCluster cluster,
            String storeType,
            String encryptedBase64,
            String existingPath,
            boolean decryptContent,
            boolean keystore
    ) {
        if (existingPath != null && !existingPath.isBlank() && java.nio.file.Files.exists(java.nio.file.Paths.get(existingPath))) {
            backfillSecurityContent(cluster, existingPath, encryptedBase64, keystore);
            return existingPath;
        }
        if (encryptedBase64 == null || encryptedBase64.isBlank()) {
            return existingPath;
        }
        String base64 = decryptContent ? encryptionService.decrypt(encryptedBase64) : encryptedBase64;
        String restoredPath = truststoreStorageService.ensureTruststoreFile(
                cluster.getId() == null ? UUID.randomUUID() : cluster.getId(),
                storeType,
                base64,
                existingPath
        );
        if (cluster.getId() != null && restoredPath != null && !restoredPath.equals(existingPath)) {
            if (keystore) {
                cluster.setKeystorePath(restoredPath);
            } else {
                cluster.setTruststorePath(restoredPath);
            }
            externalClusterRepository.save(cluster);
        }
        return restoredPath;
    }

    private void backfillSecurityContent(ExternalCluster cluster, String existingPath, String encryptedBase64, boolean keystore) {
        if (cluster.getId() == null || encryptedBase64 != null && !encryptedBase64.isBlank()) {
            return;
        }
        try {
            byte[] bytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(existingPath));
            String encoded = Base64.getEncoder().encodeToString(bytes);
            if (keystore) {
                cluster.setKeystoreContentEncrypted(encryptionService.encrypt(encoded));
            } else {
                cluster.setTruststoreContentEncrypted(encryptionService.encrypt(encoded));
            }
            externalClusterRepository.save(cluster);
        } catch (Exception e) {
            log.warn("Could not backfill external cluster security material from {}", existingPath, e);
        }
    }
}
