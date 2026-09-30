package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.dto.ClusterOverviewDto;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ClusterOverviewService {

    private final ClusterRepository clusterRepository;
    private final ExternalClusterRepository externalClusterRepository;
    private final HostRepository hostRepository;
    private final KafkaAdminService kafkaAdminService;
    private final HostStatusService hostStatusService;

    @Transactional(readOnly = true)
    public ClusterOverviewDto getOverview(UUID clusterId) {
        String clusterName;
        String kafkaVersion;
        String originType;
        String mode;
        String installDirectory;
        String configDirectory;
        String dataDirectory;
        String logDirectory;
        Cluster internalCluster = null;
        
        var internalOpt = clusterRepository.findById(clusterId);
        boolean hasInternalCluster = internalOpt.isPresent()
                && !"EXTERNAL".equalsIgnoreCase(internalOpt.get().getMode());
        if (hasInternalCluster) {
            Cluster cluster = internalOpt.get();
            internalCluster = cluster;
            clusterName = cluster.getName();
            kafkaVersion = cluster.getKafkaVersion();
            originType = cluster.getOriginType();
            mode = cluster.getMode();
            installDirectory = cluster.getInstallDirectory();
            configDirectory = cluster.getConfigDirectory();
            dataDirectory = cluster.getDataDirectory();
            logDirectory = cluster.getLogDirectory();
        } else {
            // External-cluster registration also keeps a compatibility Cluster
            // row with mode=EXTERNAL. That shadow row is routing metadata, not
            // the source of Kafka coordination mode/version. Always load those
            // fields from ExternalCluster for an external overview.
            ExternalCluster ext = externalClusterRepository.findById(clusterId)
                    .orElseThrow(() -> new IllegalArgumentException("Cluster not found"));
            clusterName = ext.getName();
            kafkaVersion = ext.getKafkaVersion();
            originType = "EXTERNAL";
            mode = ext.getKafkaMode();
            installDirectory = ext.getInstallPath();
            configDirectory = "";
            dataDirectory = "";
            logDirectory = ext.getLogDirs();
        }

        AdminClient client = kafkaAdminService.getAdminClient(clusterId);
        List<String> warnings = new ArrayList<>();

        try {
            DescribeClusterResult clusterResult = client.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get();
            Node controller = clusterResult.controller().get();
            String kafkaClusterId = clusterResult.clusterId().get();
            boolean kraft = isKraftMode(mode);
            boolean zookeeper = isZooKeeperMode(mode);
            Integer activeControllerId = kraft
                    ? kafkaAdminService.getControllerId(clusterId)
                    : (zookeeper && controller != null ? controller.id() : null);
            if (kraft && activeControllerId == null) {
                warnings.add("The active KRaft controller is unavailable because metadata quorum details could not be loaded.");
            }

            Map<Integer, BrokerAccumulator> brokerStats = new LinkedHashMap<>();
            nodes.stream()
                    .sorted(Comparator.comparingInt(Node::id))
                    .forEach(node -> brokerStats.put(node.id(), new BrokerAccumulator(node)));

            PartitionAccumulator partitionStats = collectPartitionStats(client, brokerStats, warnings);
            collectLogDirStats(client, brokerStats, warnings);
            applyInternalHostDiskStats(internalCluster, brokerStats);

            int brokerCount = brokerStats.size();
            List<ClusterOverviewDto.ControllerRow> controllers = kraft
                    ? internalControllerRows(internalCluster, activeControllerId)
                    : List.of();
            int configuredControllerCount = controllers.size();
            double avgReplicas = brokerCount == 0 ? 0 : (double) partitionStats.totalReplicas / brokerCount;
            double avgLeaders = brokerCount == 0 ? 0 : (double) partitionStats.totalPartitions / brokerCount;

            List<ClusterOverviewDto.BrokerRow> brokers = brokerStats.values().stream()
                    .map(stats -> stats.toDto(activeControllerId != null && stats.node.id() == activeControllerId, avgReplicas, avgLeaders, brokerCount))
                    .toList();

            String controllerType = zookeeper ? "ZooKeeper" : (kraft ? "KRaft" : "Not reported");
            return ClusterOverviewDto.builder()
                    .clusterId(clusterId)
                    .kafkaClusterId(kafkaClusterId)
                    .name(clusterName)
                    .kafkaVersion(kafkaVersion)
                    .controllerType(controllerType)
                    .originType(originType)
                    .installDirectory(installDirectory)
                    .configDirectory(configDirectory)
                    .dataDirectory(dataDirectory)
                    .logDirectory(logDirectory)
                    .generatedAt(OffsetDateTime.now())
                    .warnings(warnings)
                    .uptime(ClusterOverviewDto.UptimeSummary.builder()
                            .brokerCount(brokerCount)
                            .activeController(activeControllerId)
                            .activeControllerId(activeControllerId)
                            .configuredControllerCount(configuredControllerCount)
                            .version(kafkaVersion)
                            .controllerType(controllerType)
                            .build())
                    .partitions(ClusterOverviewDto.PartitionSummary.builder()
                            .online(partitionStats.onlinePartitions)
                            .total(partitionStats.totalPartitions)
                            .underReplicated(partitionStats.underReplicatedPartitions)
                            .inSyncReplicas(partitionStats.inSyncReplicas)
                            .totalReplicas(partitionStats.totalReplicas)
                            .outOfSyncReplicas(Math.max(0, partitionStats.totalReplicas - partitionStats.inSyncReplicas))
                            .build())
                    .brokers(brokers)
                    .controllers(controllers)
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            kafkaAdminService.refreshAdminClient(clusterId);
            throw new RuntimeException("Interrupted while loading cluster overview");
        } catch (Exception e) {
            kafkaAdminService.refreshAdminClient(clusterId);
            throw new RuntimeException("Failed to load cluster overview: " + e.getMessage(), e);
        }
    }

    private List<ClusterOverviewDto.ControllerRow> internalControllerRows(
            Cluster cluster,
            Integer activeControllerId
    ) {
        if (cluster == null || cluster.getServices() == null) {
            return List.of();
        }
        return cluster.getServices().stream()
                .filter(service -> isControllerRole(service.getRole()))
                .filter(service -> service.getNodeId() != null)
                .sorted(Comparator.comparingInt(service -> service.getNodeId()))
                .map(service -> ClusterOverviewDto.ControllerRow.builder()
                        .nodeId(service.getNodeId())
                        .host(controllerHost(service.getHostId()))
                        .port(controllerPort(service.getConfigJson(), cluster.getConfigJson()))
                        .activeLeader(activeControllerId != null
                                && activeControllerId.equals(service.getNodeId()))
                        .build())
                .toList();
    }

    private boolean isControllerRole(String role) {
        if (role == null) {
            return false;
        }
        String normalized = role.trim().toLowerCase(java.util.Locale.ROOT);
        return "controller".equals(normalized) || "broker_controller".equals(normalized);
    }

    private String controllerHost(String hostId) {
        if (hostId == null || hostId.isBlank()) {
            return "unknown";
        }
        return hostRepository.findById(hostId)
                .map(host -> firstNonBlank(host.getHostIp(), host.getHostname(), hostId))
                .orElse(hostId);
    }

    private Integer controllerPort(String serviceConfigJson, String clusterConfigJson) {
        Integer servicePort = configInt(serviceConfigJson, "controller_port");
        if (servicePort != null) {
            return servicePort;
        }
        Integer clusterPort = configInt(clusterConfigJson, "controller_port");
        return clusterPort != null ? clusterPort : 9093;
    }

    private Integer configInt(String configJson, String key) {
        if (configJson == null || configJson.isBlank()) {
            return null;
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(
                "\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*\"?(\\d+)\"?"
        ).matcher(configJson);
        if (!matcher.find()) {
            return null;
        }
        try {
            return Integer.parseInt(matcher.group(1));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "unknown";
    }

    private boolean isKraftMode(String mode) {
        return "kraft".equalsIgnoreCase(mode);
    }

    private boolean isZooKeeperMode(String mode) {
        return "zookeeper".equalsIgnoreCase(mode) || "zk".equalsIgnoreCase(mode);
    }

    private void applyInternalHostDiskStats(
            Cluster cluster,
            Map<Integer, BrokerAccumulator> brokerStats
    ) {
        if (cluster == null || cluster.getServices() == null) {
            return;
        }
        cluster.getServices().forEach(service -> {
            if (service.getNodeId() == null || service.getHostId() == null) {
                return;
            }
            BrokerAccumulator broker = brokerStats.get(service.getNodeId());
            if (broker == null) {
                return;
            }
            hostRepository.findById(service.getHostId()).ifPresent(host -> {
                broker.hostDiskLastSeen = host.getLastHeartbeat();
                boolean agentOnline = "ONLINE".equalsIgnoreCase(
                        hostStatusService.agentConnectivityStatus(host)
                );
                if (!agentOnline) {
                    broker.hostDiskMetricStatus = host.getLastHeartbeat() == null ? "UNAVAILABLE" : "STALE";
                    return;
                }
                broker.hostDiskMetricStatus = "LIVE";
                broker.hostDiskUsedBytes = host.getDiskUsedGb() == null ? null : gibibytesToBytes(host.getDiskUsedGb());
                broker.hostDiskTotalBytes = host.getDiskTotalGb() == null ? null : gibibytesToBytes(host.getDiskTotalGb());
            });
        });
    }

    private long gibibytesToBytes(long value) {
        if (value <= 0) {
            return 0;
        }
        long gibibyte = 1024L * 1024L * 1024L;
        return value > Long.MAX_VALUE / gibibyte ? Long.MAX_VALUE : value * gibibyte;
    }

    private PartitionAccumulator collectPartitionStats(
            AdminClient client,
            Map<Integer, BrokerAccumulator> brokerStats,
            List<String> warnings
    ) throws Exception {
        PartitionAccumulator totals = new PartitionAccumulator();
        Set<String> topicNames = client.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
        if (topicNames.isEmpty()) {
            return totals;
        }

        DescribeTopicsResult describeTopicsResult = client.describeTopics(topicNames);
        Map<String, TopicDescription> topics = describeTopicsResult.allTopicNames().get();
        for (TopicDescription topic : topics.values()) {
            for (TopicPartitionInfo partition : topic.partitions()) {
                totals.totalPartitions++;
                if (partition.leader() != null && partition.leader().id() >= 0) {
                    totals.onlinePartitions++;
                    brokerStats.computeIfAbsent(partition.leader().id(), BrokerAccumulator::new).leaders++;
                }

                Set<Integer> isrIds = new HashSet<>();
                for (Node isrNode : partition.isr()) {
                    isrIds.add(isrNode.id());
                }

                totals.inSyncReplicas += isrIds.size();
                totals.totalReplicas += partition.replicas().size();
                if (partition.replicas().size() > isrIds.size()) {
                    totals.underReplicatedPartitions++;
                }

                for (Node replica : partition.replicas()) {
                    BrokerAccumulator broker = brokerStats.computeIfAbsent(replica.id(), BrokerAccumulator::new);
                    broker.replicas++;
                    if (isrIds.contains(replica.id())) {
                        broker.inSyncReplicas++;
                    }
                }
            }
        }
        return totals;
    }

    private void collectLogDirStats(
            AdminClient client,
            Map<Integer, BrokerAccumulator> brokerStats,
            List<String> warnings
    ) {
        if (brokerStats.isEmpty()) {
            return;
        }

        try {
            DescribeLogDirsResult result = client.describeLogDirs(brokerStats.keySet());
            for (Map.Entry<Integer, org.apache.kafka.common.KafkaFuture<Map<String, LogDirDescription>>> brokerEntry : result.descriptions().entrySet()) {
                BrokerAccumulator broker = brokerStats.get(brokerEntry.getKey());
                if (broker == null) {
                    continue;
                }

                Map<String, LogDirDescription> logDirs = brokerEntry.getValue().get();
                for (LogDirDescription logDir : logDirs.values()) {
                    if (logDir.error() != null) {
                        warnings.add("Log directory details are incomplete for broker " + brokerEntry.getKey() + ": " + logDir.error().getMessage());
                        continue;
                    }
                    for (ReplicaInfo replicaInfo : logDir.replicaInfos().values()) {
                        broker.diskUsageBytes += Math.max(0L, replicaInfo.size());
                        broker.logReplicaCount++;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to load Kafka log directory details: {}", e.getMessage());
            warnings.add("Disk usage is unavailable because Kafka log directory details could not be loaded.");
        }
    }

    private String controllerType(Cluster cluster) {
        if ("zookeeper".equalsIgnoreCase(cluster.getMode())) {
            return "ZooKeeper";
        }
        return "KRaft";
    }

    private static Integer skewPct(int count, double average, int brokerCount) {
        if (brokerCount <= 1 || average <= 0) {
            return null;
        }
        return (int) Math.round(((count - average) / average) * 100.0);
    }

    private static class PartitionAccumulator {
        int onlinePartitions;
        int totalPartitions;
        int underReplicatedPartitions;
        int inSyncReplicas;
        int totalReplicas;
    }

    private static class BrokerAccumulator {
        final Node node;
        int inSyncReplicas;
        int replicas;
        int leaders;
        long diskUsageBytes;
        long diskTotalBytes;
        Long hostDiskUsedBytes;
        Long hostDiskTotalBytes;
        String hostDiskMetricStatus = "UNAVAILABLE";
        OffsetDateTime hostDiskLastSeen;
        int logReplicaCount;

        BrokerAccumulator(Node node) {
            this.node = node;
        }

        BrokerAccumulator(int brokerId) {
            this.node = new Node(brokerId, "unknown", -1);
        }

        ClusterOverviewDto.BrokerRow toDto(boolean controller, double avgReplicas, double avgLeaders, int brokerCount) {
            return ClusterOverviewDto.BrokerRow.builder()
                    .brokerId(node.id())
                    .host(node.host())
                    .port(node.port())
                    .rack(node.rack() == null ? "" : node.rack())
                    .controller(controller)
                    .diskUsageBytes(diskUsageBytes)
                    .diskTotalBytes(diskTotalBytes)
                    .hostDiskUsedBytes(hostDiskUsedBytes)
                    .hostDiskTotalBytes(hostDiskTotalBytes)
                    .hostDiskMetricStatus(hostDiskMetricStatus)
                    .hostDiskLastSeen(hostDiskLastSeen)
                    .logReplicaCount(logReplicaCount)
                    .inSyncReplicas(inSyncReplicas)
                    .replicas(replicas)
                    .replicaSkewPct(skewPct(replicas, avgReplicas, brokerCount))
                    .leaders(leaders)
                    .leaderSkewPct(skewPct(leaders, avgLeaders, brokerCount))
                    .build();
        }
    }
}
