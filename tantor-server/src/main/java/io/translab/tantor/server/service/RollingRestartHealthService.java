package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class RollingRestartHealthService {
    private final KafkaAdminService kafkaAdminService;
    private final HostRepository hostRepository;

    @Value("${tantor.rolling-restart.max-disk-used-percent:85}")
    private double maxDiskUsedPercent;
    @Value("${tantor.rolling-restart.max-consumer-lag:1000000}")
    private long maxConsumerLag;
    @Value("${tantor.hosts.heartbeat-timeout-seconds:90}")
    private long heartbeatTimeoutSeconds;

    public HealthSnapshot inspect(UUID clusterId, int expectedBrokerCount, Set<String> requiredHostIds) {
        Map<String, String> failures = new LinkedHashMap<>();
        int visibleBrokers = 0;
        int offlinePartitions = 0;
        int underReplicatedPartitions = 0;
        int minIsrViolations = 0;
        long consumerLag = 0;
        Integer controllerId = null;

        try {
            AdminClient client = kafkaAdminService.getAdminClient(clusterId);
            DescribeClusterResult cluster = client.describeCluster();
            Collection<org.apache.kafka.common.Node> nodes = cluster.nodes().get();
            org.apache.kafka.common.Node controller = cluster.controller().get();
            visibleBrokers = nodes.size();
            controllerId = controller == null ? null : controller.id();
            if (visibleBrokers < expectedBrokerCount) {
                failures.put("brokerReachability", "Only " + visibleBrokers + " of " + expectedBrokerCount + " brokers are reachable");
            }
            if (controllerId == null || controllerId < 0) failures.put("controller", "Kafka has no active controller");

            Set<String> topicNames = client.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
            Map<String, TopicDescription> topics = topicNames.isEmpty()
                    ? Map.of() : client.describeTopics(topicNames).allTopicNames().get();
            Map<String, Integer> minimumIsr = loadMinimumIsr(client, topicNames);
            for (TopicDescription topic : topics.values()) {
                int requiredIsr = minimumIsr.getOrDefault(topic.name(), 1);
                for (var partition : topic.partitions()) {
                    if (partition.leader() == null || partition.leader().id() < 0) offlinePartitions++;
                    if (partition.isr().size() < partition.replicas().size()) underReplicatedPartitions++;
                    if (partition.isr().size() < requiredIsr) minIsrViolations++;
                }
            }
            if (offlinePartitions > 0) failures.put("offlinePartitions", offlinePartitions + " partition(s) have no leader");
            if (underReplicatedPartitions > 0) failures.put("underReplicatedPartitions", underReplicatedPartitions + " partition(s) are under-replicated");
            if (minIsrViolations > 0) failures.put("minimumIsr", minIsrViolations + " partition(s) are below min.insync.replicas");

            consumerLag = totalConsumerLag(client);
            if (consumerLag > maxConsumerLag) {
                failures.put("consumerLag", "Total consumer lag " + consumerLag + " exceeds limit " + maxConsumerLag);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failures.put("kafka", "Health inspection was interrupted");
        } catch (Exception e) {
            log.warn("Rolling restart health inspection failed for cluster {}", clusterId, e);
            kafkaAdminService.refreshAdminClient(clusterId);
            failures.put("kafka", "Kafka health inspection failed: " + rootMessage(e));
        }

        inspectHosts(requiredHostIds, failures);
        return new HealthSnapshot(failures.isEmpty(), visibleBrokers, controllerId, offlinePartitions,
                underReplicatedPartitions, minIsrViolations, consumerLag, Collections.unmodifiableMap(new LinkedHashMap<>(failures)));
    }

    private Map<String, Integer> loadMinimumIsr(AdminClient client, Set<String> topicNames) throws Exception {
        if (topicNames.isEmpty()) return Map.of();
        Map<ConfigResource, String> resources = topicNames.stream().collect(Collectors.toMap(
                name -> new ConfigResource(ConfigResource.Type.TOPIC, name), name -> name));
        Map<ConfigResource, Config> configs = client.describeConfigs(resources.keySet()).all().get();
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
            ConfigEntry value = entry.getValue().get("min.insync.replicas");
            if (value != null && value.value() != null) {
                result.put(resources.get(entry.getKey()), Integer.parseInt(value.value()));
            }
        }
        return result;
    }

    private long totalConsumerLag(AdminClient client) throws Exception {
        long total = 0;
        for (ConsumerGroupListing group : client.listConsumerGroups().all().get()) {
            Map<TopicPartition, OffsetAndMetadata> committed = client.listConsumerGroupOffsets(group.groupId())
                    .partitionsToOffsetAndMetadata().get();
            if (committed.isEmpty()) continue;
            Map<TopicPartition, OffsetSpec> request = committed.keySet().stream()
                    .collect(Collectors.toMap(partition -> partition, ignored -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = client.listOffsets(request).all().get();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : committed.entrySet()) {
                ListOffsetsResult.ListOffsetsResultInfo end = latest.get(offset.getKey());
                if (end != null) total += Math.max(0, end.offset() - offset.getValue().offset());
            }
        }
        return total;
    }

    private void inspectHosts(Set<String> requiredHostIds, Map<String, String> failures) {
        OffsetDateTime cutoff = OffsetDateTime.now().minusSeconds(heartbeatTimeoutSeconds);
        List<String> unavailable = new ArrayList<>();
        List<String> diskPressure = new ArrayList<>();
        for (String hostId : requiredHostIds) {
            Host host = hostRepository.findById(hostId).orElse(null);
            if (host == null || host.getLastHeartbeat() == null || host.getLastHeartbeat().isBefore(cutoff)
                    || !"ONLINE".equalsIgnoreCase(host.getStatus())) {
                unavailable.add(hostId);
                continue;
            }
            if (host.getDiskTotalGb() != null && host.getDiskTotalGb() > 0 && host.getDiskUsedGb() != null) {
                double usedPercent = (host.getDiskUsedGb() * 100.0) / host.getDiskTotalGb();
                if (usedPercent >= maxDiskUsedPercent) diskPressure.add(hostId + " (" + Math.round(usedPercent) + "%)");
            }
        }
        if (!unavailable.isEmpty()) failures.put("hostReachability", "Agent heartbeat missing or stale for: " + String.join(", ", unavailable));
        if (!diskPressure.isEmpty()) failures.put("disk", "Disk usage is above the safe limit on: " + String.join(", ", diskPressure));
    }

    private String rootMessage(Exception exception) {
        Throwable current = exception;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    public record HealthSnapshot(boolean healthy, int visibleBrokers, Integer controllerId,
            int offlinePartitions, int underReplicatedPartitions, int minIsrViolations,
            long consumerLag, Map<String, String> failures) {
        public String failureReason() {
            return failures.values().stream().collect(Collectors.joining("; "));
        }
    }
}
