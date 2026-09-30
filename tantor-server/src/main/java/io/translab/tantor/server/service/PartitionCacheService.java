package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.dto.PartitionSummaryDto;
import io.translab.tantor.server.dto.PaginatedResponse;
import io.translab.tantor.server.repository.ClusterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PartitionCacheService {

    private final ClusterRepository clusterRepository;
    private final KafkaAdminService kafkaAdminService;
    private final TransactionTemplate transactionTemplate;

    // Cache: ClusterID -> List of Partitions
    private final Map<UUID, List<PartitionSummaryDto>> partitionCache = new ConcurrentHashMap<>();

    @Scheduled(fixedDelay = 10000)
    public void refreshCaches() {
        List<Cluster> activeClusters = clusterRepository.findByStatusNot("DELETED");
        for (Cluster cluster : activeClusters) {
            if ("SUCCESS".equals(cluster.getStatus()) || "EXTERNAL".equals(cluster.getMode())) {
                transactionTemplate.execute(status -> {
                    try {
                        refreshClusterPartitions(cluster.getId());
                    } catch (Exception e) {
                        log.warn("Failed to refresh partition cache for cluster {}: {}", cluster.getId(), e.getMessage());
                    }
                    return null;
                });
            }
        }
    }

    private void refreshClusterPartitions(UUID clusterId) throws Exception {
        AdminClient client = kafkaAdminService.getAdminClient(clusterId);
        
        // 1. Fetch Node topology for Hostname mapping
        Collection<Node> nodes = client.describeCluster().nodes().get();
        Map<Integer, String> nodeHostnameMap = new HashMap<>();
        for (Node node : nodes) {
            nodeHostnameMap.put(node.id(), node.host());
        }

        // 2. Fetch all Topics
        ListTopicsOptions options = new ListTopicsOptions().listInternal(false);
        Set<String> topicNames = client.listTopics(options).names().get();
        if (topicNames.isEmpty()) {
            partitionCache.put(clusterId, Collections.emptyList());
            return;
        }

        // 3. Describe all Topics to get Partition Info
        Map<String, TopicDescription> descriptions = client.describeTopics(topicNames).allTopicNames().get();

        // 4. Collect all TopicPartitions for Offset queries
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            for (TopicPartitionInfo tpi : entry.getValue().partitions()) {
                allPartitions.add(new TopicPartition(entry.getKey(), tpi.partition()));
            }
        }

        // 5. Build OffsetSpecs for Earliest and Latest
        Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
        for (TopicPartition tp : allPartitions) {
            earliestSpecs.put(tp, OffsetSpec.earliest());
            latestSpecs.put(tp, OffsetSpec.latest());
        }

        // 6. Fetch Offsets
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets = client.listOffsets(earliestSpecs).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = client.listOffsets(latestSpecs).all().get();

        // 7. Build DTOs
        List<PartitionSummaryDto> dtos = new ArrayList<>();
        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            String topicName = entry.getKey();
            for (TopicPartitionInfo tpi : entry.getValue().partitions()) {
                TopicPartition tp = new TopicPartition(topicName, tpi.partition());
                
                long earliest = earliestOffsets.containsKey(tp) ? earliestOffsets.get(tp).offset() : 0;
                long latest = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : 0;
                long messageCount = latest > earliest ? (latest - earliest) : 0;

                int leaderId = tpi.leader() != null ? tpi.leader().id() : -1;
                String leaderHostname = nodeHostnameMap.getOrDefault(leaderId, "Unknown");
                
                List<Integer> replicas = tpi.replicas().stream().map(Node::id).collect(Collectors.toList());
                List<Integer> isr = tpi.isr().stream().map(Node::id).collect(Collectors.toList());
                
                boolean underReplicated = replicas.size() > isr.size();
                String health = "HEALTHY";
                if (leaderId == -1) {
                    health = "CRITICAL";
                } else if (underReplicated) {
                    health = "WARNING";
                }

                PartitionSummaryDto dto = PartitionSummaryDto.builder()
                        .topicName(topicName)
                        .partitionId(tpi.partition())
                        .leaderBroker(leaderId)
                        .leaderHostname(leaderHostname)
                        .replicaBrokers(replicas)
                        .isrBrokers(isr)
                        .earliestOffset(earliest)
                        .latestOffset(latest)
                        .messageCount(messageCount)
                        .underReplicated(underReplicated)
                        .health(health)
                        .build();
                
                dtos.add(dto);
            }
        }

        partitionCache.put(clusterId, dtos);
    }

    public PaginatedResponse<PartitionSummaryDto> getPaginatedPartitions(UUID clusterId, int page, int size, String search, String sortBy) {
        List<PartitionSummaryDto> allCached = partitionCache.getOrDefault(clusterId, Collections.emptyList());

        // Filter
        List<PartitionSummaryDto> filtered = allCached.stream()
                .filter(p -> search == null || search.isEmpty() || p.getTopicName().toLowerCase().contains(search.toLowerCase()))
                .collect(Collectors.toList());

        // Sort
        filtered.sort((a, b) -> {
            if ("topicName".equalsIgnoreCase(sortBy)) {
                int cmp = a.getTopicName().compareToIgnoreCase(b.getTopicName());
                if (cmp == 0) return Integer.compare(a.getPartitionId(), b.getPartitionId());
                return cmp;
            }
            if ("partitionId".equalsIgnoreCase(sortBy)) return Integer.compare(a.getPartitionId(), b.getPartitionId());
            if ("leaderBroker".equalsIgnoreCase(sortBy)) return Integer.compare(a.getLeaderBroker(), b.getLeaderBroker());
            if ("messageCount".equalsIgnoreCase(sortBy)) return Long.compare(b.getMessageCount(), a.getMessageCount()); // DESC default
            if ("health".equalsIgnoreCase(sortBy)) return a.getHealth().compareToIgnoreCase(b.getHealth());
            
            // Default sort: topicName ASC, partitionId ASC
            int cmp = a.getTopicName().compareToIgnoreCase(b.getTopicName());
            if (cmp == 0) return Integer.compare(a.getPartitionId(), b.getPartitionId());
            return cmp;
        });

        // Paginate
        int totalElements = filtered.size();
        int totalPages = (int) Math.ceil((double) totalElements / size);
        if (page < 0) page = 0;
        if (page >= totalPages && totalPages > 0) page = totalPages - 1;
        
        int start = page * size;
        int end = Math.min(start + size, totalElements);
        List<PartitionSummaryDto> paged = filtered.subList(start, end);

        return PaginatedResponse.<PartitionSummaryDto>builder()
                .content(paged)
                .page(page)
                .size(size)
                .totalElements(totalElements)
                .totalPages(totalPages)
                .hasNext(page < totalPages - 1)
                .build();
    }
}
