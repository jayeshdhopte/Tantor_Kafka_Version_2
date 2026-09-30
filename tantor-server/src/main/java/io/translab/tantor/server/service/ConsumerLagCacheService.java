package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.dto.ConsumerGroupSummaryDto;
import io.translab.tantor.server.dto.ConsumerGroupDetailDto;
import io.translab.tantor.server.dto.MemberAssignmentDto;
import io.translab.tantor.server.dto.PartitionLagDto;
import io.translab.tantor.server.dto.PaginatedResponse;
import io.translab.tantor.server.repository.ClusterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerLagCacheService {

    private final ClusterRepository clusterRepository;
    private final KafkaAdminService kafkaAdminService;
    private final TransactionTemplate transactionTemplate;

    // Cache: ClusterID -> List of Summaries
    private final Map<UUID, List<ConsumerGroupSummaryDto>> summaryCache = new ConcurrentHashMap<>();
    
    // Cache: ClusterID -> Map<GroupId, Detail>
    private final Map<UUID, Map<String, ConsumerGroupDetailDto>> detailCache = new ConcurrentHashMap<>();

    @Scheduled(fixedDelay = 10000)
    public void refreshCaches() {
        List<Cluster> activeClusters = clusterRepository.findByStatusNot("DELETED");
        for (Cluster cluster : activeClusters) {
            if ("SUCCESS".equals(cluster.getStatus()) || "EXTERNAL".equals(cluster.getMode())) {
                transactionTemplate.execute(status -> {
                    try {
                        refreshConsumerGroups(cluster.getId());
                    } catch (Exception e) {
                        log.warn("Failed to refresh consumer lag cache for cluster {}: {}", cluster.getId(), e.getMessage());
                    }
                    return null;
                });
            }
        }
    }

    private void refreshConsumerGroups(UUID clusterId) throws Exception {
        AdminClient client = kafkaAdminService.getAdminClient(clusterId);
        
        // 1. Fetch all Group IDs
        Collection<ConsumerGroupListing> listings = client.listConsumerGroups().all().get();
        List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
        
        if (groupIds.isEmpty()) {
            summaryCache.put(clusterId, Collections.emptyList());
            detailCache.put(clusterId, Collections.emptyMap());
            return;
        }

        // 2. Describe all Groups
        Map<String, ConsumerGroupDescription> descriptions = client.describeConsumerGroups(groupIds).all().get();

        // 3. Fetch Offsets for all Groups
        Map<String, Map<TopicPartition, OffsetAndMetadata>> allGroupOffsets = new HashMap<>();
        Set<TopicPartition> allPartitions = new HashSet<>();
        
        for (String groupId : groupIds) {
            try {
                Map<TopicPartition, OffsetAndMetadata> offsets = client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                allGroupOffsets.put(groupId, offsets);
                allPartitions.addAll(offsets.keySet());
            } catch (Exception e) {
                log.warn("Failed to fetch offsets for group {}: {}", groupId, e.getMessage());
                allGroupOffsets.put(groupId, Collections.emptyMap());
            }
        }

        // 4. Fetch Log End Offsets (LEO) for all relevant partitions
        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
        for (TopicPartition tp : allPartitions) {
            latestSpecs.put(tp, OffsetSpec.latest());
        }
        
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = new HashMap<>();
        try {
            latestOffsets = client.listOffsets(latestSpecs).all().get();
        } catch (Exception e) {
            log.warn("Failed to fetch latest offsets. Some partitions may be offline: {}", e.getMessage());
            // Fallback: fetch one by one to isolate offline partitions
            for (TopicPartition tp : allPartitions) {
                try {
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> res = client.listOffsets(Collections.singletonMap(tp, OffsetSpec.latest())).all().get();
                    latestOffsets.putAll(res);
                } catch (Exception ex) {
                    log.warn("Partition {} is offline or leaderless", tp);
                }
            }
        }

        long now = System.currentTimeMillis();
        List<ConsumerGroupSummaryDto> summaries = new ArrayList<>();
        Map<String, ConsumerGroupDetailDto> details = new HashMap<>();

        for (Map.Entry<String, ConsumerGroupDescription> entry : descriptions.entrySet()) {
            String groupId = entry.getKey();
            ConsumerGroupDescription desc = entry.getValue();
            Map<TopicPartition, OffsetAndMetadata> groupOffsets = allGroupOffsets.getOrDefault(groupId, Collections.emptyMap());

            long totalLag = 0;
            boolean hasOfflinePartitions = false;
            
            List<MemberAssignmentDto> memberDtos = new ArrayList<>();
            
            // Map partitions to members based on assignment
            Map<TopicPartition, String> partitionToMember = new HashMap<>();
            
            for (MemberDescription member : desc.members()) {
                List<PartitionLagDto> partitionLags = new ArrayList<>();
                for (TopicPartition tp : member.assignment().topicPartitions()) {
                    partitionToMember.put(tp, member.consumerId());
                    
                    long currentOffset = groupOffsets.containsKey(tp) ? groupOffsets.get(tp).offset() : -1;
                    long leo = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : -1;
                    
                    long lag = -1;
                    if (currentOffset != -1 && leo != -1) {
                        lag = Math.max(0, leo - currentOffset);
                        totalLag += lag;
                    } else {
                        hasOfflinePartitions = true;
                    }
                    
                    partitionLags.add(new PartitionLagDto(tp.topic(), tp.partition(), currentOffset, leo, lag));
                }
                
                memberDtos.add(new MemberAssignmentDto(member.consumerId(), member.clientId(), member.host(), partitionLags));
            }
            
            // Handle unassigned partitions that have committed offsets
            List<PartitionLagDto> unassignedLags = new ArrayList<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry : groupOffsets.entrySet()) {
                TopicPartition tp = offsetEntry.getKey();
                if (!partitionToMember.containsKey(tp)) {
                    long currentOffset = offsetEntry.getValue().offset();
                    long leo = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : -1;
                    
                    long lag = -1;
                    if (currentOffset != -1 && leo != -1) {
                        lag = Math.max(0, leo - currentOffset);
                        totalLag += lag;
                    } else {
                        hasOfflinePartitions = true;
                    }
                    unassignedLags.add(new PartitionLagDto(tp.topic(), tp.partition(), currentOffset, leo, lag));
                }
            }
            
            if (!unassignedLags.isEmpty()) {
                memberDtos.add(new MemberAssignmentDto("UNASSIGNED", "none", "none", unassignedLags));
            }
            
            String health = "HEALTHY";
            if (hasOfflinePartitions) health = "WARNING";
            if (desc.state().toString().equals("Dead") || desc.state().toString().equals("Empty")) health = "INACTIVE";

            summaries.add(new ConsumerGroupSummaryDto(
                groupId, 
                desc.state().toString(), 
                desc.members().size(), 
                totalLag, 
                health, 
                now
            ));
            
            details.put(groupId, new ConsumerGroupDetailDto(groupId, desc.state().toString(), memberDtos));
        }

        summaryCache.put(clusterId, summaries);
        detailCache.put(clusterId, details);
    }

    public PaginatedResponse<ConsumerGroupSummaryDto> getPaginatedSummaries(UUID clusterId, int page, int size, String search, String sortBy) {
        List<ConsumerGroupSummaryDto> allCached = summaryCache.getOrDefault(clusterId, Collections.emptyList());

        List<ConsumerGroupSummaryDto> filtered = allCached.stream()
                .filter(c -> search == null || search.isEmpty() || c.getGroupId().toLowerCase().contains(search.toLowerCase()))
                .collect(Collectors.toList());

        filtered.sort((a, b) -> {
            if ("totalLag".equalsIgnoreCase(sortBy)) return Long.compare(b.getTotalLag(), a.getTotalLag()); // DESC
            if ("state".equalsIgnoreCase(sortBy)) return a.getState().compareToIgnoreCase(b.getState());
            if ("health".equalsIgnoreCase(sortBy)) return a.getHealth().compareToIgnoreCase(b.getHealth());
            if ("membersCount".equalsIgnoreCase(sortBy)) return Integer.compare(b.getMembersCount(), a.getMembersCount()); // DESC
            
            // Default: groupId ASC
            return a.getGroupId().compareToIgnoreCase(b.getGroupId());
        });

        int totalElements = filtered.size();
        int totalPages = (int) Math.ceil((double) totalElements / size);
        if (page < 0) page = 0;
        if (page >= totalPages && totalPages > 0) page = totalPages - 1;
        
        int start = page * size;
        int end = Math.min(start + size, totalElements);
        List<ConsumerGroupSummaryDto> paged = start <= end && start < totalElements ? filtered.subList(start, end) : Collections.emptyList();

        return PaginatedResponse.<ConsumerGroupSummaryDto>builder()
                .content(paged)
                .page(page)
                .size(size)
                .totalElements(totalElements)
                .totalPages(totalPages)
                .hasNext(page < totalPages - 1)
                .build();
    }

    public ConsumerGroupDetailDto getDetail(UUID clusterId, String groupId) {
        Map<String, ConsumerGroupDetailDto> map = detailCache.getOrDefault(clusterId, Collections.emptyMap());
        return map.get(groupId);
    }

    public List<ConsumerGroupSummaryDto> getSummaries(UUID clusterId) {
        return List.copyOf(summaryCache.getOrDefault(clusterId, Collections.emptyList()));
    }
}
