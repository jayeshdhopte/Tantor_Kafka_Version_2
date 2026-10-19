package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.repository.ClusterRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConsumerLagCacheServiceTest {
    @Test void exposesTheKafkaCoordinatorIdIncludingZeroAndHandlesMissingCoordinator() throws Exception {
        UUID clusterId = UUID.randomUUID();
        String group = "app-consumer";
        var kafka = mock(KafkaAdminService.class);
        var admin = mock(AdminClient.class);
        when(kafka.getAdminClient(clusterId)).thenReturn(admin);
        var groups = mock(ListConsumerGroupsResult.class);
        var listing = mock(ConsumerGroupListing.class);
        when(listing.groupId()).thenReturn(group);
        when(groups.all()).thenReturn(KafkaFuture.completedFuture(List.of(listing)));
        when(admin.listConsumerGroups()).thenReturn(groups);
        var description = mock(ConsumerGroupDescription.class);
        when(description.groupId()).thenReturn(group);
        when(description.state()).thenReturn(ConsumerGroupState.EMPTY);
        when(description.members()).thenReturn(List.of());
        var described = mock(DescribeConsumerGroupsResult.class);
        when(described.all()).thenReturn(KafkaFuture.completedFuture(Map.of(group, description)));
        when(admin.describeConsumerGroups(any())).thenReturn(described);
        var offsets = mock(ListConsumerGroupOffsetsResult.class);
        when(offsets.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(Map.<org.apache.kafka.common.TopicPartition, OffsetAndMetadata>of()));
        when(admin.listConsumerGroupOffsets(group)).thenReturn(offsets);
        var latest = mock(ListOffsetsResult.class);
        when(latest.all()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        when(admin.listOffsets(any())).thenReturn(latest);
        var service = new ConsumerLagCacheService(mock(ClusterRepository.class), kafka, mock(TransactionTemplate.class));
        for (Node coordinator : Arrays.asList(new Node(7, "broker", 9092), new Node(0, "broker", 9092), Node.noNode(), null)) {
            when(description.coordinator()).thenReturn(coordinator);
            ReflectionTestUtils.invokeMethod(service, "refreshConsumerGroups", clusterId);
            var result = service.getDetail(clusterId, group);
            Integer expected = coordinator == null || coordinator.id() < 0 ? null : coordinator.id();
            assertThat(result.getCoordinatorId()).isEqualTo(expected);
            assertThat(service.getSummaries(clusterId).getFirst().getCoordinatorId()).isEqualTo(expected);
            var json = new ObjectMapper().valueToTree(result);
            if (expected == null) assertThat(json.path("coordinatorId").isNull()).isTrue();
            else assertThat(json.path("coordinatorId").asInt()).isEqualTo(expected);
            assertThat(result.getGroupId()).isEqualTo(group);
            assertThat(result.getMembers()).isEmpty();
        }
    }
}
