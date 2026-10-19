package io.translab.tantor.server.service;

import io.translab.tantor.server.audit.AuditService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TopicOperationsServiceDetailsTest {
    @Test void detailsExposeRealSegmentCountAndPreserveSizeAndPartitionFields() {
        UUID cluster = UUID.randomUUID();
        String name = "_schema_encoders";
        Node node = new Node(1, "broker", 9092);
        var description = new TopicDescription(name, true,
                List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))));
        var kafka = mock(KafkaAdminService.class);
        var admin = mock(AdminClient.class);
        when(kafka.getAdminClient(cluster)).thenReturn(admin);
        var topics = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(any(java.util.Collection.class))).thenReturn(topics);
        when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(name, description)));
        var offsets = mock(ListOffsetsResult.class);
        when(admin.listOffsets(any())).thenReturn(offsets);
        when(offsets.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
                new TopicPartition(name, 0), new ListOffsetsResult.ListOffsetsResultInfo(0, 0, java.util.Optional.empty()))));
        var configs = mock(DescribeConfigsResult.class);
        when(admin.describeConfigs(any(), any())).thenReturn(configs);
        when(configs.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
                new ConfigResource(ConfigResource.Type.TOPIC, name), new Config(List.of(new ConfigEntry("cleanup.policy", "compact"))))));
        var dirs = mock(DescribeLogDirsResult.class);
        when(admin.describeLogDirs(any())).thenReturn(dirs);
        when(dirs.allDescriptions()).thenReturn(KafkaFuture.completedFuture(Map.of()));
        var segments = mock(TopicSegmentMetricsService.class);
        when(segments.segmentCount(cluster, description)).thenReturn(new TopicSegmentMetricsService.SegmentCount(1L, null));
        var service = new TopicOperationsService(kafka, mock(AuditService.class), segments);
        var details = service.getTopicDetails(cluster, name);
        assertThat(details).containsEntry("segmentCount", 1L).containsEntry("storedBytes", 0L)
                .containsEntry("partitionCount", 1).containsEntry("messageCount", 0L);
        when(segments.segmentCount(cluster, description)).thenReturn(new TopicSegmentMetricsService.SegmentCount(null, "JMX unavailable"));
        assertThat(service.getTopicDetails(cluster, name)).containsEntry("segmentCount", null)
                .containsEntry("segmentCountUnavailableReason", "JMX unavailable");
    }
}
