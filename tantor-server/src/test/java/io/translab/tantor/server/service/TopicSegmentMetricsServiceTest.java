package io.translab.tantor.server.service;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TopicSegmentMetricsServiceTest {
    @Test void readsLegacyAndLabelledSamplesWithoutDoubleCountingOrIncludingOtherTopics() {
        String metrics = """
                # HELP kafka_log_Log_NumLogSegments_topic_schema_encoders_partition_0_Value help
                kafka_log_Log_NumLogSegments_topic_schema_encoders_partition_0_Value 1.0
                kafka_log_Log_NumLogSegments_topic_schema_encoders_extra_partition_0_Value 99
                kafka_log_Log_NumLogSegments_topic_schema_encoders_partition_1_Value NaN
                kafka_log_Log_NumLogSegments_topic_schema_encoders_partition_2_Value -1
                kafka_log_Log_NumLogSegments_topic_schema_encoders_partition_3_Value 1.5
                kafka_log_log_numlogsegments{topic="_schema_encoders",partition="0"} 2
                kafka_log_Log_NumLogSegments_Value{partition="4",topic="_schema_encoders"} 3
                kafka_log_log_value{name="NumLogSegments",topic="_schema_encoders",partition="5"} 0
                kafka_log_log_numlogsegments{topic="other",partition="0"} 100
                kafka_log_Log_Size_topic_schema_encoders_partition_0_Value 12345
                """;
        assertThat(TopicSegmentMetricsService.parseSegmentCounts(metrics, "_schema_encoders"))
                .containsExactlyInAnyOrderEntriesOf(Map.of(0, 2L, 4, 3L, 5, 0L));
        assertThat(TopicSegmentMetricsService.parseSegmentCounts("", "orders")).isEmpty();
        assertThat(TopicSegmentMetricsService.parseSegmentCounts("kafka_log_Log_NumLogSegments_topic_Orders_partition_0_Value 5", "orders")).isEmpty();
    }

    @Test void sumsOneLeaderCopyPerPartitionAndCachesScrapes() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/metrics", exchange -> {
            calls.incrementAndGet();
            byte[] body = "kafka_log_Log_NumLogSegments_topic_orders_partition_0_Value 2\nkafka_log_Log_NumLogSegments_topic_orders_partition_1_Value 3\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start();
        try {
            UUID cluster = UUID.randomUUID();
            var monitoring = mock(PrometheusMonitoringService.class);
            when(monitoring.brokerJmxTargets(cluster)).thenReturn(Map.of(1, "127.0.0.1:" + server.getAddress().getPort()));
            var metrics = new TopicSegmentMetricsService(monitoring);
            var topic = topic(1, 1);
            assertThat(metrics.segmentCount(cluster, topic).count()).isEqualTo(5L);
            assertThat(metrics.segmentCount(cluster, topic).count()).isEqualTo(5L);
            assertThat(calls).hasValue(1);
            // A missing leader target must not turn a partial sum into a misleading total.
            assertThat(metrics.segmentCount(cluster, topic(1, 2)).count()).isNull();
            verify(monitoring, times(3)).brokerJmxTargets(cluster);
        } finally { server.stop(0); }
    }

    @Test void unreachableOrMissingPartitionMetricsRemainUnavailable() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = "kafka_log_Log_NumLogSegments_topic_orders_partition_0_Value 2\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start();
        var monitoring = mock(PrometheusMonitoringService.class);
        UUID cluster = UUID.randomUUID();
        when(monitoring.brokerJmxTargets(cluster)).thenReturn(Map.of(1, "127.0.0.1:" + server.getAddress().getPort()));
        assertThat(new TopicSegmentMetricsService(monitoring).segmentCount(cluster, topic(1, 1)).count()).isNull();
        server.stop(0);
        assertThat(new TopicSegmentMetricsService(monitoring).segmentCount(cluster, topic(1, 1)).count()).isNull();
    }

    private TopicDescription topic(int firstLeader, int secondLeader) {
        Node first = new Node(firstLeader, "broker1", 9092), second = new Node(secondLeader, "broker2", 9092);
        List<Node> replicas = List.of(first, second);
        return new TopicDescription("orders", false, List.of(
                new TopicPartitionInfo(0, first, replicas, replicas),
                new TopicPartitionInfo(1, second, replicas, replicas)));
    }
}
