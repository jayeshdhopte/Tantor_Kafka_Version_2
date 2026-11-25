package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopicProducerDiscoveryServiceTest {
    @TempDir
    Path directory;
    private final UUID id = UUID.randomUUID();

    @Test
    void returnsOnlyConfirmedRowsForTheRequestedTopicAndKeepsLatestObservation() throws Exception {
        Instant latest = Instant.now().minusSeconds(5);
        String rows = String.join(",",
                record("orders", "192.0.2.10", "checkout", latest.minusSeconds(7), "CONFIRMED"),
                record("orders", "192.0.2.10", "checkout", latest, "CONFIRMED"),
                record("orders", "192.0.2.10", "batch", latest, "CONFIRMED"),
                record("orders", "192.0.2.11", "socket", latest, "PARTIAL_EVIDENCE"),
                record("payments", "2001:db8::1", "billing", latest, "CONFIRMED"));
        write("test-kafka-id", Instant.now(), 1, rows);

        var response = service().get(id, "orders");
        assertThat(response.status()).isEqualTo("PARTIAL");
        assertThat(response.brokersAvailable()).isEqualTo(1);
        assertThat(response.producers()).extracting(TopicProducerDiscoveryService.Producer::clientId)
                .containsExactly("batch", "checkout");
        assertThat(response.producers()).allMatch(producer -> producer.lastSeen().equals(latest));
    }

    @Test
    void distinguishesMissingMismatchedStaleAndEmptySnapshots() throws Exception {
        var service = service();
        assertThat(service.get(id, "orders").status()).isEqualTo("UNAVAILABLE");
        write("wrong-cluster", Instant.now(), 2, "");
        assertThat(service.get(id, "orders").status()).isEqualTo("UNAVAILABLE");
        write("test-kafka-id", Instant.now().minusSeconds(1801), 2, "");
        assertThat(service.get(id, "orders").status()).isEqualTo("STALE");
        write("test-kafka-id", Instant.now(), 2, "");
        assertThat(service.get(id, "orders").status()).isEqualTo("AVAILABLE");
        assertThat(service.get(id, "orders").producers()).isEmpty();
    }

    private TopicProducerDiscoveryService service() {
        var external = mock(ExternalClusterRepository.class);
        var cluster = new ExternalCluster();
        cluster.setKafkaClusterId("test-kafka-id");
        when(external.findById(id)).thenReturn(Optional.of(cluster));
        return new TopicProducerDiscoveryService(directory.toString(), 1800, mock(ClusterRepository.class),
                external, new ObjectMapper());
    }

    private void write(String kafkaId, Instant captured, int available, String rows) throws Exception {
        Files.writeString(directory.resolve(id + ".json"), """
                {"schema_version":1,"cluster_id":"%s","kafka_cluster_id":"%s","snapshot_timestamp":"%s",
                 "telemetry_coverage":{"brokers_total":2,"brokers_available":%s},"producers":[%s]}
                """.formatted(id, kafkaId, captured, available, rows));
    }

    private static String record(String topic, String ip, String client, Instant seen, String confidence) {
        return """
                {"topic":"%s","client_ip":"%s","client_id":"%s","last_seen":"%s","mapping_confidence":"%s",
                 "client_type":"PRODUCER","evidence_source":"broker_request_telemetry"}
                """.formatted(topic, ip, client, seen, confidence);
    }
}
