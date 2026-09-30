package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.security.TruststoreStorageService;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaAdminServiceTopologyTest {
    @Test
    void usesExplicitNonDefaultPortForEveryBrokerInMultiHostTopology() {
        UUID id = UUID.randomUUID();
        Cluster cluster = cluster(id, "{\"listener_port\":19094}", "broker-a", "broker-b");
        HostRepository hosts = mock(HostRepository.class);
        when(hosts.findById("broker-a")).thenReturn(Optional.of(host("broker-a", "10.20.0.11")));
        when(hosts.findById("broker-b")).thenReturn(Optional.of(host("broker-b", "10.20.0.12")));

        var properties = service(cluster, hosts).getKafkaClientProperties(id);

        assertThat(properties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG))
                .isEqualTo("10.20.0.11:19094,10.20.0.12:19094");
    }

    @Test
    void missingOrMalformedListenerPortFailsInsteadOfFallingBack() {
        UUID id = UUID.randomUUID();
        HostRepository hosts = mock(HostRepository.class);
        when(hosts.findById("broker-a")).thenReturn(Optional.of(host("broker-a", "10.20.0.11")));

        assertThatThrownBy(() -> service(cluster(id, "{}", "broker-a"), hosts).getKafkaClientProperties(id))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Invalid Kafka bootstrap configuration");
        assertThatThrownBy(() -> service(cluster(id, "{\"listener_port\":\"not-a-port\"}", "broker-a"), hosts)
                .getKafkaClientProperties(id))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Invalid Kafka bootstrap configuration");
    }

    private KafkaAdminService service(Cluster cluster, HostRepository hosts) {
        ClusterRepository clusters = mock(ClusterRepository.class);
        ExternalClusterRepository external = mock(ExternalClusterRepository.class);
        when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));
        when(external.findById(cluster.getId())).thenReturn(Optional.empty());
        return new KafkaAdminService(clusters, external, hosts, new ObjectMapper(),
                mock(EncryptionService.class), mock(TruststoreStorageService.class));
    }

    private Cluster cluster(UUID id, String config, String... hostIds) {
        Cluster cluster = new Cluster();
        cluster.setId(id);
        cluster.setMode("KRAFT");
        cluster.setConfigJson(config);
        cluster.setServices(java.util.Arrays.stream(hostIds).map(hostId -> {
            ClusterServiceAssignment assignment = new ClusterServiceAssignment();
            assignment.setHostId(hostId);
            assignment.setRole("broker");
            return assignment;
        }).toList());
        return cluster;
    }

    private Host host(String id, String address) {
        Host host = new Host();
        host.setId(id);
        host.setHostIp(address);
        return host;
    }
}
