package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.HostRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BrokerMetricsCacheServiceTest {

    @Test
    void includesControllerOnlyNodesAndDoesNotInventDiskCapacity() {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(clusterId);

        ClusterServiceAssignment broker = assignment(cluster, "broker-host", "broker", 1);
        ClusterServiceAssignment controller = assignment(cluster, "controller-host", "controller", 2);
        cluster.setServices(List.of(broker, controller));

        HostRepository hosts = mock(HostRepository.class);
        HostStatusService status = mock(HostStatusService.class);
        Host brokerHost = host("broker-host");
        Host controllerHost = host("controller-host");
        when(hosts.findById("broker-host")).thenReturn(Optional.of(brokerHost));
        when(hosts.findById("controller-host")).thenReturn(Optional.of(controllerHost));
        when(status.agentConnectivityStatus(brokerHost)).thenReturn("ONLINE");
        when(status.agentConnectivityStatus(controllerHost)).thenReturn("ONLINE");

        BrokerMetricsCacheService service = new BrokerMetricsCacheService(
                hosts, mock(KafkaAdminService.class), mock(ExternalClusterService.class),
                status, new ObjectMapper(), monitoringProperties());

        var rows = service.getBrokerSummaries(cluster);

        assertThat(rows).hasSize(2);
        assertThat(rows).anySatisfy(row -> {
            assertThat(row.getRole()).isEqualTo("controller");
            assertThat(row.isController()).isTrue();
            assertThat(row.getDiskUsedGb()).isNull();
            assertThat(row.getDiskTotalGb()).isNull();
        });
    }

    @Test
    void exposesPersistedExternalIngestionRates() {
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(UUID.randomUUID());

        ExternalClusterService.ExternalBrokerRecord record =
                new ExternalClusterService.ExternalBrokerRecord();
        record.setNodeId(3);
        record.setHostname("192.168.3.191");
        record.setRole("broker");
        record.setRunning(true);
        record.setMessagesInPerSec(1.67);
        record.setBytesInPerSec(84.81);

        ExternalClusterService externalClusters = mock(ExternalClusterService.class);
        when(externalClusters.brokerRecords(cluster)).thenReturn(List.of(record));
        BrokerMetricsCacheService service = new BrokerMetricsCacheService(
                mock(HostRepository.class), mock(KafkaAdminService.class), externalClusters,
                mock(HostStatusService.class), new ObjectMapper(), monitoringProperties());

        var rows = service.getBrokerSummaries(cluster);

        assertThat(rows).singleElement().satisfies(row -> {
            assertThat(row.getMessagesInPerSec()).isEqualTo(1.67);
            assertThat(row.getBytesInPerSec()).isEqualTo(84.81);
        });
    }

    private static io.translab.tantor.server.config.MonitoringProperties monitoringProperties() {
        var properties = new io.translab.tantor.server.config.MonitoringProperties();
        properties.setMode("direct");
        properties.setPrometheusUrl(java.net.URI.create("http://prometheus:9090"));
        return properties;
    }

    private static ClusterServiceAssignment assignment(Cluster cluster, String hostId, String role, int nodeId) {
        ClusterServiceAssignment assignment = new ClusterServiceAssignment();
        assignment.setCluster(cluster);
        assignment.setHostId(hostId);
        assignment.setRole(role);
        assignment.setNodeId(nodeId);
        return assignment;
    }

    private static Host host(String id) {
        Host host = new Host();
        host.setId(id);
        host.setHostname(id);
        return host;
    }
}
