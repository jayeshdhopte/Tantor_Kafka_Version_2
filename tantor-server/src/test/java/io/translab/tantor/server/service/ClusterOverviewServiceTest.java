package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.dto.ClusterOverviewDto;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterOverviewServiceTest {

    @Mock private ClusterRepository clusterRepository;
    @Mock private ExternalClusterRepository externalClusterRepository;
    @Mock private HostRepository hostRepository;
    @Mock private KafkaAdminService kafkaAdminService;
    @Mock private HostStatusService hostStatusService;
    @Mock private AdminClient adminClient;
    @Mock private DescribeClusterResult describeClusterResult;
    @Mock private ListTopicsResult listTopicsResult;
    @Mock private DescribeLogDirsResult describeLogDirsResult;

    private ClusterOverviewService service;

    @BeforeEach
    void setUp() {
        service = new ClusterOverviewService(
                clusterRepository,
                externalClusterRepository,
                hostRepository,
                kafkaAdminService,
                hostStatusService
        );
    }

    @Test
    void reportsLiveDiskMetricsForOccupiedHostWithFreshAgentHeartbeat() throws Exception {
        UUID clusterId = UUID.randomUUID();
        String hostId = "agent-vm-191";

        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setName("internal-test");
        cluster.setKafkaVersion("3.9.2");
        cluster.setOriginType("INTERNAL");
        cluster.setMode("kraft");

        ClusterServiceAssignment brokerAssignment = new ClusterServiceAssignment();
        brokerAssignment.setCluster(cluster);
        brokerAssignment.setHostId(hostId);
        brokerAssignment.setNodeId(1);
        brokerAssignment.setRole("broker");
        cluster.setServices(List.of(brokerAssignment));

        Host host = new Host();
        host.setId(hostId);
        host.setHostname("broker3.translab.io");
        host.setStatus("OCCUPIED");
        host.setLastHeartbeat(OffsetDateTime.now());
        host.setDiskUsedGb(12L);
        host.setDiskTotalGb(16L);

        Node brokerNode = new Node(1, "192.168.3.191", 9092);
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(kafkaAdminService.getAdminClient(clusterId)).thenReturn(adminClient);
        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(brokerNode)));
        when(describeClusterResult.controller()).thenReturn(KafkaFuture.completedFuture(brokerNode));
        when(describeClusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture("cluster-191"));
        when(kafkaAdminService.getControllerId(clusterId)).thenReturn(101);
        when(adminClient.listTopics(any())).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(adminClient.describeLogDirs(any())).thenReturn(describeLogDirsResult);
        when(describeLogDirsResult.descriptions()).thenReturn(
                Map.of(1, KafkaFuture.completedFuture(Map.of()))
        );
        when(hostRepository.findById(hostId)).thenReturn(Optional.of(host));
        when(hostStatusService.agentConnectivityStatus(host)).thenReturn("ONLINE");

        ClusterOverviewDto overview = service.getOverview(clusterId);

        assertThat(overview.getBrokers()).hasSize(1);
        ClusterOverviewDto.BrokerRow broker = overview.getBrokers().getFirst();
        assertThat(broker.getHostDiskMetricStatus()).isEqualTo("LIVE");
        assertThat(broker.getHostDiskUsedBytes()).isEqualTo(12L * 1024 * 1024 * 1024);
        assertThat(broker.getHostDiskTotalBytes()).isEqualTo(16L * 1024 * 1024 * 1024);
        assertThat(broker.getHostDiskLastSeen()).isEqualTo(host.getLastHeartbeat());
        verify(hostStatusService).agentConnectivityStatus(host);
        verify(hostStatusService, never()).isOnline(host);
    }

    @Test
    void reportsZooKeeperModeWithoutCallingKraftMetadataQuorumApi() throws Exception {
        UUID clusterId = UUID.randomUUID();
        Cluster compatibilityCluster = new Cluster();
        compatibilityCluster.setId(clusterId);
        compatibilityCluster.setName("legacy-zk");
        compatibilityCluster.setMode("EXTERNAL");
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setName("legacy-zk");
        cluster.setKafkaVersion("2.5.0");
        cluster.setKafkaMode("ZooKeeper");

        Node brokerController = new Node(1, "192.168.3.150", 9092);
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(compatibilityCluster));
        when(externalClusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(kafkaAdminService.getAdminClient(clusterId)).thenReturn(adminClient);
        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(brokerController)));
        when(describeClusterResult.controller()).thenReturn(KafkaFuture.completedFuture(brokerController));
        when(describeClusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture("zk-cluster-id"));
        when(adminClient.listTopics(any())).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(adminClient.describeLogDirs(any())).thenReturn(describeLogDirsResult);
        when(describeLogDirsResult.descriptions()).thenReturn(
                Map.of(1, KafkaFuture.completedFuture(Map.of()))
        );

        ClusterOverviewDto overview = service.getOverview(clusterId);

        assertThat(overview.getControllerType()).isEqualTo("ZooKeeper");
        assertThat(overview.getUptime().getControllerType()).isEqualTo("ZooKeeper");
        assertThat(overview.getUptime().getActiveControllerId()).isEqualTo(1);
        assertThat(overview.getUptime().getConfiguredControllerCount()).isZero();
        verify(kafkaAdminService, never()).getControllerId(clusterId);
    }

    @Test
    void returnsManagedControllerVotersForInternalKraftCluster() throws Exception {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setName("internal-kraft");
        cluster.setKafkaVersion("4.1.0");
        cluster.setOriginType("INTERNAL");
        cluster.setMode("kraft");
        cluster.setConfigJson("{\"controller_port\":9093}");

        ClusterServiceAssignment controller101 = assignment(cluster, "controller-a", 101, "controller");
        controller101.setConfigJson("{\"controller_port\":19093}");
        ClusterServiceAssignment controller102 = assignment(cluster, "controller-b", 102, "broker_controller");
        cluster.setServices(List.of(controller102, controller101));

        Host firstHost = host("controller-a", "controller-a.local", "192.168.3.229");
        Host secondHost = host("controller-b", "controller-b.local", "192.168.3.164");
        Node brokerNode = new Node(2, "192.168.3.164", 9092);

        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(kafkaAdminService.getAdminClient(clusterId)).thenReturn(adminClient);
        when(adminClient.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(brokerNode)));
        when(describeClusterResult.controller()).thenReturn(KafkaFuture.completedFuture(brokerNode));
        when(describeClusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture("kraft-cluster-id"));
        when(kafkaAdminService.getControllerId(clusterId)).thenReturn(102);
        when(adminClient.listTopics(any())).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(adminClient.describeLogDirs(any())).thenReturn(describeLogDirsResult);
        when(describeLogDirsResult.descriptions()).thenReturn(
                Map.of(2, KafkaFuture.completedFuture(Map.of()))
        );
        when(hostRepository.findById("controller-a")).thenReturn(Optional.of(firstHost));
        when(hostRepository.findById("controller-b")).thenReturn(Optional.of(secondHost));

        ClusterOverviewDto overview = service.getOverview(clusterId);

        assertThat(overview.getControllers())
                .extracting(
                        ClusterOverviewDto.ControllerRow::getNodeId,
                        ClusterOverviewDto.ControllerRow::getHost,
                        ClusterOverviewDto.ControllerRow::getPort,
                        ClusterOverviewDto.ControllerRow::isActiveLeader)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(101, "192.168.3.229", 19093, false),
                        org.assertj.core.groups.Tuple.tuple(102, "192.168.3.164", 9093, true)
                );
        assertThat(overview.getUptime().getConfiguredControllerCount()).isEqualTo(2);
    }

    private ClusterServiceAssignment assignment(
            Cluster cluster,
            String hostId,
            int nodeId,
            String role
    ) {
        ClusterServiceAssignment assignment = new ClusterServiceAssignment();
        assignment.setCluster(cluster);
        assignment.setHostId(hostId);
        assignment.setNodeId(nodeId);
        assignment.setRole(role);
        return assignment;
    }

    private Host host(String id, String hostname, String hostIp) {
        Host host = new Host();
        host.setId(id);
        host.setHostname(hostname);
        host.setHostIp(hostIp);
        return host;
    }
}
