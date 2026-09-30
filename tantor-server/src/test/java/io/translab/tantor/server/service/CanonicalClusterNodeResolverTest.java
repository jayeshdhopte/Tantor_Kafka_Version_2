package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.DiscoveryAgent;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.ExternalClusterNode;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.canonical.CanonicalAgentStatus;
import io.translab.tantor.server.domain.canonical.CanonicalClusterNodesResponse;
import io.translab.tantor.server.domain.canonical.CanonicalClusterType;
import io.translab.tantor.server.domain.canonical.CanonicalKafkaMode;
import io.translab.tantor.server.domain.canonical.CanonicalNodeRole;
import io.translab.tantor.server.domain.canonical.CanonicalTelemetryStatus;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import io.translab.tantor.server.repository.DiscoveryAgentRepository;
import io.translab.tantor.server.repository.ExternalClusterNodeRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CanonicalClusterNodeResolverTest {

    @Mock ClusterRepository clusterRepository;
    @Mock ClusterServiceAssignmentRepository serviceAssignmentRepository;
    @Mock HostRepository hostRepository;
    @Mock ExternalClusterRepository externalClusterRepository;
    @Mock ExternalClusterNodeRepository externalNodeRepository;
    @Mock DiscoveryAgentRepository discoveryAgentRepository;
    @Mock HostStatusService hostStatusService;

    CanonicalClusterNodeResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new CanonicalClusterNodeResolver(
                clusterRepository,
                serviceAssignmentRepository,
                hostRepository,
                externalClusterRepository,
                externalNodeRepository,
                discoveryAgentRepository,
                hostStatusService);
        ReflectionTestUtils.setField(resolver, "discoveryAgentHeartbeatTimeoutSeconds", 45L);
    }

    @Test
    void resolvesInternalNodesFromAssignmentsAndBoundHostsOnly() {
        UUID clusterUuid = UUID.randomUUID();
        Cluster cluster = cluster(clusterUuid, "internal-kafka-id", "INTERNAL", "KRAFT");
        ClusterServiceAssignment broker = assignment(1, "broker", "agent-host-1");
        ClusterServiceAssignment schemaRegistry = assignment(null, "schema_registry", "agent-host-1");
        Host host = host("agent-host-1", "broker-1", "192.168.10.21", OffsetDateTime.now());

        when(clusterRepository.findByCanonicalClusterUuidAndStatusNot(clusterUuid, "DELETED"))
                .thenReturn(Optional.of(cluster));
        when(serviceAssignmentRepository.findByClusterId(cluster.getId()))
                .thenReturn(List.of(broker, schemaRegistry));
        when(hostRepository.findById("agent-host-1")).thenReturn(Optional.of(host));
        when(hostStatusService.agentConnectivityStatus(host)).thenReturn("ONLINE");

        CanonicalClusterNodesResponse response = resolver.resolve(clusterUuid);

        assertThat(response.cluster().clusterUuid()).isEqualTo(clusterUuid);
        assertThat(response.cluster().type()).isEqualTo(CanonicalClusterType.INTERNAL);
        assertThat(response.cluster().mode()).isEqualTo(CanonicalKafkaMode.KRAFT);
        assertThat(response.nodes()).singleElement().satisfies(node -> {
            assertThat(node.identity().clusterUuid()).isEqualTo(clusterUuid);
            assertThat(node.identity().kafkaClusterId()).isEqualTo("internal-kafka-id");
            assertThat(node.identity().nodeId()).isEqualTo(1);
            assertThat(node.identity().role()).isEqualTo(CanonicalNodeRole.BROKER);
            assertThat(node.host()).isEqualTo("broker-1");
            assertThat(node.hostname()).isEqualTo("broker-1");
            assertThat(node.ipAddress()).isEqualTo("192.168.10.21");
            assertThat(node.agentStatus()).isEqualTo(CanonicalAgentStatus.ONLINE);
            assertThat(node.telemetryStatus()).isEqualTo(CanonicalTelemetryStatus.LIVE);
        });
        verify(externalNodeRepository, never()).findByCanonicalClusterUuid(clusterUuid);
        verify(discoveryAgentRepository, never()).findByClusterId(clusterUuid);
    }

    @Test
    void resolvesExternalNodesAndModeWithoutMatchingHostOrIpToAnAgent() {
        UUID clusterUuid = UUID.randomUUID();
        Cluster cluster = cluster(clusterUuid, "external-kafka-id", "EXTERNAL", "EXTERNAL");
        ExternalCluster externalCluster = new ExternalCluster();
        externalCluster.setId(clusterUuid);
        externalCluster.setKafkaMode("ZooKeeper");
        ExternalClusterNode broker = externalNode(clusterUuid, 2, true, false, "192.168.20.22");
        ExternalClusterNode activeBrokerController = externalNode(clusterUuid, 1, true, true, "192.168.20.23");
        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setClusterId(clusterUuid);
        agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());
        agent.setHostname("broker-2.example.test");
        agent.setIpAddresses("[\"192.168.20.22\"]");

        when(clusterRepository.findByCanonicalClusterUuidAndStatusNot(clusterUuid, "DELETED"))
                .thenReturn(Optional.of(cluster));
        when(externalClusterRepository.findById(cluster.getId())).thenReturn(Optional.of(externalCluster));
        when(discoveryAgentRepository.findByClusterId(clusterUuid)).thenReturn(List.of(agent));
        when(externalNodeRepository.findByCanonicalClusterUuid(clusterUuid))
                .thenReturn(List.of(activeBrokerController, broker));

        CanonicalClusterNodesResponse response = resolver.resolve(clusterUuid);

        assertThat(response.cluster().type()).isEqualTo(CanonicalClusterType.EXTERNAL);
        assertThat(response.cluster().mode()).isEqualTo(CanonicalKafkaMode.ZOOKEEPER);
        assertThat(response.nodes()).extracting(node -> node.identity().nodeId())
                .containsExactly(1, 2);
        assertThat(response.nodes()).extracting(node -> node.identity().role())
                .containsExactly(CanonicalNodeRole.BROKER, CanonicalNodeRole.BROKER);
        assertThat(response.nodes()).allSatisfy(node -> {
            assertThat(node.agentStatus()).isEqualTo(CanonicalAgentStatus.ONLINE);
            assertThat(node.telemetryStatus()).isEqualTo(CanonicalTelemetryStatus.LIVE);
        });
        assertThat(response.nodes().get(1).hostname()).isEqualTo("broker-2.example.test");
        assertThat(response.nodes().get(1).ipAddress()).isEqualTo("192.168.20.22");
        verify(hostRepository, never()).findById("192.168.20.22");
        verify(hostRepository, never()).findById("192.168.20.23");
        verify(serviceAssignmentRepository, never()).findByClusterId(cluster.getId());
    }

    @Test
    void failsClosedWhenKafkaClusterIdIsMissing() {
        UUID clusterUuid = UUID.randomUUID();
        Cluster cluster = cluster(clusterUuid, null, "INTERNAL", "KRAFT");
        when(clusterRepository.findByCanonicalClusterUuidAndStatusNot(clusterUuid, "DELETED"))
                .thenReturn(Optional.of(cluster));

        assertThatThrownBy(() -> resolver.resolve(clusterUuid))
                .isInstanceOf(CanonicalIdentityException.class)
                .hasMessageContaining("Kafka cluster id is not available");

        verify(serviceAssignmentRepository, never()).findByClusterId(cluster.getId());
        verify(externalNodeRepository, never()).findByCanonicalClusterUuid(clusterUuid);
    }

    @Test
    void doesNotResolveDeletedOrUnknownCanonicalCluster() {
        UUID clusterUuid = UUID.randomUUID();
        when(clusterRepository.findByCanonicalClusterUuidAndStatusNot(clusterUuid, "DELETED"))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> resolver.resolve(clusterUuid))
                .isInstanceOf(CanonicalClusterNotFoundException.class)
                .hasMessageContaining(clusterUuid.toString());
    }

    private Cluster cluster(UUID canonicalUuid, String kafkaClusterId, String type, String mode) {
        Cluster cluster = new Cluster();
        cluster.setId(canonicalUuid);
        cluster.setCanonicalClusterUuid(canonicalUuid);
        cluster.setKafkaClusterId(kafkaClusterId);
        cluster.setOriginType(type);
        cluster.setMode(mode);
        return cluster;
    }

    private ClusterServiceAssignment assignment(Integer nodeId, String role, String hostId) {
        ClusterServiceAssignment assignment = new ClusterServiceAssignment();
        assignment.setId(UUID.randomUUID());
        assignment.setNodeId(nodeId);
        assignment.setRole(role);
        assignment.setHostId(hostId);
        return assignment;
    }

    private Host host(String id, String hostname, String ip, OffsetDateTime heartbeat) {
        Host host = new Host();
        host.setId(id);
        host.setHostname(hostname);
        host.setHostIp(ip);
        host.setLastHeartbeat(heartbeat);
        return host;
    }

    private ExternalClusterNode externalNode(
            UUID clusterUuid,
            int nodeId,
            boolean broker,
            boolean controller,
            String host) {
        ExternalClusterNode node = new ExternalClusterNode();
        node.setId(UUID.randomUUID());
        node.setClusterId(clusterUuid);
        node.setCanonicalClusterUuid(clusterUuid);
        node.setNodeId(nodeId);
        node.setIsBroker(broker);
        node.setIsController(controller);
        node.setHost(host);
        node.setLastSeen(OffsetDateTime.now());
        return node;
    }
}
