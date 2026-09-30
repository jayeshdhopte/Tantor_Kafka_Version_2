package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.DiscoveryAgent;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.ExternalClusterNode;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.dto.BrokerSummaryDto;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.DiscoveryAgentRepository;
import io.translab.tantor.server.repository.ExternalClusterNodeRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.ActivityAlertService;
import io.translab.tantor.server.service.BrokerMetricsCacheService;
import io.translab.tantor.server.service.ClusterOverviewService;
import io.translab.tantor.server.service.DeploymentService;
import io.translab.tantor.server.service.ExternalClusterService;
import io.translab.tantor.server.service.HostStatusService;
import io.translab.tantor.server.service.JobService;
import io.translab.tantor.server.service.KafkaAdminService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterControllerHostReleaseTest {

    @Mock DeploymentService deploymentService;
    @Mock ClusterRepository clusterRepository;
    @Mock ExternalClusterRepository externalClusterRepository;
    @Mock ExternalClusterNodeRepository externalClusterNodeRepository;
    @Mock TaskRepository taskRepository;
    @Mock HostRepository hostRepository;
    @Mock HostParcelRepository hostParcelRepository;
    @Mock BrokerMetricsCacheService brokerMetricsCacheService;
    @Mock ClusterOverviewService clusterOverviewService;
    @Mock ObjectMapper objectMapper;
    @Mock ActivityAlertService activityAlertService;
    @Mock HostStatusService hostStatusService;
    @Mock ExternalClusterService externalClusterService;
    @Mock KafkaAdminService kafkaAdminService;
    @Mock JobService jobService;
    @Mock AuditService auditService;
    @Mock DiscoveryAgentRepository discoveryAgentRepository;
    @Mock RoleAuthenticationUtil roleAuthenticationUtil;

    @InjectMocks
    ClusterController controller;

    @Test
    void deletingClusterReleasesOccupiedHost() {
        UUID clusterId = UUID.randomUUID();
        String hostId = "agent-vm-229";

        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setMode("INTERNAL");
        cluster.setKafkaVersion("3.9.2");
        cluster.setConfigJson("{}");

        ClusterServiceAssignment assignment = new ClusterServiceAssignment();
        assignment.setCluster(cluster);
        assignment.setHostId(hostId);
        assignment.setRole("broker_controller");
        assignment.setNodeId(1);
        cluster.setServices(List.of(assignment));

        Host host = new Host();
        host.setId(hostId);
        host.setClusterId(clusterId);
        host.setStatus("OCCUPIED");

        when(roleAuthenticationUtil.canAccess(any(), anyString())).thenReturn(true);
        when(externalClusterRepository.findById(clusterId)).thenReturn(Optional.empty());
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(hostRepository.findById(hostId)).thenReturn(Optional.of(host));
        when(deploymentService.deleteClusterFromHost(
                clusterId, hostId, cluster.getKafkaVersion(), cluster.getConfigJson()))
                .thenReturn(UUID.randomUUID());

        controller.deleteCluster("Bearer test-token", clusterId);

        assertThat(host.getClusterId()).isNull();
        assertThat(host.getStatus()).isEqualTo("ONLINE");
        verify(hostRepository).save(host);
        verify(clusterRepository).purgeById(clusterId);
    }

    @Test
    void externalOverviewOnlyShowsPathsForNodesWithFreshMatchingAgents() {
        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setName("external-test");
        cluster.setBootstrapServers("node-1:9092,node-2:9092");
        cluster.setInstallPath("/opt/kafka");

        ExternalClusterNode managed = new ExternalClusterNode();
        managed.setClusterId(clusterId);
        managed.setNodeId(1);
        managed.setHost("node-1");
        managed.setIsBroker(true);
        managed.setIsController(true);
        managed.setInstallDir("/srv/kafka");
        managed.setConfigFile("/srv/kafka/config/server.properties");

        ExternalClusterNode bootstrapOnly = new ExternalClusterNode();
        bootstrapOnly.setClusterId(clusterId);
        bootstrapOnly.setNodeId(2);
        bootstrapOnly.setHost("node-2");
        bootstrapOnly.setIsBroker(true);

        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setId("agent-1");
        agent.setHostname("node-1");
        agent.setClusterId(clusterId);
        agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());

        when(clusterRepository.findById(clusterId)).thenReturn(Optional.empty());
        when(externalClusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(externalClusterNodeRepository.findByClusterId(clusterId))
                .thenReturn(List.of(managed, bootstrapOnly));
        when(discoveryAgentRepository.findByClusterId(clusterId)).thenReturn(List.of(agent));
        when(discoveryAgentRepository.findAll()).thenReturn(List.of(agent));

        var body = controller.getClusterOverview(clusterId).getBody();

        assertThat(body).isNotNull();
        assertThat(body.getUptime().getConfiguredControllerCount()).isEqualTo(1);
        assertThat(body.getNodePaths()).hasSize(2);
        assertThat(body.getNodePaths().get(0).isHasTelemetry()).isTrue();
        assertThat(body.getNodePaths().get(0).getInstallDir()).isEqualTo("/srv/kafka");
        assertThat(body.getNodePaths().get(1).isHasTelemetry()).isFalse();
        assertThat(body.getNodePaths().get(1).getInstallDir()).isNull();
        assertThat(body.getNodePaths().get(1).getConfig()).isNull();
    }

    @Test
    void externalOverviewKeepsKafkaDataSeparateAndRequiresFreshAgentForHostDiskTelemetry() {
        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setName("external-test");

        ExternalClusterNode node = new ExternalClusterNode();
        node.setClusterId(clusterId);
        node.setNodeId(2);
        node.setHost("node-1");
        node.setIsBroker(true);
        node.setDiskUsedGb(7L);
        node.setDiskTotalGb(47L);
        node.setLastSeen(OffsetDateTime.now());

        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setId("agent-1");
        agent.setHostname("node-1");
        agent.setClusterId(clusterId);
        agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());

        var liveBroker = io.translab.tantor.server.dto.ClusterOverviewDto.BrokerRow.builder()
                .brokerId(2)
                .diskUsageBytes(1234L)
                .diskTotalBytes(0L)
                .build();
        var liveOverview = io.translab.tantor.server.dto.ClusterOverviewDto.builder()
                .brokers(List.of(liveBroker))
                .uptime(io.translab.tantor.server.dto.ClusterOverviewDto.UptimeSummary.builder().build())
                .build();

        when(clusterRepository.findById(clusterId)).thenReturn(Optional.empty());
        when(externalClusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(externalClusterNodeRepository.findByClusterId(clusterId)).thenReturn(List.of(node));
        when(discoveryAgentRepository.findByClusterId(clusterId)).thenReturn(List.of(agent));
        when(discoveryAgentRepository.findAll()).thenReturn(List.of(agent));
        when(clusterOverviewService.getOverview(clusterId)).thenReturn(liveOverview);

        var body = controller.getClusterOverview(clusterId).getBody();

        assertThat(body).isNotNull();
        assertThat(body.getBrokers().get(0).getDiskUsageBytes()).isEqualTo(1234L);
        assertThat(body.getBrokers().get(0).getHostDiskUsedBytes()).isEqualTo(7L * 1024 * 1024 * 1024);
        assertThat(body.getBrokers().get(0).getHostDiskTotalBytes()).isEqualTo(47L * 1024 * 1024 * 1024);
        assertThat(body.getBrokers().get(0).getHostDiskMetricStatus()).isEqualTo("LIVE");
    }

    @Test
    void externalOverviewHidesStaleHostDiskTelemetry() {
        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setName("external-test");

        ExternalClusterNode node = new ExternalClusterNode();
        node.setClusterId(clusterId);
        node.setNodeId(3);
        node.setHost("node-3");
        node.setIsBroker(true);
        node.setDiskUsedBytes(9_126_805_504L);
        node.setDiskTotalBytes(51_539_607_552L);
        node.setLastSeen(OffsetDateTime.now().minusMinutes(5));

        var liveBroker = io.translab.tantor.server.dto.ClusterOverviewDto.BrokerRow.builder()
                .brokerId(3).diskUsageBytes(999L).build();
        var liveOverview = io.translab.tantor.server.dto.ClusterOverviewDto.builder()
                .brokers(List.of(liveBroker))
                .uptime(io.translab.tantor.server.dto.ClusterOverviewDto.UptimeSummary.builder().build())
                .build();

        when(clusterRepository.findById(clusterId)).thenReturn(Optional.empty());
        when(externalClusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(externalClusterNodeRepository.findByClusterId(clusterId)).thenReturn(List.of(node));
        when(discoveryAgentRepository.findByClusterId(clusterId)).thenReturn(List.of());
        when(discoveryAgentRepository.findAll()).thenReturn(List.of());
        when(clusterOverviewService.getOverview(clusterId)).thenReturn(liveOverview);

        var broker = controller.getClusterOverview(clusterId).getBody().getBrokers().get(0);
        assertThat(broker.getDiskUsageBytes()).isEqualTo(999L);
        assertThat(broker.getHostDiskUsedBytes()).isNull();
        assertThat(broker.getHostDiskTotalBytes()).isNull();
        assertThat(broker.getHostDiskMetricStatus()).isEqualTo("STALE");
    }

    @Test
    void controllerOnlyJmxStatusDoesNotFailBrokerRuntimeHealth() {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setName("separate-roles");
        cluster.setMode("kraft");
        cluster.setStatus("SUCCESS");
        cluster.setKafkaClusterId("kafka-cluster-id");
        cluster.setConfigJson("{}");

        ClusterServiceAssignment broker = new ClusterServiceAssignment();
        broker.setCluster(cluster);
        broker.setHostId("broker-host");
        broker.setRole("broker");
        broker.setNodeId(1);
        ClusterServiceAssignment controllerOnly = new ClusterServiceAssignment();
        controllerOnly.setCluster(cluster);
        controllerOnly.setHostId("controller-host");
        controllerOnly.setRole("controller");
        controllerOnly.setNodeId(101);
        cluster.setServices(List.of(broker, controllerOnly));

        Host brokerHost = host("broker-host", "broker");
        Host controllerHost = host("controller-host", "controller");
        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of(cluster));
        when(externalClusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(discoveryAgentRepository.findAll()).thenReturn(List.of());
        when(hostRepository.findById("broker-host")).thenReturn(Optional.of(brokerHost));
        when(hostRepository.findById("controller-host")).thenReturn(Optional.of(controllerHost));
        when(hostStatusService.effectiveStatus(any(Host.class))).thenReturn("OCCUPIED");
        when(brokerMetricsCacheService.getBrokerSummaries(cluster)).thenReturn(List.of(
                BrokerSummaryDto.builder()
                        .brokerId(1).role("broker").brokerHealth("HEALTHY").build(),
                BrokerSummaryDto.builder()
                        .brokerId(101).role("controller").brokerHealth("DEGRADED").build()
        ));

        Map<String, Object> result = controller.listClusters().get(0);

        assertThat(result.get("runtimeHealth")).isEqualTo("HEALTHY");
        assertThat(result.get("runtimeStatusLabel")).isEqualTo("Active");
        assertThat(result.get("runtimeBrokerCount")).isEqualTo(1L);
        assertThat(result.get("runtimeDegradedBrokers")).isEqualTo(0L);
    }

    private static Host host(String id, String hostname) {
        Host host = new Host();
        host.setId(id);
        host.setHostname(hostname);
        host.setStatus("OCCUPIED");
        host.setHostIp("192.0.2.1");
        return host;
    }
}
