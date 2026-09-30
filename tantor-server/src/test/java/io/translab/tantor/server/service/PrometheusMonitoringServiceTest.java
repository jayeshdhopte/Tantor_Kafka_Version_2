package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.ExternalClusterNode;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterNodeRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.config.MonitoringHttpClientConfiguration.MonitoringRestTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PrometheusMonitoringServiceTest {

    private static io.translab.tantor.server.config.MonitoringProperties monitoringProperties() {
        var properties = new io.translab.tantor.server.config.MonitoringProperties();
        properties.setMode("direct");
        properties.setPrometheusUrl(java.net.URI.create("http://prometheus:9090"));
        return properties;
    }

    @Test
    void createsSeparateBrokerAndControllerJmxTargetsWithoutDuplicatingKafkaExporter() {
        UUID id = UUID.randomUUID();
        Cluster mirror = new Cluster();
        mirror.setId(id);
        mirror.setName("external-prod");
        mirror.setOriginType("EXTERNAL");
        mirror.setMode("EXTERNAL");
        mirror.setMonitoringEnabled(true);
        mirror.setJmxEnabled(true);

        ExternalCluster external = new ExternalCluster();
        external.setId(id);
        external.setName("external-prod");

        ExternalClusterNode broker = externalNode(id, 1, "192.168.10.11", true, false);
        ExternalClusterNode controller = externalNode(id, 101, "192.168.10.11", false, true);
        broker.setJmxExporterPort(17071);
        controller.setJmxExporterPort(17072);

        ClusterRepository clusters = mock(ClusterRepository.class);
        ExternalClusterRepository externalClusters = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodes = mock(ExternalClusterNodeRepository.class);
        when(clusters.findByStatusNot("DELETED")).thenReturn(List.of(mirror));
        when(externalClusters.findByStatusNot("DELETED")).thenReturn(List.of(external));
        when(nodes.findByClusterId(id)).thenReturn(List.of(broker, controller));

        PrometheusMonitoringService service = new PrometheusMonitoringService(
                clusters, externalClusters, nodes, mock(HostRepository.class),
                mock(EncryptionService.class), new ObjectMapper(), mock(MonitoringRestTemplate.class), monitoringProperties());
        ReflectionTestUtils.setField(service, "kafkaExporterPortBase", 9308);
        ReflectionTestUtils.setField(service, "defaultJmxExporterPort", 7071);
        ReflectionTestUtils.setField(service, "defaultControllerJmxExporterPort", 7072);

        assertThat(service.prometheusTargets())
                .extracting(
                        target -> target.getTargets().getFirst(),
                        target -> target.getLabels().get("job"),
                        target -> target.getLabels().get("node_id"),
                        target -> target.getLabels().get("role"))
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("192.168.10.11:17071", "kafka_jmx", "1", "broker"),
                        org.assertj.core.groups.Tuple.tuple("192.168.10.11:17072", "kafka_jmx", "101", "controller"),
                        org.assertj.core.groups.Tuple.tuple("192.168.10.11:9308", "kafka_exporter", "1", "broker"));
    }

    @Test
    void createsOneJmxTargetForCombinedBrokerControllerJvm() {
        UUID id = UUID.randomUUID();
        Cluster mirror = new Cluster();
        mirror.setId(id);
        mirror.setName("combined");
        mirror.setOriginType("EXTERNAL");
        mirror.setMode("EXTERNAL");
        mirror.setMonitoringEnabled(true);
        mirror.setJmxEnabled(true);

        ExternalClusterNode combined = externalNode(id, 1, "192.168.10.21", true, true);
        ClusterRepository clusters = mock(ClusterRepository.class);
        ExternalClusterNodeRepository nodes = mock(ExternalClusterNodeRepository.class);
        when(clusters.findByStatusNot("DELETED")).thenReturn(List.of(mirror));
        when(nodes.findByClusterId(id)).thenReturn(List.of(combined));

        PrometheusMonitoringService service = new PrometheusMonitoringService(
                clusters, mock(ExternalClusterRepository.class), nodes, mock(HostRepository.class),
                mock(EncryptionService.class), new ObjectMapper(), mock(MonitoringRestTemplate.class), monitoringProperties());
        ReflectionTestUtils.setField(service, "kafkaExporterPortBase", 9308);
        ReflectionTestUtils.setField(service, "defaultJmxExporterPort", 7071);
        ReflectionTestUtils.setField(service, "defaultControllerJmxExporterPort", 7072);

        assertThat(service.prometheusTargets())
                .extracting(
                        target -> target.getTargets().getFirst(),
                        target -> target.getLabels().get("job"),
                        target -> target.getLabels().get("role"))
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("192.168.10.21:7071", "kafka_jmx", "broker_controller"),
                        org.assertj.core.groups.Tuple.tuple("192.168.10.21:9308", "kafka_exporter", "broker_controller"));
    }

    @Test
    void returnsMirroredExternalClusterOnlyOnce() {
        UUID id = UUID.randomUUID();
        Cluster mirror = new Cluster();
        mirror.setId(id);
        mirror.setName("external-prod");
        mirror.setOriginType("EXTERNAL");
        mirror.setMode("EXTERNAL");
        mirror.setServices(List.of());

        ExternalCluster external = new ExternalCluster();
        external.setId(id);
        external.setName("external-prod");

        ExternalClusterNode broker = new ExternalClusterNode();
        broker.setClusterId(id);
        broker.setNodeId(1);
        broker.setHost("192.168.10.11");
        broker.setIsBroker(true);

        ExternalClusterNode controller = new ExternalClusterNode();
        controller.setClusterId(id);
        controller.setNodeId(101);
        controller.setHost("192.168.10.11");
        controller.setIsController(true);

        ClusterRepository clusters = mock(ClusterRepository.class);
        ExternalClusterRepository externalClusters = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodes = mock(ExternalClusterNodeRepository.class);
        when(clusters.findByStatusNot("DELETED")).thenReturn(List.of(mirror));
        when(externalClusters.findByStatusNot("DELETED")).thenReturn(List.of(external));
        when(nodes.findByClusterId(id)).thenReturn(List.of(broker, controller));

        PrometheusMonitoringService service = new PrometheusMonitoringService(
                clusters, externalClusters, nodes, mock(HostRepository.class),
                mock(EncryptionService.class), new ObjectMapper(), mock(MonitoringRestTemplate.class), monitoringProperties());

        PrometheusMonitoringService.MonitoringClusterSummary summary = service.clusters("EXTERNAL").getFirst();

        assertThat(summary.getId()).isEqualTo(id);
        assertThat(summary.getNodes())
                .extracting(
                        PrometheusMonitoringService.MonitoringNodeSummary::getNodeId,
                        PrometheusMonitoringService.MonitoringNodeSummary::getHostIp,
                        PrometheusMonitoringService.MonitoringNodeSummary::getRole)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("1", "192.168.10.11", "Broker"),
                        org.assertj.core.groups.Tuple.tuple("101", "192.168.10.11", "Controller"));
    }

    @Test
    void computesExternalHostMemoryFromDiscoveredNodesWithoutInternalAssignments() {
        UUID id = UUID.randomUUID();
        Cluster mirror = new Cluster();
        mirror.setId(id);
        mirror.setOriginType("EXTERNAL");
        mirror.setMode("EXTERNAL");
        mirror.setServices(List.of());

        ExternalClusterNode first = new ExternalClusterNode();
        first.setClusterId(id);
        first.setNodeId(1);
        first.setHost("host-a");
        first.setCpuUsagePct(10.0);
        first.setMemoryUsedMb(200L);
        first.setMemoryTotalMb(1000L);
        ExternalClusterNode second = new ExternalClusterNode();
        second.setClusterId(id);
        second.setNodeId(2);
        second.setHost("host-b");
        second.setCpuUsagePct(30.0);
        second.setMemoryUsedMb(300L);
        second.setMemoryTotalMb(1000L);
        ExternalClusterNode controllerOnFirstHost = externalNode(id, 101, "host-a", false, true);
        controllerOnFirstHost.setCpuUsagePct(10.0);
        controllerOnFirstHost.setMemoryUsedMb(200L);
        controllerOnFirstHost.setMemoryTotalMb(1000L);

        ExternalClusterNodeRepository nodes = mock(ExternalClusterNodeRepository.class);
        when(nodes.findByClusterId(id)).thenReturn(List.of(first, controllerOnFirstHost, second));
        PrometheusMonitoringService service = new PrometheusMonitoringService(
                mock(ClusterRepository.class), mock(ExternalClusterRepository.class), nodes,
                mock(HostRepository.class), mock(EncryptionService.class), new ObjectMapper(), mock(MonitoringRestTemplate.class), monitoringProperties());

        assertThat(service.computeHostMemoryPercent(mirror, null)).isEqualTo(25.0);
        assertThat(service.computeHostMemoryPercent(mirror, "2")).isEqualTo(30.0);
        assertThat(service.computeHostMemoryAvailableMb(mirror, null)).isEqualTo(1500L);
        assertThat(service.computeHostMemoryAvailableMb(mirror, "2")).isEqualTo(700L);
        assertThat(service.computeHostMemoryTotalMb(mirror, null)).isEqualTo(2000L);
        assertThat(service.computeHostMemoryTotalMb(mirror, "2")).isEqualTo(1000L);
        assertThat(service.computeExternalSystemCpuPercent(mirror, null)).isEqualTo(20.0);
        assertThat(service.computeExternalSystemCpuPercent(mirror, "2")).isEqualTo(30.0);
    }

    @Test
    void computesInternalAvailableMemoryFromAgentHeartbeatValues() {
        Cluster cluster = new Cluster();
        cluster.setOriginType("INTERNAL");
        cluster.setServices(List.of(
                assignment(cluster, "host-1", 1),
                assignment(cluster, "host-2", 2)
        ));

        Host first = new Host();
        first.setId("host-1");
        first.setMemUsedMb(2048L);
        first.setMemTotalMb(8192L);
        Host second = new Host();
        second.setId("host-2");
        second.setMemUsedMb(4096L);
        second.setMemTotalMb(8192L);

        HostRepository hosts = mock(HostRepository.class);
        when(hosts.findById("host-1")).thenReturn(Optional.of(first));
        when(hosts.findById("host-2")).thenReturn(Optional.of(second));
        PrometheusMonitoringService service = new PrometheusMonitoringService(
                mock(ClusterRepository.class), mock(ExternalClusterRepository.class),
                mock(ExternalClusterNodeRepository.class), hosts,
                mock(EncryptionService.class), new ObjectMapper(), mock(MonitoringRestTemplate.class), monitoringProperties());

        assertThat(service.computeHostMemoryPercent(cluster, null)).isEqualTo(37.5);
        assertThat(service.computeHostMemoryAvailableMb(cluster, null)).isEqualTo(10240L);
        assertThat(service.computeHostMemoryAvailableMb(cluster, "2")).isEqualTo(4096L);
        assertThat(service.computeHostMemoryTotalMb(cluster, null)).isEqualTo(16384L);
        assertThat(service.computeHostMemoryTotalMb(cluster, "2")).isEqualTo(8192L);
    }

    private ClusterServiceAssignment assignment(Cluster cluster, String hostId, int nodeId) {
        ClusterServiceAssignment assignment = new ClusterServiceAssignment();
        assignment.setCluster(cluster);
        assignment.setHostId(hostId);
        assignment.setNodeId(nodeId);
        return assignment;
    }

    private ExternalClusterNode externalNode(
            UUID clusterId,
            int nodeId,
            String host,
            boolean broker,
            boolean controller) {
        ExternalClusterNode node = new ExternalClusterNode();
        node.setClusterId(clusterId);
        node.setNodeId(nodeId);
        node.setHost(host);
        node.setIsBroker(broker);
        node.setIsController(controller);
        return node;
    }
}
