package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.Alert;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.AlertRepository;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.ConsumerLagCacheService;
import io.translab.tantor.server.service.HostStatusService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

class AlertControllerRecoveryTest {

    @Test
    void recoveredRuntimeAlertRemainsInResponseAsResolvedHistory() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);

        Alert recoveredAlert = new Alert();
        recoveredAlert.setAlertKey("host-offline-agent-1");
        recoveredAlert.setSeverity("CRITICAL");
        recoveredAlert.setTitle("Host agent offline");
        recoveredAlert.setStatus("ACTIVE");

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(hostRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE"))
                .thenAnswer(ignored -> "ACTIVE".equals(recoveredAlert.getStatus())
                        ? List.of(recoveredAlert)
                        : List.of());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc())
                .thenReturn(List.of(recoveredAlert));

        AlertController controller = new AlertController(
                alertRepository,
                clusterRepository,
                hostRepository,
                taskRepository,
                mock(HostStatusService.class),
                hostParcelRepository,
                mock(ConsumerLagCacheService.class));

        var response = controller.getActiveAlerts();

        assertThat(response.getBody()).singleElement().satisfies(alert ->
                assertThat(alert.get("status")).isEqualTo("RESOLVED"));
        assertThat(recoveredAlert.getStatus()).isEqualTo("RESOLVED");
        assertThat(recoveredAlert.getResolvedAt()).isNotNull();
        verify(alertRepository).save(recoveredAlert);
    }

    @Test
    void activeExternalHealthAlertIsNotSweptByRuntimeAlertSynchronization() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);

        UUID clusterId = UUID.randomUUID();
        Alert externalHealthAlert = new Alert();
        externalHealthAlert.setAlertKey("external-agent-degraded-" + clusterId);
        externalHealthAlert.setSeverity("WARNING");
        externalHealthAlert.setTitle("External Cluster Degraded");
        externalHealthAlert.setClusterId(clusterId);
        externalHealthAlert.setSource("external_health");
        externalHealthAlert.setStatus("ACTIVE");

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(hostRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE"))
                .thenReturn(List.of(externalHealthAlert));
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc())
                .thenReturn(List.of(externalHealthAlert));

        AlertController controller = new AlertController(
                alertRepository,
                clusterRepository,
                hostRepository,
                taskRepository,
                mock(HostStatusService.class),
                hostParcelRepository,
                mock(ConsumerLagCacheService.class));

        var response = controller.getActiveAlerts();

        assertThat(response.getBody()).hasSize(1);
        assertThat(externalHealthAlert.getStatus()).isEqualTo("ACTIVE");
        verify(alertRepository, never()).save(externalHealthAlert);
    }

    @Test
    void alertResponseUsesClusterNameAndKafkaClusterIdInsteadOfDatabaseUuid() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);

        UUID databaseClusterId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(databaseClusterId);
        cluster.setName("payments-kafka");
        cluster.setKafkaClusterId("MkU3OEVBNTcwNTJENDM2Qk");

        Alert activeAlert = new Alert();
        activeAlert.setAlertKey("external-agent-degraded-" + databaseClusterId);
        activeAlert.setSeverity("WARNING");
        activeAlert.setTitle("External Cluster Degraded");
        activeAlert.setClusterId(databaseClusterId);
        activeAlert.setSource("external_health");
        activeAlert.setStatus("ACTIVE");

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of(cluster));
        when(hostRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE"))
                .thenReturn(List.of(activeAlert));
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc())
                .thenReturn(List.of(activeAlert));

        AlertController controller = new AlertController(
                alertRepository,
                clusterRepository,
                hostRepository,
                taskRepository,
                mock(HostStatusService.class),
                hostParcelRepository,
                mock(ConsumerLagCacheService.class));

        var response = controller.getActiveAlerts();

        assertThat(response.getBody()).singleElement().satisfies(alert -> {
            assertThat(alert.get("clusterName")).isEqualTo("payments-kafka");
            assertThat(alert.get("kafkaClusterId")).isEqualTo("MkU3OEVBNTcwNTJENDM2Qk");
            assertThat(alert.get("clusterId")).isEqualTo(databaseClusterId);
        });
    }

    @Test
    void storedAlertResolvesHostIpAndDoesNotExposeManagementHostId() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);
        HostStatusService hostStatusService = mock(HostStatusService.class);

        Cluster cluster = new Cluster();
        UUID clusterId = UUID.randomUUID();
        cluster.setId(clusterId);
        cluster.setName("payments-kafka");
        cluster.setKafkaClusterId("Kafka-Actual-Cluster-Id");

        Host host = new Host();
        host.setId("host-internal-uuid");
        host.setHostIp("192.168.3.229");

        Alert alert = new Alert();
        alert.setAlertKey("manual-alert");
        alert.setSeverity("WARNING");
        alert.setTitle("Example alert");
        alert.setStatus("ACTIVE");
        alert.setClusterId(clusterId);
        alert.setHostId(host.getId());

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of(cluster));
        when(hostRepository.findAll()).thenReturn(List.of(host));
        when(hostStatusService.isInfrastructureHost(host)).thenReturn(true);
        when(hostStatusService.effectiveStatus(host)).thenReturn("ONLINE");
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE")).thenReturn(List.of());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc()).thenReturn(List.of(alert));

        AlertController controller = new AlertController(
                alertRepository, clusterRepository, hostRepository, taskRepository,
                hostStatusService, hostParcelRepository, mock(ConsumerLagCacheService.class));

        var response = controller.getActiveAlerts();

        assertThat(response.getBody()).singleElement().satisfies(item -> {
            assertThat(item.get("hostIp")).isEqualTo("192.168.3.229");
            assertThat(item.get("kafkaClusterId")).isEqualTo("Kafka-Actual-Cluster-Id");
        });
    }

    @Test
    void deletedClusterMetadataRemainsVisibleAndIsSnapshotted() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);

        UUID clusterId = UUID.randomUUID();
        Cluster deletedCluster = new Cluster();
        deletedCluster.setId(clusterId);
        deletedCluster.setName("deleted-payments");
        deletedCluster.setKafkaClusterId("deleted-kafka-id");
        deletedCluster.setStatus("DELETED");

        Alert alert = new Alert();
        alert.setAlertKey("deleted-cluster-history");
        alert.setSeverity("WARNING");
        alert.setTitle("Historical alert");
        alert.setStatus("RESOLVED");
        alert.setClusterId(clusterId);
        alert.setAffectedIps("192.168.3.213");

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(clusterRepository.findAll()).thenReturn(List.of(deletedCluster));
        when(hostRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE")).thenReturn(List.of());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc()).thenReturn(List.of(alert));

        AlertController controller = new AlertController(
                alertRepository, clusterRepository, hostRepository, taskRepository,
                mock(HostStatusService.class), hostParcelRepository, mock(ConsumerLagCacheService.class));

        var response = controller.getActiveAlerts();

        assertThat(response.getBody()).singleElement().satisfies(item -> {
            assertThat(item.get("clusterName")).isEqualTo("deleted-payments");
            assertThat(item.get("kafkaClusterId")).isEqualTo("deleted-kafka-id");
            assertThat(item.get("hostIp")).isEqualTo("192.168.3.213");
        });
        assertThat(alert.getClusterNameSnapshot()).isEqualTo("deleted-payments");
        assertThat(alert.getKafkaClusterIdSnapshot()).isEqualTo("deleted-kafka-id");
        assertThat(alert.getHostIpSnapshot()).isEqualTo("192.168.3.213");
        verify(alertRepository).save(alert);
    }

    @Test
    void storedSnapshotsRemainVisibleWhenClusterAndHostNoLongerExist() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);

        Alert alert = new Alert();
        alert.setAlertKey("orphaned-alert-history");
        alert.setSeverity("CRITICAL");
        alert.setTitle("Historical alert");
        alert.setStatus("RESOLVED");
        alert.setClusterId(UUID.randomUUID());
        alert.setHostId("removed-host");
        alert.setClusterNameSnapshot("archived-cluster");
        alert.setKafkaClusterIdSnapshot("archived-kafka-id");
        alert.setHostIpSnapshot("192.168.3.229");

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(clusterRepository.findAll()).thenReturn(List.of());
        when(hostRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE")).thenReturn(List.of());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc()).thenReturn(List.of(alert));

        AlertController controller = new AlertController(
                alertRepository, clusterRepository, hostRepository, taskRepository,
                mock(HostStatusService.class), hostParcelRepository, mock(ConsumerLagCacheService.class));

        var response = controller.getActiveAlerts();

        assertThat(response.getBody()).singleElement().satisfies(item -> {
            assertThat(item.get("clusterName")).isEqualTo("archived-cluster");
            assertThat(item.get("kafkaClusterId")).isEqualTo("archived-kafka-id");
            assertThat(item.get("hostIp")).isEqualTo("192.168.3.229");
        });
        verify(alertRepository, never()).save(alert);
    }

    @Test
    void hidesLegacyAggregateAgentAlertFromCurrentAndResolvedViews() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);

        Alert alert = new Alert();
        alert.setAlertKey("external-agent-partial-test");
        alert.setSeverity("WARNING");
        alert.setTitle("External agents partially connected");
        alert.setStatus("ACTIVE");
        alert.setAffectedIps("192.168.3.191, 192.168.3.229");

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(hostRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc()).thenReturn(List.of(alert));

        AlertController controller = new AlertController(
                alertRepository, clusterRepository, hostRepository, taskRepository,
                mock(HostStatusService.class), hostParcelRepository, mock(ConsumerLagCacheService.class));

        assertThat(controller.getActiveAlerts().getBody()).isEmpty();
    }

    @Test
    void hidesPortCheckHistoryFromAlertsBecauseItBelongsInAudits() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);

        Alert portCheck = new Alert();
        portCheck.setAlertKey("task-failed-port-check");
        portCheck.setSeverity("CRITICAL");
        portCheck.setTitle("Check Ports failed");
        portCheck.setDescription("Port check failed: one port is unavailable");
        portCheck.setStatus("RESOLVED");

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(hostRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE")).thenReturn(List.of());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc()).thenReturn(List.of(portCheck));

        AlertController controller = new AlertController(
                alertRepository, clusterRepository, hostRepository, taskRepository,
                mock(HostStatusService.class), hostParcelRepository, mock(ConsumerLagCacheService.class));

        assertThat(controller.getActiveAlerts().getBody()).isEmpty();
    }
}
