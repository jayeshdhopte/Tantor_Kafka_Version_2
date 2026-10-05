package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.Alert;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.dto.HostHeartbeatDto;
import io.translab.tantor.server.service.AgentService;
import io.translab.tantor.server.service.ActivityAlertService;
import io.translab.tantor.server.repository.AlertRepository;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.ConsumerLagCacheService;
import io.translab.tantor.server.service.HostStatusService;
import io.translab.tantor.server.service.ParcelService;
import io.translab.tantor.server.audit.AuditService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.time.OffsetDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

class AlertControllerRecoveryTest {

    @Test
    void internalHostHeartbeatResolvesHostAndClusterOfflineAlerts() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);
        ConsumerLagCacheService consumerLagCacheService = mock(ConsumerLagCacheService.class);
        Map<String, Alert> savedAlerts = new ConcurrentHashMap<>();

        Host host = host("node-1", "192.168.3.11");
        host.setLastHeartbeat(OffsetDateTime.now().minusMinutes(5));
        Cluster cluster = new Cluster();
        cluster.setId(UUID.randomUUID());
        cluster.setName("internal-kafka");
        cluster.setStatus("RUNNING");
        ClusterServiceAssignment assignment = new ClusterServiceAssignment();
        assignment.setHostId(host.getId());
        cluster.setServices(List.of(assignment));

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of(cluster));
        when(hostRepository.findAll()).thenReturn(List.of(host));
        when(hostRepository.findById(host.getId())).thenReturn(Optional.of(host));
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(consumerLagCacheService.getSummaries(cluster.getId())).thenReturn(List.of());
        when(alertRepository.findByAlertKey(anyString()))
                .thenAnswer(call -> Optional.ofNullable(savedAlerts.get(call.getArgument(0))));
        when(alertRepository.save(any(Alert.class))).thenAnswer(call -> {
            Alert alert = call.getArgument(0);
            if (alert.getId() == null) alert.setId(UUID.randomUUID());
            savedAlerts.put(alert.getAlertKey(), alert);
            return alert;
        });
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE"))
                .thenAnswer(ignored -> savedAlerts.values().stream()
                        .filter(alert -> "ACTIVE".equals(alert.getStatus())).toList());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc())
                .thenAnswer(ignored -> savedAlerts.values().stream().toList());

        HostStatusService hostStatusService = new HostStatusService();
        org.springframework.test.util.ReflectionTestUtils.setField(
                hostStatusService, "heartbeatTimeoutSeconds", 90L);
        AlertController alerts = new AlertController(alertRepository, clusterRepository,
                hostRepository, taskRepository, hostStatusService,
                hostParcelRepository, consumerLagCacheService);
        assertThat(alerts.getActiveAlerts().getBody()).hasSize(2)
                .allSatisfy(alert -> assertThat(alert.get("status")).isEqualTo("ACTIVE"));

        AgentService agentService = new AgentService(hostRepository, taskRepository,
                clusterRepository, new ObjectMapper(), mock(ParcelService.class),
                mock(ActivityAlertService.class), mock(AuditService.class));
        HostHeartbeatDto heartbeat = new HostHeartbeatDto();
        heartbeat.setHostId(host.getId());
        assertThat(agentService.processHeartbeat(heartbeat, "192.168.3.11"))
                .isEqualTo(AgentService.HeartbeatResult.ACCEPTED);
        assertThat(host.getStatus()).isEqualTo("OCCUPIED");

        assertThat(alerts.getActiveAlerts().getBody()).hasSize(2)
                .allSatisfy(alert -> {
                    assertThat(alert.get("status")).isEqualTo("RESOLVED");
                    assertThat(alert.get("resolvedAt")).isNotNull();
                });
    }

    @Test
    void assignedOccupiedHostsResolveTheirAlertsWhenHeartbeatsReturn() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);
        HostStatusService hostStatusService = mock(HostStatusService.class);
        ConsumerLagCacheService consumerLagCacheService = mock(ConsumerLagCacheService.class);
        Map<String, Alert> savedAlerts = new ConcurrentHashMap<>();
        AtomicBoolean online = new AtomicBoolean(false);

        Cluster cluster = new Cluster();
        cluster.setId(UUID.randomUUID());
        cluster.setName("internal-kafka");
        cluster.setStatus("RUNNING");
        Host first = host("node-1", "192.168.3.11");
        Host second = host("node-2", "192.168.3.12");
        ClusterServiceAssignment firstAssignment = new ClusterServiceAssignment();
        firstAssignment.setHostId(first.getId());
        ClusterServiceAssignment secondAssignment = new ClusterServiceAssignment();
        secondAssignment.setHostId(second.getId());
        cluster.setServices(List.of(firstAssignment, secondAssignment));

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of(cluster));
        when(hostRepository.findAll()).thenReturn(List.of(first, second));
        when(hostStatusService.isInfrastructureHost(any(Host.class))).thenReturn(true);
        when(hostStatusService.agentConnectivityStatus(any(Host.class)))
                .thenAnswer(ignored -> online.get() ? "ONLINE" : "OFFLINE");
        when(taskRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(consumerLagCacheService.getSummaries(cluster.getId())).thenReturn(List.of());
        when(alertRepository.findByAlertKey(anyString()))
                .thenAnswer(call -> Optional.ofNullable(savedAlerts.get(call.getArgument(0))));
        when(alertRepository.save(any(Alert.class))).thenAnswer(call -> {
            Alert alert = call.getArgument(0);
            if (alert.getId() == null) alert.setId(UUID.randomUUID());
            savedAlerts.put(alert.getAlertKey(), alert);
            return alert;
        });
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE"))
                .thenAnswer(ignored -> savedAlerts.values().stream()
                        .filter(alert -> "ACTIVE".equals(alert.getStatus())).toList());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc())
                .thenAnswer(ignored -> savedAlerts.values().stream().toList());

        AlertController controller = new AlertController(
                alertRepository, clusterRepository, hostRepository, taskRepository,
                hostStatusService, hostParcelRepository, consumerLagCacheService);

        var current = controller.getActiveAlerts().getBody();
        assertThat(current).hasSize(4);
        assertThat(current).allSatisfy(alert -> {
            assertThat(alert.get("status")).isEqualTo("ACTIVE");
            assertThat(alert.get("source")).isIn("host", "cluster");
        });

        online.set(true);
        first.setDiskUsedGb(20L);
        second.setDiskUsedGb(20L);
        first.setMemUsedMb(20L);
        second.setMemUsedMb(20L);
        var recovered = controller.getActiveAlerts().getBody();
        assertThat(recovered).hasSize(4);
        assertThat(recovered).allSatisfy(alert -> {
            assertThat(alert.get("status")).isEqualTo("RESOLVED");
            assertThat(alert.get("resolvedAt")).isNotNull();
        });
    }

    private Host host(String id, String ip) {
        Host host = new Host();
        host.setId(id);
        host.setHostname(id);
        host.setHostIp(ip);
        host.setStatus("OCCUPIED");
        host.setDiskTotalGb(100L);
        host.setDiskUsedGb(99L);
        host.setMemTotalMb(100L);
        host.setMemUsedMb(99L);
        return host;
    }

    @Test
    void successfulRetryMovesFailedTaskAlertToResolved() {
        AlertRepository alertRepository = mock(AlertRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        HostRepository hostRepository = mock(HostRepository.class);
        TaskRepository taskRepository = mock(TaskRepository.class);
        HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);
        Map<String, Alert> savedAlerts = new ConcurrentHashMap<>();

        Task failed = new Task();
        failed.setId(UUID.randomUUID());
        failed.setHostId("node-1");
        failed.setCommand("INSTALL_KAFKA");
        failed.setStatus("FAILED");
        failed.setUpdatedAt(OffsetDateTime.now().minusMinutes(2));
        failed.setErrorMsg("Package installation failed");
        Task retry = new Task();
        retry.setId(UUID.randomUUID());
        retry.setHostId("node-1");
        retry.setCommand("INSTALL_KAFKA");
        retry.setStatus("SUCCESS");
        retry.setUpdatedAt(OffsetDateTime.now());

        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of());
        when(hostRepository.findAll()).thenReturn(List.of());
        when(hostParcelRepository.findAll()).thenReturn(List.of());
        when(taskRepository.findAll()).thenReturn(List.of(failed), List.of(failed, retry));
        when(alertRepository.findByAlertKey(anyString()))
                .thenAnswer(call -> Optional.ofNullable(savedAlerts.get(call.getArgument(0))));
        when(alertRepository.save(any(Alert.class))).thenAnswer(call -> {
            Alert alert = call.getArgument(0);
            if (alert.getId() == null) alert.setId(UUID.randomUUID());
            savedAlerts.put(alert.getAlertKey(), alert);
            return alert;
        });
        when(alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE"))
                .thenAnswer(ignored -> savedAlerts.values().stream()
                        .filter(alert -> "ACTIVE".equals(alert.getStatus())).toList());
        when(alertRepository.findTop100ByOrderByUpdatedAtDesc())
                .thenAnswer(ignored -> savedAlerts.values().stream().toList());

        AlertController controller = new AlertController(
                alertRepository, clusterRepository, hostRepository, taskRepository,
                mock(HostStatusService.class), hostParcelRepository, mock(ConsumerLagCacheService.class));

        assertThat(controller.getActiveAlerts().getBody()).singleElement().satisfies(alert -> {
            assertThat(alert.get("status")).isEqualTo("ACTIVE");
            assertThat(alert.get("source")).isEqualTo("task");
        });
        assertThat(controller.getActiveAlerts().getBody()).singleElement().satisfies(alert ->
                assertThat(alert.get("status")).isEqualTo("RESOLVED"));
    }

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
