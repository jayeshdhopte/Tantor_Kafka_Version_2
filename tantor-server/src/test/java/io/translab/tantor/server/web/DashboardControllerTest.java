package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.ActivityLogRepository;
import io.translab.tantor.server.repository.AlertRepository;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.HostStatusService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DashboardControllerTest {

    @Test
    void offlineHostDiskUsageRemainsVisibleAndReturnsToLive() {
        Host host = new Host();
        host.setId("external-host-1");
        host.setHostname("broker1.translab.io");
        host.setDiskUsedGb(20L);
        host.setDiskTotalGb(100L);

        ClusterRepository clusters = mock(ClusterRepository.class);
        HostRepository hosts = mock(HostRepository.class);
        AlertRepository alerts = mock(AlertRepository.class);
        ActivityLogRepository activities = mock(ActivityLogRepository.class);
        TaskRepository tasks = mock(TaskRepository.class);
        HostParcelRepository parcels = mock(HostParcelRepository.class);
        HostStatusService hostStatus = mock(HostStatusService.class);

        when(clusters.findByStatusNot("DELETED")).thenReturn(List.of());
        when(hosts.findAll()).thenReturn(List.of(host));
        when(tasks.findAll()).thenReturn(List.of());
        when(parcels.findAll()).thenReturn(List.of());
        when(activities.findTop50ByOrderByCreatedAtDesc()).thenReturn(List.of());
        when(hostStatus.isInfrastructureHost(host)).thenReturn(true);
        AtomicReference<String> connectivityStatus = new AtomicReference<>("OFFLINE");
        when(hostStatus.agentConnectivityStatus(host)).thenAnswer(ignored -> connectivityStatus.get());
        when(hostStatus.effectiveStatus(host)).thenReturn("OCCUPIED");

        DashboardController controller = new DashboardController(
                clusters, hosts, alerts, activities, tasks, parcels, hostStatus);

        Map<String, Object> offlineResponse = controller.getDashboard().getBody();
        assertThat(offlineResponse).isNotNull();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> offlineDiskUsage =
                (List<Map<String, Object>>) offlineResponse.get("hostDiskUsage");
        assertThat(offlineDiskUsage)
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.get("hostId")).isEqualTo("external-host-1");
                    assertThat(row.get("status")).isEqualTo("OFFLINE");
                    assertThat(row.get("usedPct")).isEqualTo(20L);
                });

        connectivityStatus.set("ONLINE");
        Map<String, Object> onlineResponse = controller.getDashboard().getBody();
        assertThat(onlineResponse).isNotNull();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> onlineDiskUsage =
                (List<Map<String, Object>>) onlineResponse.get("hostDiskUsage");
        assertThat(onlineDiskUsage)
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.get("hostId")).isEqualTo("external-host-1");
                    assertThat(row.get("status")).isEqualTo("ONLINE");
                });
    }

    @Test
    void occupiedHostWithFreshAgentIsStillCountedAsActive() {
        Host occupied = new Host();
        occupied.setId("occupied-host");
        occupied.setHostname("occupied-host");
        occupied.setStatus("OCCUPIED");

        ClusterRepository clusters = mock(ClusterRepository.class);
        HostRepository hosts = mock(HostRepository.class);
        AlertRepository alerts = mock(AlertRepository.class);
        ActivityLogRepository activities = mock(ActivityLogRepository.class);
        TaskRepository tasks = mock(TaskRepository.class);
        HostParcelRepository parcels = mock(HostParcelRepository.class);
        HostStatusService hostStatus = mock(HostStatusService.class);

        when(clusters.findByStatusNot("DELETED")).thenReturn(List.of());
        when(hosts.findAll()).thenReturn(List.of(occupied));
        when(tasks.findAll()).thenReturn(List.of());
        when(parcels.findAll()).thenReturn(List.of());
        when(activities.findTop50ByOrderByCreatedAtDesc()).thenReturn(List.of());
        when(hostStatus.isInfrastructureHost(occupied)).thenReturn(true);
        when(hostStatus.effectiveStatus(occupied)).thenReturn("OCCUPIED");
        when(hostStatus.agentConnectivityStatus(occupied)).thenReturn("ONLINE");

        DashboardController controller = new DashboardController(
                clusters, hosts, alerts, activities, tasks, parcels, hostStatus);

        Map<String, Object> response = controller.getDashboard().getBody();

        assertThat(response).isNotNull();
        @SuppressWarnings("unchecked")
        Map<String, Object> summary = (Map<String, Object>) response.get("summary");
        assertThat(summary.get("activeHosts")).isEqualTo(1L);
        assertThat(summary.get("totalHosts")).isEqualTo(1);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> runningServices =
                (List<Map<String, Object>>) response.get("runningServices");
        assertThat(runningServices)
                .filteredOn(row -> "Agent fleet".equals(row.get("name")))
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.get("description")).isEqualTo("1 active host");
                    assertThat(row.get("status")).isEqualTo("SUCCESS");
                });
    }
}
