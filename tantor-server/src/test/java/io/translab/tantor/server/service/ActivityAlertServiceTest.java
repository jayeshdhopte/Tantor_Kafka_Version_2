package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Alert;
import io.translab.tantor.server.repository.ActivityLogRepository;
import io.translab.tantor.server.repository.AlertRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ActivityAlertServiceTest {

    @Test
    void activatesOneKeyedDegradedAlertAndResolvesLegacyDuplicate() {
        UUID clusterId = UUID.randomUUID();
        AlertRepository repository = mock(AlertRepository.class);
        ActivityAlertService service = new ActivityAlertService(
                mock(ActivityLogRepository.class), repository);

        Alert legacy = alert(null, ActivityAlertService.EXTERNAL_DEGRADED_TITLE, "ACTIVE", clusterId);
        when(repository.findByAlertKey(ActivityAlertService.externalDegradedKey(clusterId)))
                .thenReturn(Optional.empty());
        when(repository.findByClusterIdAndStatusAndTitleIn(
                clusterId,
                "ACTIVE",
                List.of(ActivityAlertService.EXTERNAL_DEGRADED_TITLE, ActivityAlertService.EXTERNAL_FAILED_TITLE, ActivityAlertService.EXTERNAL_AGENT_PARTIAL_TITLE, ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE)))
                .thenReturn(List.of(legacy));

        service.synchronizeExternalClusterHealth(clusterId, "payments", "DEGRADED", 0, 0, (List<ActivityAlertService.OfflineAgentInfo>) null);

        ArgumentCaptor<Alert> saved = ArgumentCaptor.forClass(Alert.class);
        verify(repository, org.mockito.Mockito.times(2)).save(saved.capture());
        Alert active = saved.getAllValues().stream()
                .filter(value -> ActivityAlertService.externalDegradedKey(clusterId)
                        .equals(value.getAlertKey()))
                .findFirst()
                .orElseThrow();
        assertThat(active.getStatus()).isEqualTo("ACTIVE");
        assertThat(active.getSource()).isEqualTo("external_health");
        assertThat(active.getResolvedAt()).isNull();
        assertThat(legacy.getStatus()).isEqualTo("RESOLVED");
        assertThat(legacy.getResolvedAt()).isNotNull();
    }

    @Test
    void resolvesDegradedAndFailedAlertsWhenExternalClusterRecovers() {
        UUID clusterId = UUID.randomUUID();
        AlertRepository repository = mock(AlertRepository.class);
        ActivityAlertService service = new ActivityAlertService(
                mock(ActivityLogRepository.class), repository);

        Alert degraded = alert(
                ActivityAlertService.externalDegradedKey(clusterId),
                ActivityAlertService.EXTERNAL_DEGRADED_TITLE,
                "ACTIVE",
                clusterId);
        Alert failed = alert(
                ActivityAlertService.externalFailedKey(clusterId),
                ActivityAlertService.EXTERNAL_FAILED_TITLE,
                "ACTIVE",
                clusterId);
        when(repository.findByClusterIdAndStatusAndTitleIn(
                clusterId,
                "ACTIVE",
                List.of(ActivityAlertService.EXTERNAL_DEGRADED_TITLE, ActivityAlertService.EXTERNAL_FAILED_TITLE, ActivityAlertService.EXTERNAL_AGENT_PARTIAL_TITLE, ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE)))
                .thenReturn(List.of(degraded, failed));

        service.synchronizeExternalClusterHealth(clusterId, "payments", "SUCCESS", 1, 1, (List<ActivityAlertService.OfflineAgentInfo>) null);

        assertThat(degraded.getStatus()).isEqualTo("RESOLVED");
        assertThat(degraded.getResolvedAt()).isNotNull();
        assertThat(failed.getStatus()).isEqualTo("RESOLVED");
        assertThat(failed.getResolvedAt()).isNotNull();
        verify(repository).save(degraded);
        verify(repository).save(failed);
    }

    @Test
    void resolvesLegacyAlertWhenExternalClusterNoLongerExists() {
        UUID deletedClusterId = UUID.randomUUID();
        UUID activeClusterId = UUID.randomUUID();
        AlertRepository repository = mock(AlertRepository.class);
        ActivityAlertService service = new ActivityAlertService(
                mock(ActivityLogRepository.class), repository);

        Alert orphaned = alert(
                null,
                ActivityAlertService.EXTERNAL_DEGRADED_TITLE,
                "ACTIVE",
                deletedClusterId);
        Alert current = alert(
                ActivityAlertService.externalDegradedKey(activeClusterId),
                ActivityAlertService.EXTERNAL_DEGRADED_TITLE,
                "ACTIVE",
                activeClusterId);
        when(repository.findByStatusAndTitleIn(
                "ACTIVE",
                List.of(ActivityAlertService.EXTERNAL_DEGRADED_TITLE, ActivityAlertService.EXTERNAL_FAILED_TITLE, ActivityAlertService.EXTERNAL_AGENT_PARTIAL_TITLE, ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE)))
                .thenReturn(List.of(orphaned, current));

        service.resolveOrphanedExternalClusterHealthAlerts(Set.of(activeClusterId));

        assertThat(orphaned.getStatus()).isEqualTo("RESOLVED");
        assertThat(orphaned.getResolvedAt()).isNotNull();
        assertThat(current.getStatus()).isEqualTo("ACTIVE");
        verify(repository).save(orphaned);
        verify(repository, org.mockito.Mockito.never()).save(current);
    }

    @Test
    void createsIndependentOfflineAgentAlerts() {
        UUID clusterId = UUID.randomUUID();
        AlertRepository repository = mock(AlertRepository.class);
        ActivityAlertService service = new ActivityAlertService(
                mock(ActivityLogRepository.class), repository);

        String agentId = "agent-1";
        when(repository.findByAlertKey(ActivityAlertService.externalAgentOfflineKey(clusterId, agentId)))
                .thenReturn(Optional.empty());
        when(repository.findByClusterIdAndStatusAndTitleIn(
                clusterId,
                "ACTIVE",
                List.of(ActivityAlertService.EXTERNAL_DEGRADED_TITLE, ActivityAlertService.EXTERNAL_FAILED_TITLE, ActivityAlertService.EXTERNAL_AGENT_PARTIAL_TITLE, ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE)))
                .thenReturn(List.of());

        List<ActivityAlertService.OfflineAgentInfo> offlineAgents = List.of(
                new ActivityAlertService.OfflineAgentInfo(agentId, "host-1", List.of("192.168.3.191")));

        service.synchronizeExternalClusterHealth(
                clusterId, "payments", "PARTIAL", 2, 3, offlineAgents);

        ArgumentCaptor<Alert> saved = ArgumentCaptor.forClass(Alert.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().getTitle()).isEqualTo(ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE);
        assertThat(saved.getValue().getDescription()).contains("host-1");
        assertThat(saved.getValue().getAffectedIps()).isEqualTo("192.168.3.191");
        assertThat(saved.getValue().getAlertKey()).isEqualTo(ActivityAlertService.externalAgentOfflineKey(clusterId, agentId));
    }

    @Test
    void createsOneAlertForEachOfflineAgentWithoutAggregateAlert() {
        UUID clusterId = UUID.randomUUID();
        AlertRepository repository = mock(AlertRepository.class);
        ActivityAlertService service = new ActivityAlertService(
                mock(ActivityLogRepository.class), repository);

        when(repository.findByAlertKey(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(Optional.empty());
        when(repository.findByClusterIdAndStatusAndTitleIn(
                clusterId,
                "ACTIVE",
                List.of(ActivityAlertService.EXTERNAL_DEGRADED_TITLE, ActivityAlertService.EXTERNAL_FAILED_TITLE, ActivityAlertService.EXTERNAL_AGENT_PARTIAL_TITLE, ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE)))
                .thenReturn(List.of());

        service.synchronizeExternalClusterHealth(
                clusterId,
                "payments",
                "DEGRADED",
                1,
                3,
                List.of(
                        new ActivityAlertService.OfflineAgentInfo("agent-1", "broker-1", List.of("192.168.3.191")),
                        new ActivityAlertService.OfflineAgentInfo("agent-2", "broker-2", List.of("192.168.3.229"))));

        ArgumentCaptor<Alert> saved = ArgumentCaptor.forClass(Alert.class);
        verify(repository, org.mockito.Mockito.times(2)).save(saved.capture());
        assertThat(saved.getAllValues())
                .extracting(Alert::getAlertKey)
                .containsExactlyInAnyOrder(
                        ActivityAlertService.externalAgentOfflineKey(clusterId, "agent-1"),
                        ActivityAlertService.externalAgentOfflineKey(clusterId, "agent-2"));
        assertThat(saved.getAllValues())
                .extracting(Alert::getAffectedIps)
                .containsExactlyInAnyOrder("192.168.3.191", "192.168.3.229");
        assertThat(saved.getAllValues())
                .extracting(Alert::getTitle)
                .containsOnly(ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE);
    }

    @Test
    void keepsAgentOfflineAlertsActive() {
        UUID clusterId = UUID.randomUUID();
        AlertRepository repository = mock(AlertRepository.class);
        ActivityAlertService service = new ActivityAlertService(
                mock(ActivityLogRepository.class), repository);

        String agentId = "agent-2";
        when(repository.findByAlertKey(ActivityAlertService.externalAgentOfflineKey(clusterId, agentId)))
                .thenReturn(Optional.empty());
        when(repository.findByClusterIdAndStatusAndTitleIn(
                clusterId,
                "ACTIVE",
                List.of(ActivityAlertService.EXTERNAL_DEGRADED_TITLE, ActivityAlertService.EXTERNAL_FAILED_TITLE, ActivityAlertService.EXTERNAL_AGENT_PARTIAL_TITLE, ActivityAlertService.EXTERNAL_AGENT_OFFLINE_TITLE)))
                .thenReturn(List.of());

        List<ActivityAlertService.OfflineAgentInfo> offlineAgents = List.of(
                new ActivityAlertService.OfflineAgentInfo(agentId, "host-2", List.of("192.168.3.229")));

        service.synchronizeExternalClusterHealth(
                clusterId, "payments", "DEGRADED", 0, 3, offlineAgents);

        ArgumentCaptor<Alert> saved = ArgumentCaptor.forClass(Alert.class);
        verify(repository, org.mockito.Mockito.atLeastOnce()).save(saved.capture());
        Alert active = saved.getAllValues().stream()
                .filter(value -> ActivityAlertService.externalAgentOfflineKey(clusterId, agentId)
                        .equals(value.getAlertKey()))
                .findFirst()
                .orElseThrow();
        
        assertThat(active.getStatus()).isEqualTo("ACTIVE");
        assertThat(active.getAffectedIps()).isEqualTo("192.168.3.229");
    }

    private Alert alert(String key, String title, String status, UUID clusterId) {
        Alert alert = new Alert();
        alert.setAlertKey(key);
        alert.setTitle(title);
        alert.setSeverity("WARNING");
        alert.setStatus(status);
        alert.setClusterId(clusterId);
        return alert;
    }
}

