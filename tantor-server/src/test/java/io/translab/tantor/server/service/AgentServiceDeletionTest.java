package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.dto.TaskResultDto;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AgentServiceDeletionTest {
    @Mock HostRepository hostRepository;
    @Mock TaskRepository taskRepository;
    @Mock ClusterRepository clusterRepository;
    @Mock ParcelService parcelService;
    @Mock ActivityAlertService activityAlertService;
    @Mock AuditService auditService;

    private AgentService service;

    @BeforeEach
    void setUp() {
        service = new AgentService(hostRepository, taskRepository, clusterRepository, new ObjectMapper(),
                parcelService, activityAlertService, auditService);
    }

    @Test
    void waitsForEveryHostBeforeReleasingAssignments() {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = cluster(clusterId, "host-1", "host-2");
        Task first = cleanupTask(clusterId, "host-1");
        Task second = cleanupTask(clusterId, "host-2");
        Host firstHost = host(clusterId, "host-1");
        Host secondHost = host(clusterId, "host-2");
        when(taskRepository.findById(first.getId())).thenReturn(Optional.of(first));
        when(taskRepository.findById(second.getId())).thenReturn(Optional.of(second));
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(taskRepository.findByClusterIdAndHostIdAndCommandOrderByCreatedAtDesc(clusterId, "host-1", "DELETE_CLUSTER"))
                .thenReturn(List.of(first));
        when(taskRepository.findByClusterIdAndHostIdAndCommandOrderByCreatedAtDesc(clusterId, "host-2", "DELETE_CLUSTER"))
                .thenReturn(List.of(second));
        when(hostRepository.findById("host-1")).thenReturn(Optional.of(firstHost));
        when(hostRepository.findById("host-2")).thenReturn(Optional.of(secondHost));

        service.processTaskResult(result(first, "SUCCESS"));

        assertThat(cluster.getStatus()).isEqualTo("DELETING");
        assertThat(cluster.getDeletedAt()).isNull();
        assertThat(firstHost.getClusterId()).isEqualTo(clusterId);
        verify(hostRepository, never()).save(firstHost);

        service.processTaskResult(result(second, "SUCCESS"));

        assertThat(cluster.getStatus()).isEqualTo("DELETED");
        assertThat(cluster.getDeletedAt()).isNotNull();
        assertThat(firstHost.getClusterId()).isNull();
        assertThat(secondHost.getClusterId()).isNull();
        verify(hostRepository).save(firstHost);
        verify(hostRepository).save(secondHost);
    }

    @Test
    void failedCleanupKeepsClusterAndHostForRetry() {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = cluster(clusterId, "host-1");
        Task task = cleanupTask(clusterId, "host-1");
        Host host = host(clusterId, "host-1");
        when(taskRepository.findById(task.getId())).thenReturn(Optional.of(task));
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));

        service.processTaskResult(result(task, "FAILED"));

        assertThat(cluster.getStatus()).isEqualTo("DELETE_FAILED");
        assertThat(cluster.getDeletedAt()).isNull();
        assertThat(host.getClusterId()).isEqualTo(clusterId);
        verify(hostRepository, never()).save(host);
    }

    @Test
    void laterHostSuccessDoesNotHideAnotherHostsCleanupFailure() {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = cluster(clusterId, "host-1", "host-2");
        Task failed = cleanupTask(clusterId, "host-1");
        failed.setStatus("FAILED");
        Task successful = cleanupTask(clusterId, "host-2");
        when(taskRepository.findById(successful.getId())).thenReturn(Optional.of(successful));
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(taskRepository.findByClusterIdAndHostIdAndCommandOrderByCreatedAtDesc(clusterId, "host-1", "DELETE_CLUSTER"))
                .thenReturn(List.of(failed));
        when(taskRepository.findByClusterIdAndHostIdAndCommandOrderByCreatedAtDesc(clusterId, "host-2", "DELETE_CLUSTER"))
                .thenReturn(List.of(successful));

        service.processTaskResult(result(successful, "SUCCESS"));

        assertThat(cluster.getStatus()).isEqualTo("DELETE_FAILED");
        assertThat(cluster.getDeletedAt()).isNull();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"PRECHECK_SCHEMA", "INSTALL_SCHEMA",
            "VERIFY_SCHEMA_REGISTRY", "PRECHECK_CONNECT", "INSTALL_CONNECT", "VERIFY_CONNECT"})
    void dataServiceTaskFailureDoesNotChangeHealthyKafkaCluster(String command) {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = cluster(clusterId, "host-1");
        cluster.setStatus("SUCCESS");
        Task task = cleanupTask(clusterId, "host-1");
        task.setCommand(command);
        when(taskRepository.findById(task.getId())).thenReturn(Optional.of(task));
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));

        service.processTaskResult(result(task, "FAILED"));

        assertThat(cluster.getStatus()).isEqualTo("SUCCESS");
        verify(clusterRepository, never()).save(cluster);
    }

    private Cluster cluster(UUID id, String... hostIds) {
        Cluster cluster = new Cluster();
        cluster.setId(id);
        cluster.setStatus("DELETING");
        cluster.setServices(java.util.Arrays.stream(hostIds).map(hostId -> {
            ClusterServiceAssignment assignment = new ClusterServiceAssignment();
            assignment.setHostId(hostId);
            assignment.setCluster(cluster);
            return assignment;
        }).toList());
        return cluster;
    }

    private Task cleanupTask(UUID clusterId, String hostId) {
        Task task = new Task();
        task.setId(UUID.randomUUID());
        task.setClusterId(clusterId);
        task.setHostId(hostId);
        task.setCommand("DELETE_CLUSTER");
        task.setStatus("IN_PROGRESS");
        task.setClaimToken("claim-" + hostId);
        task.setParameters("{}");
        return task;
    }

    private Host host(UUID clusterId, String hostId) {
        Host host = new Host();
        host.setId(hostId);
        host.setClusterId(clusterId);
        host.setStatus("OCCUPIED");
        return host;
    }

    private TaskResultDto result(Task task, String status) {
        TaskResultDto result = new TaskResultDto();
        result.setTaskId(task.getId().toString());
        result.setClaimToken(task.getClaimToken());
        result.setStatus(status);
        return result;
    }
}
