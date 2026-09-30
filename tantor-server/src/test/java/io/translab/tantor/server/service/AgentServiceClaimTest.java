package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.dto.TaskDto;
import io.translab.tantor.server.dto.TaskResultDto;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AgentServiceClaimTest {

    @Mock private HostRepository hostRepository;
    @Mock private TaskRepository taskRepository;
    @Mock private ClusterRepository clusterRepository;
    @Mock private ParcelService parcelService;
    @Mock private ActivityAlertService activityAlertService;
    @Mock private AuditService auditService;

    private AgentService service;

    @BeforeEach
    void setUp() {
        service = new AgentService(hostRepository, taskRepository, clusterRepository, new ObjectMapper(),
                parcelService, activityAlertService, auditService);
        ReflectionTestUtils.setField(service, "taskClaimLeaseSeconds", 120L);
        ReflectionTestUtils.setField(service, "maxTaskClaimAttempts", 3);
    }

    @Test
    void returnsOnlyTasksWhoseAtomicClaimSucceeds() {
        Task candidate = pendingTask();
        when(taskRepository.findByHostIdAndStatusOrderByCreatedAtAsc("host-a", "PENDING"))
                .thenReturn(List.of(candidate));
        when(taskRepository.claimPendingTask(eq(candidate.getId()), eq("host-a"), anyString(),
                any(OffsetDateTime.class), any(OffsetDateTime.class))).thenReturn(1);
        when(taskRepository.findById(candidate.getId())).thenReturn(Optional.of(candidate));

        List<TaskDto> claimed = service.getPendingTasks("host-a");

        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().getTaskId()).isEqualTo(candidate.getId().toString());
        assertThat(claimed.getFirst().getClaimToken()).isNotBlank();
        verify(taskRepository).claimPendingTask(eq(candidate.getId()), eq("host-a"), anyString(),
                any(OffsetDateTime.class), any(OffsetDateTime.class));
    }

    @Test
    void omitsTaskWhenAnotherPollerAlreadyClaimedIt() {
        Task candidate = pendingTask();
        when(taskRepository.findByHostIdAndStatusOrderByCreatedAtAsc("host-a", "PENDING"))
                .thenReturn(List.of(candidate));
        when(taskRepository.claimPendingTask(any(), anyString(), anyString(), any(), any())).thenReturn(0);

        assertThat(service.getPendingTasks("host-a")).isEmpty();
        verify(taskRepository, never()).findById(candidate.getId());
    }

    @Test
    void ignoresResultWithStaleClaimToken() {
        Task claimed = pendingTask();
        claimed.setStatus("IN_PROGRESS");
        claimed.setClaimToken("current-claim");
        when(taskRepository.findById(claimed.getId())).thenReturn(Optional.of(claimed));
        TaskResultDto result = new TaskResultDto();
        result.setTaskId(claimed.getId().toString());
        result.setClaimToken("stale-claim");
        result.setStatus("SUCCESS");

        service.processTaskResult(result);

        assertThat(claimed.getStatus()).isEqualTo("IN_PROGRESS");
        verify(taskRepository, never()).save(any(Task.class));
    }

    private Task pendingTask() {
        Task task = new Task();
        task.setId(UUID.randomUUID());
        task.setHostId("host-a");
        task.setCommand("INSTALL_KAFKA");
        task.setStatus("PENDING");
        task.setParameters("{}");
        return task;
    }
}
