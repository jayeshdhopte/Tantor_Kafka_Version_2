package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfigRecoverySupersessionTest {
    final JobRepository repository = mock(JobRepository.class);
    final JobStepRepository steps = mock(JobStepRepository.class);
    final JobService service = new JobService(repository, steps, new ObjectMapper(), mock(ClusterRepository.class),
            mock(ExternalClusterRepository.class), mock(RollingRestartMappingService.class), mock(ConfigChangeMappingService.class));

    Job original(JobStatus status) {
        Job job = new Job(); job.setId(UUID.randomUUID()); job.setType(JobType.TARGETED_CONFIG_CHANGE);
        job.setStatus(status); job.setRollbackSupported(true);
        job.setResourceKey("cluster:" + UUID.randomUUID());
        when(repository.findById(job.getId())).thenReturn(Optional.of(job));
        when(repository.findForUpdateById(job.getId())).thenReturn(Optional.of(job));
        Job newer = new Job(); newer.setId(UUID.randomUUID()); newer.setStatus(JobStatus.SUCCESS);
        when(repository.findNewerAppliedConfiguration(job.getId())).thenReturn(Optional.of(newer));
        return job;
    }

    @Test void rejectsOldRollbackRetryBeforeChangingStatusStepsOrHistory() {
        Job old = original(JobStatus.ROLLBACK_FAILED);
        assertThatThrownBy(() -> service.retryJob(old.getId())).isInstanceOf(ClusterOperationConflictException.class)
                .hasMessageContaining("newer configuration job");
        assertThat(old.getStatus()).isEqualTo(JobStatus.ROLLBACK_FAILED);
        assertThat(old.getRetryCount()).isZero();
        verify(repository, never()).save(any()); verifyNoInteractions(steps);
    }

    @Test void rejectsOldInitialRollbackAndForwardResume() {
        Job old = original(JobStatus.FAILED);
        assertThatThrownBy(() -> service.requestRollback(old.getId())).isInstanceOf(ClusterOperationConflictException.class);
        assertThatThrownBy(() -> service.retryJob(old.getId())).isInstanceOf(ClusterOperationConflictException.class);
        verify(repository, never()).save(any()); verifyNoInteractions(steps);
    }

    @Test void reportsReasonWithoutChangingStoredJobStatusOrLogs() {
        Job old = original(JobStatus.ROLLBACK_FAILED); old.setLogs("original failure");
        Job view = service.getJob(old.getId());
        assertThat(view.getConfigRecoveryBlockedReason()).contains("newer configuration job", "disabled");
        assertThat(view.getLogs()).isEqualTo("original failure");
        assertThat(view.getStatus()).isEqualTo(JobStatus.ROLLBACK_FAILED);
        verify(repository, never()).save(any());
    }

    @Test void currentJobCanRecoverAndReasonClearsWhenNoNewerChangeExists() {
        Job old = original(JobStatus.ROLLBACK_FAILED);
        when(repository.findNewerAppliedConfiguration(old.getId())).thenReturn(Optional.empty());
        service.assertConfigRecoveryCurrent(old);
        assertThat(service.getJob(old.getId()).getConfigRecoveryBlockedReason()).isNull();
    }

    @Test void versionedChangesAreAlsoProtectedAndUnrelatedJobsAreUnaffected() {
        Job old = original(JobStatus.FAILED); old.setType(JobType.CONFIG_CHANGE);
        assertThatThrownBy(() -> service.assertConfigRecoveryCurrent(old)).isInstanceOf(ClusterOperationConflictException.class);
        old.setType(JobType.DEPLOYMENT);
        service.assertConfigRecoveryCurrent(old);
    }
}
