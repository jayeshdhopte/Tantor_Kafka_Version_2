package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RollingRestartSummaryLifecycleTest {
    final JobRepository jobs = mock(JobRepository.class);
    final JobStepRepository steps = mock(JobStepRepository.class);
    final RollingRestartMappingService mapping = mock(RollingRestartMappingService.class);
    final JobService service = new JobService(jobs, steps, new ObjectMapper(), mock(ClusterRepository.class),
            mock(ExternalClusterRepository.class), mapping);
    Job job(JobStatus status) {
        var job = new Job(); job.setId(UUID.randomUUID()); job.setType(JobType.ROLLING_RESTART);
        job.setStatus(status); job.setRequestedBy("alice"); return job;
    }
    @ParameterizedTest
    @EnumSource(value=JobStatus.class,names={"SUCCESS","FAILED","PARTIAL_SUCCESS"})
    void terminalJobOutcomesReachTheSummary(JobStatus status) {
        var job = job(JobStatus.IN_PROGRESS);
        when(jobs.findById(job.getId())).thenReturn(Optional.of(job));
        service.updateJobStatus(job.getId(),status);
        verify(jobs).save(job);
        verify(mapping).synchronizeStatus(job,null,status==JobStatus.SUCCESS?"ROLLING_RESTART_COMPLETED":"ROLLING_RESTART_"+status.name());
        assertThat(job.getEndTime()).isNotNull();
    }
    @Test void claimingAQueuedRestartRecordsItsStart() {
        var job = job(JobStatus.PENDING);
        when(jobs.findByStatusOrderByCreatedAtAsc(JobStatus.PENDING)).thenReturn(List.of(job));
        assertThat(service.claimNextJob()).isPresent();
        verify(mapping).synchronizeStatus(job,null,"ROLLING_RESTART_STARTED");
        assertThat(job.getStatus()).isEqualTo(JobStatus.IN_PROGRESS);
    }
    @Test void retryCapturesTheAuthenticatedActorWithoutReplacingTheOriginalRequester() {
        var job = job(JobStatus.FAILED);
        when(jobs.findForUpdateById(job.getId())).thenReturn(Optional.of(job));
        when(jobs.findById(job.getId())).thenReturn(Optional.of(job));
        SecurityContextHolder.getContext().setAuthentication(UsernamePasswordAuthenticationToken.authenticated("bob","unused",List.of()));
        try { service.retryJob(job.getId()); } finally { SecurityContextHolder.clearContext(); }
        verify(mapping).synchronizeStatus(job,"bob","ROLLING_RESTART_RETRY_REQUESTED");
        assertThat(job.getRequestedBy()).isEqualTo("alice");
        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
    }
    @Test void backendRecoveryAlsoReturnsTheSummaryToPending() {
        var job = job(JobStatus.IN_PROGRESS);
        when(jobs.findByStatus(JobStatus.IN_PROGRESS)).thenReturn(List.of(job));
        when(jobs.findById(job.getId())).thenReturn(Optional.of(job));
        service.recoverInterruptedJobs();
        verify(mapping).synchronizeStatus(job,null,"ROLLING_RESTART_RECOVERED");
        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
    }
    @Test void otherJobTypesDoNotTouchRestartStorage() {
        var job = job(JobStatus.IN_PROGRESS); job.setType(JobType.CONFIG_CHANGE);
        when(jobs.findById(job.getId())).thenReturn(Optional.of(job));
        service.updateJobStatus(job.getId(),JobStatus.SUCCESS);
        verifyNoInteractions(mapping);
    }
}
