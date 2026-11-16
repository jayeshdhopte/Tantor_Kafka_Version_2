package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class ExternalBrokerJobCompletionTest {
  @Test
  void staleFailureCannotDowngradePublicationOrNewAttempt() {
    var repository = mock(JobRepository.class);
    var service =
        new JobService(
            repository,
            mock(JobStepRepository.class),
            new ObjectMapper(),
            mock(ClusterRepository.class),
            mock(ExternalClusterRepository.class),
            mock(RollingRestartMappingService.class),
            mock(ConfigChangeMappingService.class));
    var job = new Job();
    job.setId(UUID.randomUUID());
    job.setType(JobType.EXTERNAL_BROKER_ADDITION);
    job.setRetryCount(1);
    job.setStatus(JobStatus.SUCCESS);
    when(repository.findForUpdateById(job.getId())).thenReturn(Optional.of(job));
    assertFalse(
        service.finishExternalBrokerAttempt(job.getId(), 1, JobStatus.FAILED, "late callback"));
    job.setStatus(JobStatus.IN_PROGRESS);
    job.setRetryCount(2);
    assertFalse(
        service.finishExternalBrokerAttempt(job.getId(), 1, JobStatus.FAILED, "old worker"));
    verify(repository, never()).save(any());
    assertThrows(
        IllegalStateException.class,
        () -> service.finishExternalBrokerAttempt(job.getId(), 2, JobStatus.SUCCESS, "unverified"));
    assertTrue(
        service.finishExternalBrokerAttempt(
            job.getId(), 2, JobStatus.PARTIAL_SUCCESS, "reconciliation required"));
    assertEquals(JobStatus.PARTIAL_SUCCESS, job.getStatus());
    assertTrue(job.getLogs().contains("reconciliation required"));
  }

  @Test
  void genericExecutorCannotInventExternalSuccess() {
    var jobs = mock(JobService.class);
    var handler = mock(JobHandler.class);
    var job = new Job();
    job.setId(UUID.randomUUID());
    job.setType(JobType.EXTERNAL_BROKER_ADDITION);
    job.setStatus(JobStatus.IN_PROGRESS);
    job.setRetryCount(0);
    when(jobs.getJob(job.getId())).thenReturn(job);
    when(handler.supports(job.getType())).thenReturn(true);
    new JobExecutor(jobs, List.of(handler)).execute(job.getId(), false);
    verify(jobs, never()).updateJobStatus(job.getId(), JobStatus.SUCCESS);
    verify(jobs)
        .finishExternalBrokerAttempt(
            eq(job.getId()), eq(0), eq(JobStatus.FAILED), contains("without verified publication"));
  }
}
