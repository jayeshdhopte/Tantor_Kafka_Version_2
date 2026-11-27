package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class JavaUpgradeLifecycleTest {
 @Test void completedFailureReleasesJobLockWithoutChangingCluster() {
  JobRepository repository=mock(JobRepository.class);
  ClusterRepository clusters=mock(ClusterRepository.class);
  JobService service=new JobService(repository,mock(JobStepRepository.class),new ObjectMapper(),clusters,mock(ExternalClusterRepository.class),mock(RollingRestartMappingService.class),mock(ConfigChangeMappingService.class));
  UUID clusterId=UUID.randomUUID();
  Job job=new Job();job.setId(UUID.randomUUID());job.setType(JobType.JAVA_UPGRADE);job.setResourceKey("cluster:"+clusterId);job.setStatus(JobStatus.IN_PROGRESS);
  when(repository.findById(job.getId())).thenReturn(Optional.of(job));
  when(repository.save(job)).thenReturn(job);
  service.updateJobStatus(job.getId(),JobStatus.FAILED);
  assertThat(job.getStatus()).isEqualTo(JobStatus.FAILED);
  assertThat(job.getEndTime()).isNotNull();
  assertThat(service.getActiveClusterJobs()).isEmpty();
  service.assertClusterMutable(clusterId);
  verify(clusters,never()).save(any());
 }
 @Test void unfinishedAgentWorkRetainsSafetyLockEvenIfJobFailed() {
  JobRepository repository=mock(JobRepository.class);
  JobService service=new JobService(repository,mock(JobStepRepository.class),new ObjectMapper(),mock(ClusterRepository.class),mock(ExternalClusterRepository.class),mock(RollingRestartMappingService.class),mock(ConfigChangeMappingService.class));
  UUID clusterId=UUID.randomUUID();
  Job job=new Job();job.setId(UUID.randomUUID());job.setType(JobType.JAVA_UPGRADE);job.setResourceKey("cluster:"+clusterId);job.setStatus(JobStatus.FAILED);
  when(repository.findWithUnfinishedRestartTasks()).thenReturn(List.of(job));
  assertThat(service.getActiveClusterJobs()).hasSize(1);
  assertThatThrownBy(()->service.assertClusterMutable(clusterId)).isInstanceOf(ClusterOperationConflictException.class);
 }
}
