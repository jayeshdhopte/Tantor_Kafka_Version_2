package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaExpansionJobGuardTest {
    final JobRepository jobs=mock(JobRepository.class);final JobStepRepository steps=mock(JobStepRepository.class);final ClusterRepository clusters=mock(ClusterRepository.class);final KafkaExpansionOperationRepository operations=mock(KafkaExpansionOperationRepository.class);
    final JobService service=new JobService(jobs,steps,new ObjectMapper(),clusters,mock(ExternalClusterRepository.class),mock(RollingRestartMappingService.class),mock(ConfigChangeMappingService.class));
    KafkaExpansionOperation reserved(){var op=new KafkaExpansionOperation();op.setId(UUID.randomUUID());op.setClusterId(UUID.randomUUID());op.setJobId(UUID.randomUUID());op.setState(KafkaExpansionState.RECOVERY_REQUIRED);ReflectionTestUtils.setField(service,"quorumExpansions",operations);when(operations.findReservedForCluster(op.getClusterId())).thenReturn(List.of(op));return op;}
    @Test void unresolvedExpansionBlocksAllCompetingClusterMutations(){var op=reserved();for(var type:List.of(JobType.ROLLING_RESTART,JobType.CONFIG_CHANGE,JobType.TARGETED_CONFIG_CHANGE,JobType.EXTERNAL_SERVICES,JobType.BROKER_ADDITION,JobType.DEPLOYMENT,JobType.CLUSTER_DELETION)){var job=new Job();job.setType(type);job.setResourceKey("cluster:"+op.getClusterId());assertThrows(ClusterOperationConflictException.class,()->service.createJob(job),type.name());}verify(jobs,never()).saveAndFlush(any());}
    @Test void onlyTheRunningOwnerCanResumeUnderItsRetainedReservation(){var op=reserved();var job=new Job();job.setId(op.getJobId());job.setType(JobType.QUORUM_EXPANSION);job.setResourceKey("cluster:"+op.getClusterId());job.setStatus(JobStatus.IN_PROGRESS);when(jobs.findById(job.getId())).thenReturn(Optional.of(job));assertDoesNotThrow(()->service.assertQuorumExpansionOwnsCluster(op.getClusterId(),job.getId()));job.setStatus(JobStatus.PENDING);assertThrows(ClusterOperationConflictException.class,()->service.assertQuorumExpansionOwnsCluster(op.getClusterId(),job.getId()));}
    @Test void failedExpansionRemainsVisibleAsRecoveryRequired(){var op=reserved();when(operations.findAllReserved()).thenReturn(List.of(op));var job=new Job();job.setId(op.getJobId());job.setType(JobType.QUORUM_EXPANSION);job.setResourceKey("cluster:"+op.getClusterId());job.setStatus(JobStatus.FAILED);when(jobs.findById(job.getId())).thenReturn(Optional.of(job));var active=service.getActiveClusterJobs();assertEquals(1,active.size());assertEquals("RECOVERY_REQUIRED",active.getFirst().get("status"));assertTrue(active.getFirst().get("progress").contains("same operation"));}
    @Test void genericTaskActionsCannotBypassTheParentLedger(){var deployment=mock(DeploymentService.class,CALLS_REAL_METHODS);for(String command:List.of(KafkaExpansionLedger.CANDIDATE,KafkaExpansionLedger.PARTICIPANT,KafkaExpansionLedger.VALIDATE,KafkaExpansionRuntimeInspection.COMMAND)){var task=new Task();task.setCommand(command);assertThrows(IllegalArgumentException.class,()->ReflectionTestUtils.invokeMethod(deployment,"rejectBrokerAdditionTask",task));}}
    @Test void genericJobRetryCannotReopenSafelyCancelledExpansion(){var op=reserved();op.setState(KafkaExpansionState.CANCELLED_SAFE);var job=new Job();job.setId(op.getJobId());job.setType(JobType.QUORUM_EXPANSION);job.setStatus(JobStatus.FAILED);when(jobs.findForUpdateById(job.getId())).thenReturn(Optional.of(job));when(operations.findByJobId(job.getId())).thenReturn(Optional.of(op));assertThrows(IllegalStateException.class,()->service.retryJob(job.getId()));verifyNoInteractions(steps);verify(jobs,never()).save(any());}
}
