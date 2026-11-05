package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class BrokerAdditionLedgerTest {
 final BrokerAdditionOperationRepository ops=mock(BrokerAdditionOperationRepository.class);
 final ClusterRepository clusters=mock(ClusterRepository.class);final HostRepository hosts=mock(HostRepository.class);
 final ClusterServiceAssignmentRepository assignments=mock(ClusterServiceAssignmentRepository.class);
 final JobStepRepository steps=mock(JobStepRepository.class);final TaskRepository tasks=mock(TaskRepository.class);
 final JobService jobs=mock(JobService.class);final BrokerAdditionPlanningService planning=mock(BrokerAdditionPlanningService.class);final JdbcTemplate jdbc=mock(JdbcTemplate.class);
 final BrokerAdditionLedger ledger=new BrokerAdditionLedger(ops,clusters,hosts,assignments,steps,tasks,jobs,planning,new ObjectMapper(),jdbc);
 BrokerAdditionOperation operation(){var o=new BrokerAdditionOperation();o.setId(UUID.randomUUID());o.setClusterId(UUID.randomUUID());o.setHostId("candidate");o.setJobId(UUID.randomUUID());o.setNodeId(4);o.setState(BrokerAdditionState.RECOVERY_REQUIRED);when(ops.findById(o.getId())).thenReturn(Optional.of(o));when(ops.findForUpdate(o.getId())).thenReturn(Optional.of(o));return o;}
 @Test void disabledExecutionCannotReserveOrSchedule(){assertThrows(org.springframework.web.server.ResponseStatusException.class,()->ledger.accept(UUID.randomUUID(),UUID.randomUUID(),"a".repeat(64),"request-key-123456"));verifyNoInteractions(ops,clusters,tasks,planning);}
 @Test void repeatedRequestReturnsOriginalRecoveryOperation(){var o=operation();UUID preview=UUID.randomUUID();String context="a".repeat(64);o.setRequestDigest(BrokerAdditionLedger.hash(preview+":"+context));when(clusters.findForServiceUpdate(o.getClusterId())).thenReturn(Optional.of(new Cluster()));when(ops.findByClusterIdAndRequestKey(o.getClusterId(),"request-key-123456")).thenReturn(Optional.of(o));ReflectionTestUtils.setField(ledger,"executionEnabled",true);assertEquals(o.getJobId(),ledger.accept(o.getClusterId(),preview,context,"request-key-123456").jobId());verifyNoInteractions(planning,tasks,hosts);}
 @Test void reusedKeyCannotChangeInputs(){var o=operation();o.setRequestDigest("old");when(clusters.findForServiceUpdate(o.getClusterId())).thenReturn(Optional.of(new Cluster()));when(ops.findByClusterIdAndRequestKey(o.getClusterId(),"request-key-123456")).thenReturn(Optional.of(o));ReflectionTestUtils.setField(ledger,"executionEnabled",true);assertThrows(org.springframework.web.server.ResponseStatusException.class,()->ledger.accept(o.getClusterId(),UUID.randomUUID(),"a".repeat(64),"request-key-123456"));verifyNoInteractions(planning,tasks);}
 @Test void pendingActionIsNotReplaced(){var o=operation();var job=new Job();job.setId(o.getJobId());var s=new JobStep();s.setId(UUID.randomUUID());s.setJob(job);s.setAgentTaskId(UUID.randomUUID());when(steps.findById(s.getId())).thenReturn(Optional.of(s));var t=new Task();t.setId(s.getAgentTaskId());t.setStatus("IN_PROGRESS");t.setCommand("FORMAT_ADD_BROKER");when(tasks.findById(t.getId())).thenReturn(Optional.of(t));assertSame(t,ledger.dispatch(o.getId(),s.getId(),"INSPECT_ADD_BROKER",BrokerAdditionState.RECOVERY_REQUIRED));verify(tasks,never()).saveAndFlush(any());verifyNoInteractions(jdbc);}
 @Test void failedMutationCannotBeRetriedWithoutInspection(){var o=operation();var job=new Job();job.setId(o.getJobId());var s=new JobStep();s.setId(UUID.randomUUID());s.setJob(job);s.setAgentTaskId(UUID.randomUUID());when(steps.findById(s.getId())).thenReturn(Optional.of(s));var t=new Task();t.setStatus("FAILED");t.setCommand("FORMAT_ADD_BROKER");when(tasks.findById(s.getAgentTaskId())).thenReturn(Optional.of(t));assertThrows(IllegalStateException.class,()->ledger.dispatch(o.getId(),s.getId(),"FORMAT_ADD_BROKER",BrokerAdditionState.FORMATTING));verify(tasks,never()).saveAndFlush(any());}
 @Test void publicationCannotSkipVerification(){var o=operation();o.setState(BrokerAdditionState.PREPARED);assertThrows(IllegalStateException.class,()->ledger.publish(o.getId(),"not proof"));verifyNoInteractions(hosts,assignments);}
 @Test void cancellationCannotReleaseAfterMutationDispatch(){var o=operation();when(clusters.findForServiceUpdate(o.getClusterId())).thenReturn(Optional.of(new Cluster()));when(jdbc.queryForObject(anyString(),eq(Integer.class),eq(o.getId()))).thenReturn(1);assertThrows(org.springframework.web.server.ResponseStatusException.class,()->ledger.cancelSafe(o.getClusterId(),o.getId()));assertTrue(o.getState().reserved());verify(ops,never()).saveAndFlush(any());}
}
