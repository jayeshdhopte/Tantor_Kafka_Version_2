package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.config.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class BrokerAdditionJobHandlerTest {
 @Test void ordinaryRegistrationDelayWaitsWithoutReplayingMutations(){
  var ledger=mock(BrokerAdditionLedger.class);var jobs=mock(JobService.class);var kafka=mock(BrokerAdditionKafka.class);var clusters=mock(ClusterRepository.class);var awaiter=mock(AgentTaskAwaiter.class);
  var mapper=new ObjectMapper().findAndRegisterModules();var handler=new BrokerAdditionJobHandler(ledger,jobs,mock(AddNodeReadinessService.class),kafka,mock(AddNodeArtifactCatalog.class),awaiter,clusters,mapper,new ArtifactRepositoryProperties(),new KafkaDeploymentProperties());
  var op=new BrokerAdditionOperation();op.setId(UUID.randomUUID());op.setClusterId(UUID.randomUUID());op.setHostId("candidate");op.setNodeId(4);
  var plan=mock(BrokerAdditionPlan.class);when(plan.kafkaClusterId()).thenReturn("cluster-id");var payload=new BrokerAdditionLedger.Payload(plan,"","digest","voters",new BrokerAdditionLedger.RuntimeOptions("","",0,100000));
  var cluster=new Cluster();cluster.setConfigJson("{}");when(clusters.findWithServicesById(op.getClusterId())).thenReturn(Optional.of(cluster));
  var step=new JobStep();step.setId(UUID.randomUUID());var task=new Task();task.setId(UUID.randomUUID());
  when(ledger.dispatch(op.getId(),step.getId(),"INSPECT_ADD_BROKER",BrokerAdditionState.VERIFYING)).thenReturn(task);
  when(awaiter.await(task.getId(),90)).thenAnswer(call->{task.setLogOutput(mapper.writeValueAsString(Map.of("schemaVersion","add-broker/v1","operationId",op.getId().toString(),"hostId","candidate","planDigest","digest","nodeId",4,"kafkaClusterId","cluster-id","observedAt",java.time.Instant.now().toString(),"phase","STARTED","storageState","MATCHING","serviceActive",true)));return task;});
  when(kafka.verify(eq(cluster),eq(payload),anyMap())).thenThrow(new BrokerAdditionKafka.RegistrationPending()).thenReturn("healthy");
  String result=org.springframework.test.util.ReflectionTestUtils.invokeMethod(handler,"verifyStable",op,payload,step);
  assertTrue(result.contains("Three successful observations"));verify(kafka,times(4)).verify(eq(cluster),eq(payload),anyMap());verify(ledger,times(3)).dispatch(op.getId(),step.getId(),"INSPECT_ADD_BROKER",BrokerAdditionState.VERIFYING);verifyNoMoreInteractions(ledger);
 }
 @Test void registrationWaitIsBoundedAndCannotPublishOnExpiry(){
  var ledger=mock(BrokerAdditionLedger.class);var jobs=mock(JobService.class);var kafka=mock(BrokerAdditionKafka.class);var clusters=mock(ClusterRepository.class);
  var handler=new BrokerAdditionJobHandler(ledger,jobs,mock(AddNodeReadinessService.class),kafka,mock(AddNodeArtifactCatalog.class),mock(AgentTaskAwaiter.class),clusters,new ObjectMapper(),new ArtifactRepositoryProperties(),new KafkaDeploymentProperties());
  org.springframework.test.util.ReflectionTestUtils.setField(handler,"registrationTimeoutSeconds",0);
  var op=new BrokerAdditionOperation();op.setClusterId(UUID.randomUUID());var cluster=new Cluster();cluster.setConfigJson("{}");when(clusters.findWithServicesById(op.getClusterId())).thenReturn(Optional.of(cluster));
  var payload=new BrokerAdditionLedger.Payload(mock(BrokerAdditionPlan.class),"","digest","voters",new BrokerAdditionLedger.RuntimeOptions("","",0,100000));when(kafka.verify(eq(cluster),eq(payload),anyMap())).thenThrow(new BrokerAdditionKafka.RegistrationPending());
  assertThrows(BrokerAdditionKafka.RegistrationPending.class,()->org.springframework.test.util.ReflectionTestUtils.invokeMethod(handler,"verifyStable",op,payload,new JobStep()));verifyNoInteractions(ledger);
 }
 @Test void failedLiveVerificationCannotPublishOrReleaseReservation(){
  var ledger=mock(BrokerAdditionLedger.class);var jobs=mock(JobService.class);var kafka=mock(BrokerAdditionKafka.class);var clusters=mock(ClusterRepository.class);
  var handler=new BrokerAdditionJobHandler(ledger,jobs,mock(AddNodeReadinessService.class),kafka,mock(AddNodeArtifactCatalog.class),mock(AgentTaskAwaiter.class),clusters,new ObjectMapper(),new ArtifactRepositoryProperties(),new KafkaDeploymentProperties());
  var job=new Job();job.setId(UUID.randomUUID());var op=new BrokerAdditionOperation();op.setId(UUID.randomUUID());op.setJobId(job.getId());op.setClusterId(UUID.randomUUID());op.setState(BrokerAdditionState.VERIFYING);when(ledger.forJob(job.getId())).thenReturn(op);
  when(ledger.payload(op)).thenReturn(new BrokerAdditionLedger.Payload(mock(BrokerAdditionPlan.class),"","digest","voters",new BrokerAdditionLedger.RuntimeOptions("","",0,100000)));
  var steps=new ArrayList<JobStep>();for(int i=0;i<6;i++){var step=new JobStep();step.setId(UUID.randomUUID());step.setStatus(i<4?JobStepStatus.SUCCESS:JobStepStatus.PENDING);steps.add(step);}when(jobs.getSteps(job.getId())).thenReturn(steps);
  var cluster=new Cluster();cluster.setConfigJson("{}");when(clusters.findWithServicesById(op.getClusterId())).thenReturn(Optional.of(cluster));when(kafka.verify(eq(cluster),any(),anyMap())).thenThrow(new IllegalStateException("candidate still fenced"));
  assertThrows(IllegalStateException.class,()->handler.execute(job));verify(ledger).recovery(eq(op.getId()),contains("candidate still fenced"));verify(ledger,never()).publish(any(),anyString());assertTrue(op.getState().reserved());assertFalse(handler.supports(JobType.ROLLING_RESTART));assertFalse(handler.supports(JobType.DEPLOYMENT));assertFalse(handler.supports(JobType.EXTERNAL_SERVICES));
 }
}
