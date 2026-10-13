package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.HostRepository;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
class ClusterDeletionHistoryServiceTest {
 @Test void groupsRealCleanupTasksByHostAndRetainsClusterDetails() {
  var jobs=mock(JobService.class);var deployment=mock(DeploymentService.class);var hosts=mock(HostRepository.class);var service=new ClusterDeletionHistoryService(jobs,deployment,hosts,new ObjectMapper());
  when(jobs.createJob(any(),anyList())).thenAnswer(call->{Job job=call.getArgument(0);job.setId(UUID.randomUUID());for(JobStep step:call.<List<JobStep>>getArgument(1))step.setId(UUID.randomUUID());return job;});
  when(deployment.deleteClusterFromHost(any(),anyString(),anyString(),anyString(),anyList(),anyBoolean())).thenReturn(UUID.randomUUID());
  Cluster cluster=new Cluster();cluster.setId(UUID.randomUUID());cluster.setName("retain-this-name");cluster.setMode("kraft");cluster.setKafkaVersion("4.3.0");cluster.setConfigJson("{}");
  var broker=node("host-1","broker",1);var controller=node("host-1","controller",101);var schema=node("host-1","schema_registry",null);schema.setConfigJson("{\"install_dir\":\"/opt/kafka\"}");cluster.setServices(List.of(broker,controller,schema,node("host-2","broker",2)));
  service.queue(cluster,"operator",false);
  verify(deployment,times(2)).deleteClusterFromHost(eq(cluster.getId()),anyString(),eq("4.3.0"),eq("{}"),anyList(),eq(true));
  verify(deployment).deleteClusterFromHost(eq(cluster.getId()),eq("host-1"),eq("4.3.0"),eq("{}"),argThat(values->values.size()==1 && "schema_registry".equals(values.getFirst().get("kind"))),eq(true));
  verify(jobs).createJob(argThat(job->job.getPayload().contains("retain-this-name")&&"operator".equals(job.getRequestedBy())&&job.getType()==JobType.CLUSTER_DELETION),argThat(steps->steps.size()==2));
  verify(jobs,times(2)).attachAgentTask(any(),any());
 }
 @Test void externalRemovalHasCompletedTimestampsWithoutDispatchingHostCleanup() {
  var jobs=mock(JobService.class);var deployment=mock(DeploymentService.class);var service=new ClusterDeletionHistoryService(jobs,deployment,mock(HostRepository.class),new ObjectMapper());
  when(jobs.createJob(any(),anyList())).thenAnswer(call->{Job job=call.getArgument(0);job.setId(UUID.randomUUID());return job;});
  service.completed(UUID.randomUUID(),"external-name","kafka-id","4.3.0","EXTERNAL","operator",false,true);
  verifyNoInteractions(deployment);verify(jobs).createJob(argThat(job->job.getStatus()==JobStatus.SUCCESS&&job.getEndTime()!=null&&job.getStartTime()!=null&&job.getPayload().contains("external-name")),argThat(steps->steps.getFirst().getStatus()==JobStepStatus.SUCCESS&&steps.getFirst().getLogs().contains("not stopped")));
 }
 private ClusterServiceAssignment node(String host,String role,Integer id){var node=new ClusterServiceAssignment();node.setHostId(host);node.setRole(role);node.setNodeId(id);return node;}
}
