package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.audit.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
class ClusterDeletionJobHandlerTest {
 private final JobService jobs=mock(JobService.class);private final TaskRepository tasks=mock(TaskRepository.class);private final HostRepository hosts=mock(HostRepository.class);private final AgentTaskAwaiter awaiter=mock(AgentTaskAwaiter.class);private final AuditService audit=mock(AuditService.class);private final AuditLogRepository auditLogs=mock(AuditLogRepository.class);
 private final ClusterDeletionJobHandler handler=new ClusterDeletionJobHandler(jobs,tasks,hosts,awaiter,audit,auditLogs,new ObjectMapper());
 private final UUID cluster=UUID.randomUUID();private final Job job=job();
 @Test void waitsForEveryHostAndRecordsCompletionAsOriginalActor() {
  JobStep first=step("host-1");JobStep second=step("host-2");when(jobs.getSteps(job.getId())).thenReturn(List.of(first,second));
  success(first);success(second);handler.execute(job);
  var ordered=inOrder(awaiter,audit);ordered.verify(awaiter).await(first.getAgentTaskId());ordered.verify(awaiter).await(second.getAgentTaskId());
  ordered.verify(audit).recordAs(eq("operator"),eq("MANAGEMENT_SERVER"),isNull(),eq("CLUSTER_CHANGE"),eq("CLUSTER_DELETED"),eq("CLUSTER"),eq(cluster.toString()),eq(cluster),eq("SUCCESS"),isNull(),isNull(),isNull(),argThat(details->details.toString().contains("removed-cluster")));
 }
 @Test void failedCleanupIsAuditedAndOtherHostsStillFinishWithoutAutomaticRedispatch() {
  JobStep first=step("host-1");JobStep second=step("host-2");when(jobs.getSteps(job.getId())).thenReturn(List.of(first,second));
  Task failed=task(first,"FAILED");failed.setErrorMsg("disk permission denied");when(tasks.findById(first.getAgentTaskId())).thenReturn(Optional.of(failed));success(second);
  assertThatThrownBy(()->handler.execute(job)).hasMessageContaining("1 host");verify(tasks,never()).save(any());verify(jobs).completeStep(eq(second.getId()),any());
  verify(audit).recordAs(eq("operator"),anyString(),isNull(),anyString(),eq("CLUSTER_DELETION_FAILED"),anyString(),anyString(),eq(cluster),eq("FAILED"),isNull(),isNull(),isNull(),any());
 }
 @Test void retryPreservesSuccessAndRefusesCleanupOnAReassignedHost() {
  JobStep completed=step("done-host");completed.setStatus(JobStepStatus.SUCCESS);JobStep failed=step("reused-host");failed.setRetryCount(1);when(jobs.getSteps(job.getId())).thenReturn(List.of(completed,failed));when(tasks.findById(failed.getAgentTaskId())).thenReturn(Optional.of(task(failed,"FAILED")));
  Host reassigned=new Host();reassigned.setClusterId(UUID.randomUUID());when(hosts.findById("reused-host")).thenReturn(Optional.of(reassigned));
  assertThatThrownBy(()->handler.execute(job)).hasMessageContaining("cleanup failed");verify(tasks,never()).save(any());verify(awaiter,never()).await(any());verify(jobs).failStep(eq(failed.getId()),contains("another cluster"));
 }
 private void success(JobStep step){Task task=task(step,"SUCCESS");task.setLogOutput("Cleanup verified");when(tasks.findById(step.getAgentTaskId())).thenReturn(Optional.of(task));when(awaiter.await(step.getAgentTaskId())).thenReturn(task);}
 private Task task(JobStep step,String status){Task task=new Task();task.setId(step.getAgentTaskId());task.setHostId(step.getTargetId());task.setCommand("DELETE_CLUSTER");task.setStatus(status);task.setParameters("{}");return task;}
 private JobStep step(String host){JobStep step=new JobStep();step.setId(UUID.randomUUID());step.setAgentTaskId(UUID.randomUUID());step.setTargetId(host);step.setName("Clean "+host);return step;}
 private Job job(){Job job=new Job();job.setId(UUID.randomUUID());job.setRequestedBy("operator");job.setPayload("{\"clusterId\":\""+cluster+"\",\"clusterName\":\"removed-cluster\"}");return job;}
 @Test void explicitRetryClonesFailedTaskParametersAndKeepsSuccessfulHosts() {
  JobStep done=step("done");done.setStatus(JobStepStatus.SUCCESS);JobStep retry=step("retry-host");retry.setRetryCount(1);Task original=task(retry,"FAILED");original.setParameters("{\"cleanup_kafka\":\"false\",\"data_services\":\"schema-only\"}");
  when(jobs.getSteps(job.getId())).thenReturn(List.of(done,retry));when(tasks.findById(retry.getAgentTaskId())).thenReturn(Optional.of(original));UUID newId=UUID.randomUUID();
  when(tasks.save(any())).thenAnswer(call->{Task replacement=call.getArgument(0);replacement.setId(newId);return replacement;});Task result=new Task();result.setStatus("SUCCESS");result.setLogOutput("Retried cleanup completed");when(awaiter.await(newId)).thenReturn(result);
  handler.execute(job);verify(tasks).save(argThat(replacement->"retry-host".equals(replacement.getHostId())&&original.getParameters().equals(replacement.getParameters())));verify(jobs).attachAgentTask(retry.getId(),newId);verify(awaiter,never()).await(done.getAgentTaskId());
 }
}
