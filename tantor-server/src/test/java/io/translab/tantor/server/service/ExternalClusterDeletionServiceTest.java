package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ExternalClusterDeletionServiceTest {
    final JobService jobs=mock(JobService.class);
    final ExternalDataServiceRepository services=mock(ExternalDataServiceRepository.class);
    final ExternalServiceTaskRepository tasks=mock(ExternalServiceTaskRepository.class);
    final ExternalDataServiceManager manager=mock(ExternalDataServiceManager.class);
    final ExternalClusterService external=mock(ExternalClusterService.class);
    final ExternalClusterRepository clusters=mock(ExternalClusterRepository.class);
    final ClusterRepository mirrors=mock(ClusterRepository.class);
    final ObjectMapper mapper=new ObjectMapper();
    final ExternalClusterDeletionService deletion=new ExternalClusterDeletionService(jobs,services,tasks,manager,external,clusters,mirrors,mapper);
    final UUID id=UUID.randomUUID();
    final ExternalCluster cluster=new ExternalCluster();
    @BeforeEach void setup() throws Exception {
        cluster.setId(id);cluster.setName("external");cluster.setStatus("SUCCESS");
        when(clusters.findById(id)).thenReturn(Optional.of(cluster));
        when(manager.read(anyString())).thenAnswer(call->mapper.readValue(call.getArgument(0,String.class),new com.fasterxml.jackson.core.type.TypeReference<Map<String,String>>(){}));
        when(jobs.createJob(any(),anyList())).thenAnswer(call->{Job job=call.getArgument(0);job.setId(UUID.randomUUID());return job;});
    }
    ExternalDataService service() {
        var service=new ExternalDataService();service.setId(UUID.randomUUID());service.setClusterId(id);service.setAgentId("external-host");service.setRole("schema_registry");service.setStatus("ONLINE");service.setConfigJson("{}");
        return service;
    }
    JobStep step(ExternalDataService service) {
        JobStep step=new JobStep();step.setId(UUID.randomUUID());step.setTargetId(service.getAgentId());step.setName("Remove owned schema");step.setPayload("{\"serviceId\":\""+service.getId()+"\"}");step.setStatus(JobStepStatus.PENDING);step.setRetryCount(0);
        when(services.findById(service.getId())).thenReturn(Optional.of(service));return step;
    }
    JobStep finish() {JobStep step=new JobStep();step.setId(UUID.randomUUID());step.setTargetId(id.toString());step.setPayload("{\"operation\":\"UNREGISTER\"}");step.setStatus(JobStepStatus.PENDING);return step;}
    Job job() {Job job=new Job();job.setId(UUID.randomUUID());return job;}
    ExternalServiceTask task(ExternalDataService service,String status) {var task=new ExternalServiceTask();task.setId(UUID.randomUUID());task.setServiceId(service.getId());task.setAgentId(service.getAgentId());task.setCommand("CLEAN");task.setStatus(status);task.setMessage("verified cleanup");when(tasks.findById(task.getId())).thenReturn(Optional.of(task));return task;}

    @Test void queuesOnlyOwnedInstancesAndUnregistersLast() {
        var first=service();var second=service();second.setRole("kafka_connect");var deleted=service();deleted.setStatus("DELETED");
        when(services.findByClusterId(id)).thenReturn(List.of(first,second,deleted));
        assertThat(deletion.queue(cluster,"operator",true)).isNotNull();
        verify(jobs).createJob(argThat(job->job.getType()==JobType.CLUSTER_DELETION && job.getPayload().contains("externalCleanup") && !Boolean.TRUE.equals(job.getRollbackSupported())),argThat(steps->steps.size()==3 && steps.getLast().getName().startsWith("Unregister")));
        assertThat(cluster.getStatus()).isEqualTo("DELETING");verifyNoInteractions(external,tasks);
    }
    @Test void noOwnedServicesLeavesExistingUnregisterFlow() {
        when(services.findByClusterId(id)).thenReturn(List.of());
        assertThat(deletion.queue(cluster,"operator",false)).isNull();verify(jobs,never()).createJob(any(),anyList());
    }
    @Test void successfulCleanupFinishesBeforeRemovingRegistration() {
        var service=service();var step=step(service);var finish=finish();var job=job();var task=task(service,"SUCCESS");
        when(manager.enqueue(service,"CLEAN")).thenReturn(task);when(jobs.getSteps(job.getId())).thenReturn(List.of(step,finish));
        assertThat(deletion.execute(job,id)).isEmpty();
        var order=inOrder(services,external,jobs);order.verify(services).save(argThat(s->"DELETED".equals(s.getStatus())));order.verify(jobs).completeStep(step.getId(),"verified cleanup");order.verify(external).deleteExternalCluster(id);order.verify(jobs).completeStep(eq(finish.getId()),contains("Existing Kafka"));
    }
    @Test void timeoutRetainsRegistrationAndSuccessfulStepsForRetry() {
        var service=service();var step=step(service);var done=new JobStep();done.setStatus(JobStepStatus.SUCCESS);var job=job();var task=task(service,"IN_PROGRESS");task.setDeadline(Instant.now().minusSeconds(1));
        step.setExternalServiceTaskId(task.getId());when(jobs.getSteps(job.getId())).thenReturn(List.of(done,step,finish()));
        assertThat(deletion.execute(job,id)).hasSize(1);verify(manager).expire(task.getId());verify(manager,never()).enqueue(any(),anyString());verifyNoInteractions(external);assertThat(cluster.getStatus()).isEqualTo("DELETE_FAILED");
    }
    @Test void explicitRetryRequeuesOnlyFailedOwnedTask() {
        var service=service();var step=step(service);step.setRetryCount(1);var failed=task(service,"FAILED");step.setExternalServiceTaskId(failed.getId());var replacement=task(service,"SUCCESS");var job=job();
        when(manager.enqueue(service,"CLEAN")).thenReturn(replacement);when(jobs.getSteps(job.getId())).thenReturn(List.of(step,finish()));
        assertThat(deletion.execute(job,id)).isEmpty();verify(jobs).attachExternalServiceTask(step.getId(),replacement.getId());
    }
    @Test void reassignedDeploymentCannotBeDeleted() {
        var service=service();var step=step(service);service.setClusterId(UUID.randomUUID());var job=job();when(jobs.getSteps(job.getId())).thenReturn(List.of(step,finish()));
        assertThat(deletion.execute(job,id)).hasSize(1);verify(manager,never()).enqueue(any(),anyString());verifyNoInteractions(external);
    }
}
