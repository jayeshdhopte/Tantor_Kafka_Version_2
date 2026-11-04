package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class ExternalServiceReadinessLifecycleTest {
    final JobService jobs=mock(JobService.class);final ExternalDataServiceManager manager=mock(ExternalDataServiceManager.class);
    final ExternalDataServiceRepository services=mock(ExternalDataServiceRepository.class);final ExternalServiceTaskRepository tasks=mock(ExternalServiceTaskRepository.class);
    final DataServiceConnectionService connections=mock(DataServiceConnectionService.class);final AddServiceMappingService mapping=mock(AddServiceMappingService.class);
    final DataServiceReadiness readiness=mock(DataServiceReadiness.class);
    final ExternalServicesJobHandler handler=new ExternalServicesJobHandler(jobs,manager,services,tasks,mock(KafkaAdminService.class),connections,mapping,readiness);
    final Job job=new Job();final JobStep step=new JobStep();final ExternalDataService service=new ExternalDataService();
    ExternalServiceReadinessLifecycleTest(){
        job.setId(UUID.randomUUID());job.setRequestedBy("requester");step.setId(UUID.randomUUID());step.setStatus(JobStepStatus.PENDING);step.setExternalServiceTaskId(UUID.randomUUID());step.setPayload("step");
        service.setId(UUID.randomUUID());service.setClusterId(UUID.randomUUID());service.setAgentId("agent");service.setRole("kafka_connect");service.setConfigJson("config");
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        when(manager.read("step")).thenReturn(Map.of("serviceId",service.getId().toString(),"operation","INSTALL"));
        when(manager.read("config")).thenReturn(Map.of("rest_port","8084","kafka_cluster_id","native-cluster"));
        when(services.findById(service.getId())).thenReturn(Optional.of(service));var agent=new DiscoveryAgent();when(manager.requireAgent(any(),any())).thenReturn(agent);when(manager.serviceHost(agent)).thenReturn("192.168.3.229");
    }
    void failedTask(String message){var task=new ExternalServiceTask();task.setStatus("FAILED");task.setMessage(message);when(tasks.findById(step.getExternalServiceTaskId())).thenReturn(Optional.of(task));}
    @Test void delayedStartupCanRecoverWithoutReinstallingOrMarkingOnlineBeforeSave(){
        failedTask("kafka-connect verification failed: connection refused (context deadline exceeded)");handler.execute(job);
        verify(readiness).await("kafka_connect","192.168.3.229",8084,"native-cluster");
        verify(manager,never()).enqueue(any(),anyString());verify(jobs).completeStep(eq(step.getId()),contains("readiness subsequently confirmed"));
        assertThat(service.getStatus()).isEqualTo("DEPLOYING");verify(connections,never()).saveConnection(any(),any(),any(),any());
    }
    @Test void realInstallFailuresRemainFailedWithoutReadinessOverride(){
        failedTask("checksum mismatch");assertThatThrownBy(()->handler.execute(job)).hasMessageContaining("checksum mismatch");
        assertThat(service.getStatus()).isEqualTo("FAILED");verifyNoInteractions(readiness);verify(jobs).failStep(eq(step.getId()),contains("checksum mismatch"));
    }
    @Test void permanentStartupFailureNeverBecomesOnlineOrCreatesAConnection(){
        failedTask("kafka-connect verification failed: connection refused (context deadline exceeded)");
        doThrow(new IllegalStateException("Readiness timeout on port 8084")).when(readiness).await(any(),any(),anyInt(),any());
        assertThatThrownBy(()->handler.execute(job)).hasMessageContaining("8084");assertThat(service.getStatus()).isEqualTo("FAILED");verifyNoInteractions(connections);
    }
    @Test void saveWaitsForReadinessBeforePersistingAConnection(){
        when(manager.read("step")).thenReturn(Map.of("serviceId",service.getId().toString(),"operation","SAVE"));
        doThrow(new IllegalStateException("Readiness timeout")).when(readiness).await(any(),any(),anyInt(),any());
        assertThatThrownBy(()->handler.execute(job)).hasMessageContaining("Readiness timeout");verifyNoInteractions(connections);
    }
}
