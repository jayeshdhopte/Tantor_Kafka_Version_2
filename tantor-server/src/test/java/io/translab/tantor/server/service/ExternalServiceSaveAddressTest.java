package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.dto.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class ExternalServiceSaveAddressTest {
 @Test void retrySaveUsesCurrentAgentIpAndPreservesOldConnectionName(){
  var jobs=mock(JobService.class);var manager=mock(ExternalDataServiceManager.class);
  var services=mock(ExternalDataServiceRepository.class);var connections=mock(DataServiceConnectionService.class);
  var handler=new ExternalServicesJobHandler(jobs,manager,services,mock(ExternalServiceTaskRepository.class),mock(KafkaAdminService.class),connections);
  UUID cluster=UUID.randomUUID(),serviceId=UUID.randomUUID();
  var job=new Job();job.setId(UUID.randomUUID());job.setRequestedBy("operator");
  var step=new JobStep();step.setId(UUID.randomUUID());step.setStatus(JobStepStatus.FAILED);step.setPayload("step");
  when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
  when(manager.read("step")).thenReturn(Map.of("serviceId",serviceId.toString(),"operation","SAVE"));
  var service=new ExternalDataService();service.setId(serviceId);service.setClusterId(cluster);service.setAgentId("agent");service.setRole("schema_registry");service.setConfigJson("config");
  when(services.findById(serviceId)).thenReturn(Optional.of(service));
  when(manager.read("config")).thenReturn(Map.of("host_name","node174.translab.io","rest_port","8088"));
  var agent=new DiscoveryAgent();when(manager.requireAgent(cluster,"agent")).thenReturn(agent);when(manager.serviceHost(agent)).thenReturn("192.168.3.174");
  when(connections.saveConnection(eq(cluster),eq("SCHEMA_REGISTRY"),any(),eq("operator"))).thenReturn(ConnectionResponse.builder().status("ONLINE").build());
  handler.execute(job);
  verify(connections).saveConnection(eq(cluster),eq("SCHEMA_REGISTRY"),argThat(r->r.getIsDefault()==null&&"192.168.3.174".equals(r.getHost())&&r.getPort()==8088&&"Deployed schema_registry on node174.translab.io:8088".equals(r.getConnectionName())),eq("operator"));
  verify(jobs).completeStep(eq(step.getId()),anyString());
  verify(manager,never()).enqueue(any(),anyString());
 }
}
