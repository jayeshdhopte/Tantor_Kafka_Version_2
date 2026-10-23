package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.time.OffsetDateTime;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class ExternalCleanupCapabilityTest {
 @Test void requiresUpdatedAgentBeforeDispatchingDestructiveCleanup() {
  var agents=mock(DiscoveryAgentRepository.class);var tasks=mock(ExternalServiceTaskRepository.class);
  var manager=new ExternalDataServiceManager(mock(ExternalClusterRepository.class),agents,mock(ExternalDataServiceRepository.class),tasks,mock(JobService.class),mock(ExternalServiceArtifacts.class),new ObjectMapper());
  UUID cluster=UUID.randomUUID();var service=new ExternalDataService();service.setId(UUID.randomUUID());service.setClusterId(cluster);service.setAgentId("agent");
  var agent=new DiscoveryAgent();agent.setId("agent");agent.setClusterId(cluster);agent.setStatus("ONLINE");agent.setLastHeartbeat(OffsetDateTime.now());agent.setCanDeployServices(true);agent.setVersion("3.4.0-dataservices.2-paths.1");
  when(agents.findById("agent")).thenReturn(Optional.of(agent));
  assertThatThrownBy(()->manager.enqueue(service,"CLEAN")).hasMessageContaining("Update the external agent");verifyNoInteractions(tasks);
  agent.setVersion("3.4.0-dataservices.2-paths.1-cleanup.1-plugins.1");
  when(tasks.saveAndFlush(any())).thenAnswer(call->call.getArgument(0));
  assertThat(manager.enqueue(service,"CLEAN").getCommand()).isEqualTo("CLEAN");
  assertThatThrownBy(()->manager.enqueue(service,"DELETE_KAFKA")).hasMessageContaining("Unsupported");
 }
}
