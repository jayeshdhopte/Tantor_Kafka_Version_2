package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExternalServiceJobDisplayTest {
    final DiscoveryAgentRepository agents=mock(DiscoveryAgentRepository.class);
    final ExternalDataServiceRepository services=mock(ExternalDataServiceRepository.class);
    final ObjectMapper mapper=new ObjectMapper();
    final ExternalDataServiceManager manager=new ExternalDataServiceManager(null,agents,services,null,null,null,mapper);

    ExternalDataService service() {
        var service=new ExternalDataService();service.setId(UUID.randomUUID());service.setAgentId("agent-id");
        service.setClusterId(UUID.randomUUID());service.setRole("schema_registry");
        service.setConfigJson("{\"service_name\":\"schema-registry-2\",\"rest_port\":\"8082\",\"host_name\":\"192.168.3.229\"}");
        return service;
    }
    @Test void snapshotsIdentityAndInstanceBeforeDeletionAndKeepsDispatchIds() throws Exception {
        var service=service();var agent=new DiscoveryAgent();agent.setClusterId(service.getClusterId());
        agent.setHostname("node229.translab.io");agent.setIpAddresses("[\"127.0.0.1\",\"192.168.3.229\"]");
        when(agents.findById(service.getAgentId())).thenReturn(Optional.of(agent));
        var payload=manager.serviceStepPayload(service,"CLEAN");
        var step=new JobStep();step.setId(UUID.randomUUID());step.setTargetId(service.getAgentId());
        step.setExternalServiceTaskId(UUID.randomUUID());step.setStatus(JobStepStatus.SUCCESS);
        step.setPayload(mapper.writeValueAsString(payload));step.setName("legacy UUID name");
        reset(agents,services);
        var view=manager.serviceHistoryView(step);
        assertThat(view.getName()).isEqualTo("Remove Schema Registry [schema-registry-2:8082] on node229.translab.io (192.168.3.229)");
        assertThat(view.getTargetId()).isEqualTo(step.getTargetId());
        assertThat(view.getExternalServiceTaskId()).isEqualTo(step.getExternalServiceTaskId());
        assertThat(view.getStatus()).isEqualTo(JobStepStatus.SUCCESS);
        assertThat(view.getPayload()).contains(service.getId().toString());
        assertThat(step.getName()).isEqualTo("legacy UUID name");
        verifyNoInteractions(agents,services);
    }
    @Test void enrichesLegacyHistoryWithoutRewritingExecutionPayload() {
        var service=service();when(services.findById(service.getId())).thenReturn(Optional.of(service));
        var step=new JobStep();step.setTargetId("agent-id");step.setPayload("{\"serviceId\":\""+service.getId()+"\",\"operation\":\"INSTALL\"}");
        assertThat(manager.serviceHistoryView(step).getName()).isEqualTo("Install and start Schema Registry [schema-registry-2:8082] on Hostname not reported (192.168.3.229)");
        assertThat(step.getPayload()).doesNotContain("hostname");verify(services,never()).save(any());
    }
    @Test void missingMetadataIsExplicitAndUnregisterStepIsUnchanged() {
        var service=service();service.setConfigJson("{}");
        assertThat(ExternalDataServiceManager.serviceStepName(manager.serviceStepPayload(service,"PRECHECK")))
                .contains("Hostname not reported (IP not reported)").doesNotContain("agent-id",service.getId().toString());
        var finish=new JobStep();finish.setPayload("{\"operation\":\"UNREGISTER\"}");
        assertThat(manager.serviceHistoryView(finish)).isSameAs(finish);
    }
    @Test void legacyUnitUuidIsHiddenOnlyInTheLabel() {
        var service=service();
        service.setConfigJson("{\"service_name\":\"schema-registry-"+service.getId()+"\",\"rest_port\":\"8082\"}");
        var payload=manager.serviceStepPayload(service,"CLEAN");
        assertThat(ExternalDataServiceManager.serviceStepName(payload)).contains("[schema-registry:8082]").doesNotContain(service.getId().toString());
        assertThat(payload.get("serviceName")).contains(service.getId().toString());
    }
}
