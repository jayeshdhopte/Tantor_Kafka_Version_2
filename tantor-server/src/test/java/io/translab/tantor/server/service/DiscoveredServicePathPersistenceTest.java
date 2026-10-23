package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.DiscoveryAgent;
import io.translab.tantor.server.dto.DiscoveredServicePath;
import io.translab.tantor.server.repository.DiscoveryAgentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DiscoveredServicePathPersistenceTest {
    @Test void persistsReportsClearsConfirmedEmptyInventoryAndKeepsSnapshotForOldAgents() {
        var service = mock(ExternalClusterService.class,CALLS_REAL_METHODS);
        var agents = mock(DiscoveryAgentRepository.class);
        ReflectionTestUtils.setField(service,"discoveryAgentRepository",agents);
        ReflectionTestUtils.setField(service,"objectMapper",new ObjectMapper());
        var agent = new DiscoveryAgent();agent.setId("external-test");
        when(agents.findById("external-test")).thenReturn(Optional.of(agent));
        var report = new ExternalClusterService.ExternalDiscoveryReport();
        report.setHostId("external-test");report.setHostname("node229");report.setBootstrapServers("node229:9092");
        report.setDataServicePaths(List.of(new DiscoveredServicePath("SCHEMA_REGISTRY","schema-registry",8081,
                "/custom/install","/custom/config/schema.properties",null,"/custom/work","/custom/log")));
        ReflectionTestUtils.invokeMethod(service,"upsertDiscoveryAgent",report,null);
        assertThat(agent.getDataServicePathsJson()).contains("/custom/install");
        assertThat(agent.getServicePathsReportedAt()).isNotNull();
        report.setDataServicePaths(null);
        ReflectionTestUtils.invokeMethod(service,"upsertDiscoveryAgent",report,null);
        assertThat(agent.getDataServicePathsJson()).contains("/custom/install");
        report.setDataServicePaths(List.of());
        ReflectionTestUtils.invokeMethod(service,"upsertDiscoveryAgent",report,null);
        assertThat(agent.getDataServicePathsJson()).isEqualTo("[]");
    }
}
