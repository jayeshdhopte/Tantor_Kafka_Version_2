package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.time.OffsetDateTime;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExternalServiceDirectoriesTest {
    @Test void reportsDistinctInstalledInstancesAndPreservesPathsWhenAgentIsOfflineOrSaveFailed() {
        var services = mock(ExternalDataServiceRepository.class);
        var agents = mock(DiscoveryAgentRepository.class);
        var tasks = mock(ExternalServiceTaskRepository.class);
        var overview = new ClusterOverviewService(null,null,null,null,null,services,agents,tasks);
        UUID cluster = UUID.randomUUID();
        var first = service("schema_registry","ONLINE","/client/tantor/schema-registry");
        var second = service("schema_registry","FAILED","/client/tantor/schema-registry-2");
        var connect = service("kafka_connect","ONLINE","/client/tantor/connect");
        var pending = service("kafka_connect","PENDING","/never-installed");
        var deleted = service("schema_registry","DELETED","/deleted");
        when(services.findByClusterId(cluster)).thenReturn(List.of(first,second,connect,pending,deleted));
        when(tasks.existsByServiceIdAndCommandAndStatus(second.getId(),"INSTALL","SUCCESS")).thenReturn(true);
        var agent = new DiscoveryAgent(); agent.setId("agent");agent.setHostname("node229");agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());
        when(agents.findById("agent")).thenReturn(Optional.of(agent));
        var rows = overview.externalDataServicePaths(cluster);
        assertThat(rows).hasSize(3);
        assertThat(rows).extracting(r->r.getInstanceId()).doesNotHaveDuplicates();
        var schema = rows.stream().filter(r->r.getInstanceId().equals(first.getId().toString())).findFirst().orElseThrow();
        assertThat(schema.getInstallDir()).isEqualTo("/client/tantor/schema-registry");
        assertThat(schema.getConfigFile()).isEqualTo("/client/tantor/schema-registry/etc/schema-registry.properties");
        assertThat(schema.getPathSource()).isEqualTo("DEPLOYMENT_CONFIGURATION");
        assertThat(schema.getAgentStatus()).isEqualTo("ONLINE");
        var worker = rows.stream().filter(r->r.getServiceType().equals("KAFKA_CONNECT")).findFirst().orElseThrow();
        assertThat(worker.getPluginDir()).isEqualTo("/client/tantor/connect/plugins");
        agent.setLastHeartbeat(OffsetDateTime.now().minusMinutes(5));
        assertThat(overview.externalDataServicePaths(cluster)).allSatisfy(r->assertThat(r.getAgentStatus()).isEqualTo("OFFLINE"));
        assertThat(overview.externalDataServicePaths(cluster)).extracting(r->r.getInstallDir())
                .contains("/client/tantor/schema-registry-2").doesNotContain("/never-installed","/deleted");
    }
    private ExternalDataService service(String role,String status,String install) {
        var s = new ExternalDataService(); s.setId(UUID.randomUUID());s.setAgentId("agent");s.setRole(role);s.setStatus(status);
        s.setConfigJson("{\"install_dir\":\""+install+"\",\"config_dir\":\""+install+"/etc\",\"plugin_dir\":\""+install+"/plugins\",\"host_name\":\"192.168.3.229\",\"service_name\":\""+role+"\"}");
        return s;
    }
}
