package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.OffsetDateTime;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExternalInternalServiceCollisionTest {
    final ExternalClusterRepository clusters=mock(ExternalClusterRepository.class);
    final DiscoveryAgentRepository agents=mock(DiscoveryAgentRepository.class);
    final ExternalDataServiceRepository services=mock(ExternalDataServiceRepository.class);
    final ClusterServiceAssignmentRepository internal=mock(ClusterServiceAssignmentRepository.class);
    final JobService jobs=mock(JobService.class);
    final ExternalServiceArtifacts artifacts=mock(ExternalServiceArtifacts.class);
    final ExternalDataServiceManager manager=new ExternalDataServiceManager(clusters,agents,services,
            mock(ExternalServiceTaskRepository.class),jobs,artifacts,new ObjectMapper(),mock(AddServiceMappingService.class),internal);
    final ExternalCluster cluster=new ExternalCluster();
    final DiscoveryAgent agent=new DiscoveryAgent();
    final List<ExternalDataService> saved=new ArrayList<>();

    @BeforeEach void setup() {
        cluster.setId(UUID.randomUUID());cluster.setStatus("SUCCESS");cluster.setSecurityProtocol("PLAINTEXT");
        cluster.setBootstrapServers("192.168.3.229:9092");cluster.setBrokerCount(2);
        when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));
        when(clusters.findForServiceUpdate(cluster.getId())).thenReturn(Optional.of(cluster));
        agent.setId("external-agent-id");agent.setHostname("node229");agent.setIpAddresses("[\"192.168.3.229\"]");
        agent.setClusterId(cluster.getId());agent.setVersion("3.4.0-schema.1");agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());agent.setCanDeployServices(true);
        when(agents.findById(agent.getId())).thenReturn(Optional.of(agent));
        when(artifacts.verifiedChecksum(any(),any())).thenReturn("a".repeat(64));
        when(services.saveAndFlush(any())).thenAnswer(i->{var s=(ExternalDataService)i.getArgument(0);s.setId(UUID.randomUUID());saved.add(s);return s;});
        when(jobs.createJob(any(),anyList())).thenAnswer(i->{var j=(Job)i.getArgument(0);j.setId(UUID.randomUUID());return j;});
    }

    ClusterServiceAssignment installed(String role,String host,int port) {
        var assignment=new ClusterServiceAssignment();assignment.setHostId("different-internal-host-id");
        assignment.setRole(role);assignment.setStatus("ONLINE");
        var config=new LinkedHashMap<String,String>(ExternalDataServiceManager.defaultPaths(role,role.replace('_','-')));
        config.put("host_name",host);config.put("rest_port",String.valueOf(port));
        assignment.setConfigJson(manager.json(config));return assignment;
    }
    Map<String,Object> request(int port) {
        return new LinkedHashMap<>(Map.of("host_id",agent.getId(),"artifact_id",UUID.randomUUID().toString(),"port",port));
    }

    @Test void existingInternalSchemaReservesItsUnitAndStateDirectoriesAcrossAgentIdentities() {
        var existing=installed("schema_registry","192.168.3.229",8086);String original=existing.getConfigJson();
        when(internal.findReservationsForLiveClusters()).thenReturn(List.of(existing));
        manager.create(cluster.getId(),Map.of("schema_registry",request(8081)),"operator");
        assertThat(saved).hasSize(1);
        assertThat(manager.read(saved.get(0).getConfigJson()))
                .containsEntry("service_name","schema-registry-2")
                .containsEntry("working_dir","/var/lib/tantor/schema-registry-2")
                .containsEntry("rest_port","8081");
        assertThat(existing.getConfigJson()).isEqualTo(original);
        verify(internal,never()).save(any());
    }
    @Test void occupiedInternalConnectPortIsRejectedBeforeAnyServiceOrJobIsCreated() {
        when(internal.findReservationsForLiveClusters()).thenReturn(List.of(installed("kafka_connect","192.168.3.229",8083)));
        assertThatThrownBy(()->manager.create(cluster.getId(),Map.of("kafka_connect",request(8083)),"operator"))
                .hasMessageContaining("8083").hasMessageContaining("kafka-connect").hasMessageContaining("Choose a free port");
        verify(services,never()).saveAndFlush(any());verify(jobs,never()).createJob(any(),anyList());
    }
    @Test void explicitDirectoryOverlapWithInternalServiceIsRejected() {
        when(internal.findReservationsForLiveClusters()).thenReturn(List.of(installed("schema_registry","192.168.3.229",8086)));
        var input=request(8081);input.put("working_dir","/var/lib/tantor/schema-registry");
        assertThatThrownBy(()->manager.create(cluster.getId(),Map.of("schema_registry",input),"operator"))
                .hasMessageContaining("directories overlap");
        verify(jobs,never()).createJob(any(),anyList());
    }
    @Test void anotherPhysicalHostsInternalServicesDoNotReserveThisHostsResources() {
        when(internal.findReservationsForLiveClusters()).thenReturn(List.of(installed("kafka_connect","192.168.3.191",8083)));
        manager.create(cluster.getId(),Map.of("kafka_connect",request(8083)),"operator");
        assertThat(manager.read(saved.get(0).getConfigJson())).containsEntry("service_name","kafka-connect")
                .containsEntry("rest_port","8083");
    }
}
