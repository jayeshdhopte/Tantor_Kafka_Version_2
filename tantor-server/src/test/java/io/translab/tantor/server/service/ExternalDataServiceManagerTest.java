package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.DiscoveryAgent;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.ExternalDataService;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.domain.JobType;
import io.translab.tantor.server.repository.DiscoveryAgentRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.ExternalDataServiceRepository;
import io.translab.tantor.server.repository.ExternalServiceTaskRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ExternalDataServiceManagerTest {
    @Test
    void createsOneOrderedJobForBothServicesOnAReadyDiscoveryAgent() {
        var clusters = mock(ExternalClusterRepository.class);
        var agents = mock(DiscoveryAgentRepository.class);
        var services = mock(ExternalDataServiceRepository.class);
        var tasks = mock(ExternalServiceTaskRepository.class);
        var jobs = mock(JobService.class);
        var artifacts = mock(ExternalServiceArtifacts.class);
        var manager = new ExternalDataServiceManager(clusters, agents, services, tasks, jobs, artifacts,
                new ObjectMapper());

        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setStatus("SUCCESS");
        cluster.setSecurityProtocol("PLAINTEXT");
        cluster.setBootstrapServers("broker1.example:9092");
        cluster.setBrokerCount(3);
        when(clusters.findForServiceUpdate(clusterId)).thenReturn(Optional.of(cluster));
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));

        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setId("agent-1");
        agent.setClusterId(clusterId);
        agent.setHostname("broker1.example");
        agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());
        agent.setVersion("3.3.0-dataservices.1");
        agent.setCanDeployServices(true);
        when(agents.findById("agent-1")).thenReturn(Optional.of(agent));
        when(services.findByAgentId("agent-1")).thenReturn(List.of());
        when(services.saveAndFlush(any())).thenAnswer(invocation -> {
            ExternalDataService service = invocation.getArgument(0);
            service.setId(UUID.randomUUID());
            return service;
        });
        when(artifacts.verifiedChecksum(any(), any())).thenReturn("a".repeat(64));
        when(jobs.createJob(any(), anyList())).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(UUID.randomUUID());
            return job;
        });

        var selection = Map.of(
                "schema_registry", Map.<String, Object>of("host_id", "agent-1", "artifact_id", UUID.randomUUID().toString()),
                "kafka_connect", Map.<String, Object>of("host_id", "agent-1", "artifact_id", UUID.randomUUID().toString()));
        var response = manager.create(clusterId, selection, "operator");

        assertThat(response).containsKeys("id", "jobId");
        var jobCaptor = ArgumentCaptor.forClass(Job.class);
        @SuppressWarnings("unchecked") ArgumentCaptor<List<JobStep>> stepsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jobs).createJob(jobCaptor.capture(), stepsCaptor.capture());
        assertThat(jobCaptor.getValue().getType()).isEqualTo(JobType.EXTERNAL_SERVICES);
        assertThat(jobCaptor.getValue().getRollbackSupported()).isFalse();
        assertThat(stepsCaptor.getValue()).extracting(JobStep::getName).containsExactly(
                "schema_registry: PRECHECK", "schema_registry: TOPICS", "schema_registry: INSTALL", "schema_registry: SAVE",
                "kafka_connect: PRECHECK", "kafka_connect: TOPICS", "kafka_connect: INSTALL", "kafka_connect: SAVE");
    }

    @Test
    void rejectsOfflineAgentsBeforeCreatingAJob() {
        var clusters = mock(ExternalClusterRepository.class);
        var agents = mock(DiscoveryAgentRepository.class);
        var jobs = mock(JobService.class);
        var manager = new ExternalDataServiceManager(clusters, agents, mock(ExternalDataServiceRepository.class),
                mock(ExternalServiceTaskRepository.class), jobs, mock(ExternalServiceArtifacts.class), new ObjectMapper());
        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setStatus("SUCCESS");
        cluster.setSecurityProtocol("PLAINTEXT");
        cluster.setBootstrapServers("broker1.example:9092");
        when(clusters.findForServiceUpdate(clusterId)).thenReturn(Optional.of(cluster));
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setId("agent-1");
        agent.setClusterId(clusterId);
        agent.setStatus("OFFLINE");
        when(agents.findById("agent-1")).thenReturn(Optional.of(agent));

        assertThatThrownBy(() -> manager.create(clusterId,
                Map.of("schema_registry", Map.<String, Object>of("host_id", "agent-1", "artifact_id", UUID.randomUUID().toString())),
                "operator")).hasMessageContaining("offline");
        verifyNoInteractions(jobs);
    }
}
