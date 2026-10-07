package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TargetedConfigChangeServiceTest {
    private final ClusterRepository clusters = mock(ClusterRepository.class);
    private final ExternalClusterRepository external = mock(ExternalClusterRepository.class);
    private final JobRepository jobs = mock(JobRepository.class);
    private final JobService jobService = mock(JobService.class);
    private final KafkaAdminService kafka = mock(KafkaAdminService.class);
    private final RollingRestartHealthService health = mock(RollingRestartHealthService.class);
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final DeploymentService deployment = mock(DeploymentService.class);
    private final AgentTaskAwaiter awaiter = mock(AgentTaskAwaiter.class);
    private final PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
    private final TargetedConfigChangeService service = new TargetedConfigChangeService(
            clusters, external, jobs, jobService, kafka, health, jdbc, new ObjectMapper(), deployment, awaiter, transactions);
    private final UUID clusterId = UUID.randomUUID();
    private final Cluster cluster = new Cluster();

    @BeforeEach
    void setup() {
        cluster.setId(clusterId);
        cluster.setMode("kraft");
        cluster.setKafkaVersion("4.1.0");
        cluster.setServices(List.of());
        when(clusters.findWithServicesById(clusterId)).thenReturn(Optional.of(cluster));
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(Map.of(
                201, Map.of("num.network.threads", Map.of("value", "3", "isReadOnly", true),
                        "log.retention.hours", Map.of("value", "168", "isReadOnly", false)),
                202, Map.of("num.network.threads", Map.of("value", "3", "isReadOnly", true),
                        "log.retention.hours", Map.of("value", "168", "isReadOnly", false))));
        when(health.inspect(eq(clusterId), eq(2), eq(Set.of()))).thenReturn(
                new RollingRestartHealthService.HealthSnapshot(true, 2, 101, 0, 0, 0, 0, Map.of()));
        when(jobs.findByResourceKeyAndStatusIn(eq("cluster:" + clusterId), any())).thenReturn(List.of());
        when(deployment.readKafkaProperty(eq(clusterId), anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(UUID.randomUUID());
        Task read = new Task();
        read.setLogOutput("{\"found\":false,\"value\":\"\"}");
        when(awaiter.await(any(UUID.class), eq(60L))).thenReturn(read);
    }

    @Test
    void dynamicPreviewUsesSelectedLiveBrokersOnly() {
        var request = request("BROKER", "SPECIFIC", List.of(202), "log.retention.hours", "72");
        Map<String, Object> plan = service.preview(clusterId, request);
        assertThat(plan.get("classification")).isEqualTo("DYNAMIC_ALLOWED");
        assertThat((List<?>) plan.get("targets")).hasSize(1);
        assertThat(((Map<?, ?>) ((List<?>) plan.get("targets")).get(0)).get("nodeId")).isEqualTo(202);
    }

    @Test
    void controllerPreviewOrdersLeaderLast() {
        cluster.setServices(List.of(controller(101), controller(102), controller(103)));
        when(kafka.getControllerId(clusterId)).thenReturn(102);
        var request = request("CONTROLLER", "ALL", List.of(), "controller.quorum.fetch.timeout.ms", "2500");
        Map<String, Object> plan = service.preview(clusterId, request);
        assertThat(plan.get("classification")).isEqualTo("ROLLING_RESTART_REQUIRED");
        assertThat(((List<?>) plan.get("targets")).stream()
                .map(item -> (Integer) ((Map<?, ?>) item).get("nodeId")).toList()).containsExactly(101, 103, 102);
    }

    @Test
    void protectedTopologyKeyIsRejected() {
        var request = request("BROKER", "ALL", List.of(), "process.roles", "broker");
        assertThatThrownBy(() -> service.preview(clusterId, request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be changed");
    }

    @Test
    void dynamicJobPersistsTheAgentAndConfigFileNeededForBackup() {
        var broker = controller(202); broker.setRole("broker");
        cluster.setServices(List.of(broker));
        when(jobService.createJob(any(), anyList())).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0); job.setId(UUID.randomUUID()); return job;
        });
        var target = Map.<String,Object>of("nodeId",202,"oldValue","168");
        var plan = Map.<String,Object>of("classification","DYNAMIC_ALLOWED","targets",List.of(target),"key","log.retention.hours","role","BROKER","scope","SPECIFIC");
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(service,"persistPlan",clusterId,
                request("BROKER","SPECIFIC",List.of(202),"log.retention.hours","72"),plan);
        verify(jobService).createJob(any(), argThat(steps -> steps.size()==1
                && steps.get(0).getPayload().contains("host-202")
                && steps.get(0).getPayload().contains("/opt/kafka/config/broker.properties")));
    }

    private ClusterServiceAssignment controller(int nodeId) {
        ClusterServiceAssignment assignment = new ClusterServiceAssignment();
        assignment.setCluster(cluster);
        assignment.setNodeId(nodeId);
        assignment.setHostId("host-" + nodeId);
        assignment.setRole("controller");
        return assignment;
    }

    private TargetedConfigChangeService.ChangeRequest request(String role, String scope, List<Integer> ids, String key, String value) {
        var request = new TargetedConfigChangeService.ChangeRequest();
        request.setRole(role);
        request.setScope(scope);
        request.setTargetNodeIds(ids);
        request.setKey(key);
        request.setValue(value);
        return request;
    }
}
