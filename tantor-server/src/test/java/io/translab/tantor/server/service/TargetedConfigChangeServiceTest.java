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
    private final io.translab.tantor.server.repository.HostRepository hosts = mock(io.translab.tantor.server.repository.HostRepository.class);
    private final PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
    private final TargetedConfigChangeService service = new TargetedConfigChangeService(
            clusters, external, jobs, jobService, kafka, health, jdbc, new ObjectMapper(), deployment, awaiter, transactions, hosts);
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
                        "log.retention.hours", Map.of("value", "168", "isReadOnly", false),
                        "controller.quorum.fetch.timeout.ms", Map.of("value", "2000", "isReadOnly", true)),
                202, Map.of("num.network.threads", Map.of("value", "3", "isReadOnly", true),
                        "log.retention.hours", Map.of("value", "168", "isReadOnly", false),
                        "controller.quorum.fetch.timeout.ms", Map.of("value", "2000", "isReadOnly", true))));
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
        verify(kafka).validateBrokerConfigs(clusterId, 202, Map.of("log.retention.hours", "72"));
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
        var host = new io.translab.tantor.server.domain.Host(); host.setHostname("broker202.example"); host.setHostIp("192.168.3.213");
        when(hosts.findById("host-202")).thenReturn(Optional.of(host));
        when(jobService.createJob(any(), anyList())).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0); job.setId(UUID.randomUUID()); return job;
        });
        var target = Map.<String,Object>of("nodeId",202,"oldValue","168");
        var plan = Map.<String,Object>of("classification","DYNAMIC_ALLOWED","targets",List.of(target),"key","log.retention.hours","role","BROKER","scope","SPECIFIC");
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(service,"persistPlan",clusterId,
                request("BROKER","SPECIFIC",List.of(202),"log.retention.hours","72"),plan, (String) null);
        verify(jobService).createJob(any(), argThat(steps -> steps.size()==1
                && steps.get(0).getPayload().contains("host-202")
                && steps.get(0).getPayload().contains("/opt/kafka/config/broker.properties")
                && steps.get(0).getPayload().contains("broker202.example")
                && steps.get(0).getName().contains("node 202 on broker202.example (192.168.3.213)")));
    }

    @Test
    void staticBatchQueuesAllNodeReadsBeforeWaitingAndReusesThemAcrossProperties() {
        var one = controller(201); one.setRole("broker");
        var two = controller(202); two.setRole("broker");
        cluster.setServices(List.of(one, two));
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(Map.of(
                201, Map.of("num.network.threads", Map.of("value", "3", "isReadOnly", true),
                        "num.io.threads", Map.of("value", "8", "isReadOnly", true)),
                202, Map.of("num.network.threads", Map.of("value", "3", "isReadOnly", true),
                        "num.io.threads", Map.of("value", "8", "isReadOnly", true))));
        UUID first = UUID.randomUUID(), second = UUID.randomUUID();
        when(deployment.readKafkaProperties(eq(clusterId), anyString(), anyString(), eq("201"), anyString(), anyString())).thenReturn(first);
        when(deployment.readKafkaProperties(eq(clusterId), anyString(), anyString(), eq("202"), anyString(), anyString())).thenReturn(second);
        Task read = new Task(); read.setLogOutput("{\"properties\":{\"num.network.threads\":\"3\",\"num.io.threads\":\"8\"}}");
        when(awaiter.await(first, 90)).thenReturn(read);
        when(awaiter.await(second, 90)).thenReturn(read);
        var request = request("BROKER", "ALL", List.of(), null, null);
        request.setChanges(Map.of("num.network.threads", "5", "num.io.threads", "12"));
        var plan = service.preview(clusterId, request);
        assertThat((List<?>) plan.get("properties")).hasSize(2);
        assertThat((List<?>) plan.get("targets")).hasSize(2);
        var order = org.mockito.Mockito.inOrder(deployment, awaiter);
        order.verify(deployment).readKafkaProperties(eq(clusterId), anyString(), anyString(), eq("201"), anyString(), anyString());
        order.verify(deployment).readKafkaProperties(eq(clusterId), anyString(), anyString(), eq("202"), anyString(), anyString());
        order.verify(awaiter).await(first, 90);
        order.verify(awaiter).await(second, 90);
        order.verifyNoMoreInteractions();
    }

    @Test
    void batchPreviewGroupsChangesByNodeAndRejectsMixedPolicies() {
        var request = request("BROKER", "ALL", List.of(), null, null);
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(Map.of(201, Map.of(
                "log.retention.hours", Map.of("value", "168", "isReadOnly", false),
                "log.segment.bytes", Map.of("value", "100", "isReadOnly", false),
                "num.network.threads", Map.of("value", "3", "isReadOnly", true))));
        when(health.inspect(clusterId, 1, Set.of())).thenReturn(new RollingRestartHealthService.HealthSnapshot(true, 1, 101, 0, 0, 0, 0, Map.of()));
        request.setChanges(Map.of("log.retention.hours", "72", "log.segment.bytes", "200"));
        var plan = service.preview(clusterId, request);
        assertThat((List<?>) plan.get("properties")).hasSize(2);
        assertThat((List<?>) plan.get("targets")).hasSize(1);
        var node = (Map<?, ?>) ((List<?>) plan.get("targets")).get(0);
        assertThat((Map<?, ?>) node.get("changes")).hasSize(2);
        var one = controller(201); one.setRole("broker"); var two = controller(202); two.setRole("broker");
        cluster.setServices(List.of(one, two));
        request.setChanges(Map.of("log.retention.hours", "72", "num.network.threads", "6"));
        // Static reads still happen during preview, so supply a batch read response.
        UUID read = UUID.randomUUID(); when(deployment.readKafkaProperties(any(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(read);
        Task result = new Task(); result.setLogOutput("{\"properties\":{\"num.network.threads\":\"3\"}}"); when(awaiter.await(read, 90)).thenReturn(result);
        assertThatThrownBy(() -> service.preview(clusterId, request)).hasMessageContaining("separately");
    }

    @Test
    void batchRejectsProtectedDuplicateAndEmptySelections() {
        var request = request("BROKER", "ALL", List.of(), null, null);
        request.setChanges(Map.of("process.roles", "broker", "log.retention.hours", "72"));
        assertThatThrownBy(() -> service.preview(clusterId, request)).hasMessageContaining("cannot be changed");
        request.setChanges(Map.of("log.retention.hours", "72", " log.retention.hours", "73"));
        assertThatThrownBy(() -> service.preview(clusterId, request)).hasMessageContaining("Duplicate");
        request.setChanges(Map.of());
        assertThatThrownBy(() -> service.preview(clusterId, request)).hasMessageContaining("1 and 100");
    }

    @Test
    void batchPlanPersistsOneStepPerNodeWithAllExpectedValues() throws Exception {
        var broker = controller(202); broker.setRole("broker"); cluster.setServices(List.of(broker));
        when(jobService.createJob(any(), anyList())).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0); job.setId(UUID.randomUUID()); return job;
        });
        var changes = Map.of("log.retention.hours", "72", "log.segment.bytes", "200");
        var target = Map.<String,Object>of("nodeId", 202, "changes", changes, "expectedValues", Map.of("log.retention.hours", "168", "log.segment.bytes", "100"));
        var plan = Map.<String,Object>of("classification", "ROLLING_RESTART_REQUIRED", "targets", List.of(target), "key", "2 properties", "role", "BROKER", "scope", "ALL");
        var request = request("BROKER", "ALL", List.of(), null, null); request.setChanges(changes);
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(service, "persistPlan", clusterId, request, plan, "tester");
        var captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(jobService).createJob(any(), captor.capture());
        assertThat(captor.getValue()).hasSize(1);
        var stored = new ObjectMapper().readTree(((io.translab.tantor.server.domain.JobStep) captor.getValue().get(0)).getPayload());
        assertThat(stored.get("changes").size()).isEqualTo(2); assertThat(stored.get("expectedValues").size()).isEqualTo(2);
        assertThat(stored.has("key")).isFalse();
    }

    @Test
    void rejectedValuesNeverCreateAJobAndNoopBatchesDoNotValidateWrites() {
        org.mockito.Mockito.doThrow(new IllegalArgumentException("Kafka rejected the value")).when(kafka)
                .validateBrokerConfigs(clusterId, 202, Map.of("log.retention.hours", "abc"));
        assertThatThrownBy(() -> service.apply(clusterId, request("BROKER", "SPECIFIC", List.of(202), "log.retention.hours", "abc")))
                .hasMessageContaining("Kafka rejected");
        org.mockito.Mockito.verifyNoInteractions(jobService);
        var unchanged = request("BROKER", "SPECIFIC", List.of(202), null, null);
        unchanged.setChanges(Map.of("log.retention.hours", "168"));
        assertThatThrownBy(() -> service.preview(clusterId, unchanged)).hasMessageContaining("already");
    }

    @Test
    void removalPlansRealFileDeletionAndValidatesDynamicOverridesBeforeCreatingJobs() {
        var first=controller(201); first.setRole("broker"); var second=controller(202); second.setRole("broker"); cluster.setServices(List.of(first,second));
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(Map.of(201,Map.of("num.network.threads",Map.of("value","3","isReadOnly",true),
                "log.retention.hours",Map.of("value","72","isReadOnly",false,"source","DYNAMIC_BROKER_CONFIG")),
                202,Map.of("num.network.threads",Map.of("value","3","isReadOnly",true),
                "log.retention.hours",Map.of("value","72","isReadOnly",false,"source","DYNAMIC_BROKER_CONFIG"))));
        UUID read=UUID.randomUUID(); when(deployment.readKafkaProperties(any(),anyString(),anyString(),anyString(),anyString(),anyString())).thenReturn(read);
        Task result=new Task(); result.setLogOutput("{\"properties\":{\"num.network.threads\":\"3\",\"log.retention.hours\":\"168\"}}"); when(awaiter.await(read,90)).thenReturn(result);
        var request=request("BROKER","ALL",List.of(),null,null); request.setOperation("REMOVE"); request.setKeys(List.of("num.network.threads","log.retention.hours"));
        var plan=service.preview(clusterId,request);
        assertThat(plan.get("classification")).isEqualTo("ROLLING_RESTART_REQUIRED");
        var nodes=(List<?>)plan.get("targets"); assertThat(nodes).hasSize(2);
        var target=(Map<?,?>)nodes.get(0); var changes=(Map<?,?>)target.get("changes");
        assertThat(changes.containsKey("num.network.threads")).isTrue(); assertThat(changes.get("num.network.threads")).isNull();
        assertThat((Map<?,?>)target.get("dynamicRemovalExpected")).hasSize(1);
        verify(kafka).removeBrokerConfigs(clusterId,201,Set.of("log.retention.hours"),true);
        verify(health).assertBrokerCanStop(clusterId,201);
        org.mockito.Mockito.doThrow(new IllegalStateException("Unsafe restart")).when(health).assertBrokerCanStop(clusterId,201);
        assertThat(service.preview(clusterId,request).get("healthy")).isEqualTo(false);
        request.setKeys(List.of("node.id")); assertThatThrownBy(()->service.preview(clusterId,request)).hasMessageContaining("Protected");
        org.mockito.Mockito.verifyNoInteractions(jobService);
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
    @Test void blankValueCannotBeUsedAsPropertyDeletion() {
        var request = new TargetedConfigChangeService.ChangeRequest();
        request.setRole("BROKER"); request.setScope("ALL"); request.setKey("broker.session.timeout.ms"); request.setValue("");
        assertThatThrownBy(() -> service.preview(clusterId, request)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Use property deletion");
        verify(deployment, org.mockito.Mockito.never()).patchKafkaConfig(any(), anyString(), anyString(), anyString(), anyString(), anyString(), any(), any(), org.mockito.ArgumentMatchers.anyBoolean(), anyString());
    }
}
