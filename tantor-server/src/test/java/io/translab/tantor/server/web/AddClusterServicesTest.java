package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.service.ActivityAlertService;
import io.translab.tantor.server.service.JobService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AddClusterServicesTest {
    private final ClusterController controller = mock(ClusterController.class, CALLS_REAL_METHODS);
    private final ClusterRepository clusters = mock(ClusterRepository.class);
    private final JobService jobs = mock(JobService.class);
    private final RoleAuthenticationUtil auth = mock(RoleAuthenticationUtil.class);
    private final Cluster cluster = new Cluster();

    @BeforeEach
    void setup() {
        ReflectionTestUtils.setField(controller, "clusterRepository", clusters);
        ReflectionTestUtils.setField(controller, "jobService", jobs);
        ReflectionTestUtils.setField(controller, "roleAuthenticationUtil", auth);
        ReflectionTestUtils.setField(controller, "objectMapper", new ObjectMapper());
        ReflectionTestUtils.setField(controller, "hostRepository", mock(HostRepository.class));
        ReflectionTestUtils.setField(controller, "activityAlertService", mock(ActivityAlertService.class));
        when(auth.canAccess("token", RoleAuthenticationUtil.ADD_NODE)).thenReturn(true);
        when(auth.extractUsername("token")).thenReturn("operator");
        cluster.setId(UUID.randomUUID());
        cluster.setName("existing");
        cluster.setStatus("SUCCESS");
        cluster.setMode("kraft");
        cluster.setKafkaVersion("4.3.0");
        cluster.setBootstrapServers("broker:9092");
        cluster.setConfigJson("{\"kafka_install_dir\":\"/opt/kafka\",\"data_dir\":\"/data/kafka\"}");
        var broker = new ClusterServiceAssignment();
        broker.setRole("broker_controller");
        broker.setHostId("host-1");
        broker.setJmxExporterPort(7071);
        cluster.setServices(new ArrayList<>(List.of(broker)));
        when(clusters.findForServiceUpdate(cluster.getId())).thenReturn(Optional.of(cluster));
        when(jobs.createJob(any(), anyList())).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(UUID.randomUUID());
            return job;
        });
    }

    private ClusterController.AddonsReq request() {
        var request = new ClusterController.AddonsReq();
        var schema = new ClusterController.SchemaRegistryAddonReq();
        schema.setHost_id("host-1");
        schema.setArtifact_id("schema");
        schema.setArtifact_url("https://artifacts.example/schema.tar.gz");
        var connect = new ClusterController.KafkaConnectAddonReq();
        connect.setHost_id("host-1");
        connect.setArtifact_id("connect");
        connect.setArtifact_url("https://artifacts.example/connect.tar.gz");
        request.setSchema_registry(schema);
        request.setKafka_connect(connect);
        return request;
    }

    @Test
    void createsOneJobForBothServicesWithoutKafkaDeploymentOrRollback() throws Exception {
        var response = controller.addServicesToCluster("token", cluster.getId(), request());
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).containsKeys("id", "jobId");
        var job = ArgumentCaptor.forClass(Job.class);
        @SuppressWarnings("unchecked") ArgumentCaptor<List<JobStep>> steps = ArgumentCaptor.forClass(List.class);
        verify(jobs).createJob(job.capture(), steps.capture());
        assertThat(job.getValue().getRollbackSupported()).isFalse();
        var payload = new ObjectMapper().readTree(job.getValue().getPayload());
        assertThat(payload.path("servicesOnly").asBoolean()).isTrue();
        var schemaPayload = new ObjectMapper().readTree(steps.getValue().get(0).getPayload()).path("serviceConfig");
        var connectPayload = new ObjectMapper().readTree(steps.getValue().get(5).getPayload()).path("serviceConfig");
        assertThat(schemaPayload.path("bootstrap_servers").asText()).contains("broker:9092");
        assertThat(schemaPayload.path("install_dir").asText()).isEqualTo("/opt/kafka");
        assertThat(schemaPayload.path("config_dir").asText()).isEqualTo("/opt/kafka/etc/schema-registry");
        assertThat(connectPayload.path("other_install_dir").asText()).isEqualTo("/opt/kafka");
        assertThat(connectPayload.path("other_config_dir").asText()).isEqualTo("/opt/kafka/etc/schema-registry");
        assertThat(steps.getValue()).hasSize(10).noneSatisfy(step -> assertThat(step.getPayload()).contains("\"operation\":\"deploy\""));
        assertThat(cluster.getServices()).extracting(ClusterServiceAssignment::getRole).containsExactly("broker_controller", "schema_registry", "kafka_connect");
        assertThat(cluster.getStatus()).isEqualTo("SUCCESS");
    }

    @Test
    void clusterDetailsExposeExistingHostAndGroupForScaleOut() {
        cluster.setKafkaClusterId("test-kafka-cluster-id");
        ReflectionTestUtils.setField(controller, "brokerMetricsCacheService", mock(io.translab.tantor.server.service.BrokerMetricsCacheService.class));
        var worker = new ClusterServiceAssignment(); worker.setRole("kafka_connect"); worker.setHostId("host-1"); worker.setStatus("ONLINE");
        worker.setConfigJson("{\"group_id\":\"custom-connect-group\",\"rest_port\":8083}");
        cluster.getServices().add(worker);
        when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));
        var response = controller.getCluster(cluster.getId());
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        List<java.util.Map<String, Object>> instances = (List<java.util.Map<String, Object>>) response.getBody().get("serviceInstances");
        assertThat(instances).hasSize(1);
        assertThat(instances.get(0)).containsEntry("hostId", "host-1").containsEntry("groupId", "custom-connect-group").containsEntry("status", "ONLINE");
    }

    @Test
    void scaleOutCreatesHostSpecificStepsWithoutRedeployingTheExistingInstance() throws Exception {
        var broker = new ClusterServiceAssignment(); broker.setHostId("host-2"); broker.setRole("broker");
        cluster.getServices().add(broker);
        var existing = new ClusterServiceAssignment(); existing.setHostId("host-1"); existing.setRole("kafka_connect"); existing.setStatus("ONLINE");
        existing.setConfigJson("{\"group_id\":\"tantor-connect-" + cluster.getId() + "\",\"replication_factor\":1}");
        cluster.getServices().add(existing);
        var request = new ClusterController.AddonsReq(); var worker = request().getKafka_connect(); worker.setHost_id("host-2");
        request.setKafka_connect_workers(List.of(worker));
        assertThat(controller.addServicesToCluster("token", cluster.getId(), request).getStatusCode().value()).isEqualTo(200);
        @SuppressWarnings("unchecked") ArgumentCaptor<List<JobStep>> steps = ArgumentCaptor.forClass(List.class);
        verify(jobs).createJob(any(), steps.capture());
        assertThat(steps.getValue()).hasSize(5).allSatisfy(step -> assertThat(step.getTargetId()).isEqualTo("host-2"));
        assertThat(existing.getStatus()).isEqualTo("ONLINE");
        assertThat(cluster.getServices().stream().filter(service -> "kafka_connect".equals(service.getRole()))).hasSize(2);
    }

    @Test
    void rejectsDuplicateServiceBeforeCreatingAnyJob() {
        var existing = new ClusterServiceAssignment();
        existing.setRole("schema_registry");
        existing.setHostId("host-1");
        cluster.getServices().add(existing);
        assertThat(controller.addServicesToCluster("token", cluster.getId(), request()).getStatusCode().value()).isEqualTo(409);
        verifyNoInteractions(jobs);
    }

    @Test
    void rejectsEmptySelectionAndConflictingPorts() {
        assertThat(controller.addServicesToCluster("token", cluster.getId(), new ClusterController.AddonsReq()).getStatusCode().value()).isEqualTo(400);
        var request = request();
        request.getKafka_connect().setPort(8081);
        assertThat(controller.addServicesToCluster("token", cluster.getId(), request).getStatusCode().value()).isEqualTo(400);
        request.getKafka_connect().setPort(9092);
        assertThat(controller.addServicesToCluster("token", cluster.getId(), request).getStatusCode().value()).isEqualTo(400);
        verifyNoInteractions(jobs);
    }

    @Test
    void rejectsUnknownHostsAndUnmanagedClusters() {
        var request = request();
        request.getSchema_registry().setHost_id("another-cluster-host");
        assertThat(controller.addServicesToCluster("token", cluster.getId(), request).getStatusCode().value()).isEqualTo(400);
        cluster.setMode("EXTERNAL");
        assertThat(controller.addServicesToCluster("token", cluster.getId(), request()).getStatusCode().value()).isEqualTo(400);
        verifyNoInteractions(jobs);
    }

    @Test
    void checksPermissionBeforeLoadingCluster() {
        assertThat(controller.addServicesToCluster("unauthorized", cluster.getId(), request()).getStatusCode().value()).isEqualTo(401);
        verifyNoInteractions(clusters, jobs);
    }
}
