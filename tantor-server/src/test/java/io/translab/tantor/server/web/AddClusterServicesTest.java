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
        assertThat(payload.path("schemaRegistry").path("bootstrap_servers").asText()).contains("broker:9092");
        assertThat(payload.path("kafkaConnect").path("other_install_dir").asText()).isEqualTo("/opt/tantor/schema-registry");
        assertThat(steps.getValue()).hasSize(10).noneSatisfy(step -> assertThat(step.getPayload()).contains("\"operation\":\"deploy\""));
        assertThat(cluster.getServices()).extracting(ClusterServiceAssignment::getRole).containsExactly("broker_controller", "schema_registry", "kafka_connect");
        assertThat(cluster.getStatus()).isEqualTo("SUCCESS");
    }

    @Test
    void rejectsDuplicateServiceBeforeCreatingAnyJob() {
        var existing = new ClusterServiceAssignment();
        existing.setRole("schema_registry");
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
