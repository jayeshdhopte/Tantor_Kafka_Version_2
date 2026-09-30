package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.repository.HostRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DataServiceDeploymentTest {
    private final ClusterController controller = mock(ClusterController.class, CALLS_REAL_METHODS);

    @BeforeEach
    void configureMapper() {
        ReflectionTestUtils.setField(controller, "objectMapper", new ObjectMapper());
        ReflectionTestUtils.setField(controller, "hostRepository", mock(HostRepository.class));
    }

    @Test
    void blocksKnownProd19BeforeKafkaDeploymentStarts() {
        HostRepository hosts = mock(HostRepository.class);
        var host = new io.translab.tantor.server.domain.Host();
        host.setAgentVersion("1.0.0-prod.19");
        when(hosts.findById("host-1")).thenReturn(Optional.of(host));
        ReflectionTestUtils.setField(controller, "hostRepository", hosts);
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateConnectAddon", connect(), List.of(node())))
                .hasMessageContaining("agent update is required");
    }

    @Test
    void permitsReviewedCandidateAndForwardsProtectedDirectories() {
        assertThat(io.translab.tantor.server.service.DataServiceAgentSupport.precheckBlockReason("1.0.0-v2-dataservices.1")).isNull();
        assertThat(io.translab.tantor.server.service.DataServiceAgentSupport.precheckBlockReason("1.0.0-prod.16-reporting.9-dataservices.1")).isNull();
        assertThat(io.translab.tantor.server.service.DataServiceAgentSupport.precheckBlockReason("1.0.0-prod.16-reporting.9")).contains("Install the updated internal agent");
        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        ReflectionTestUtils.invokeMethod(controller, "addDataServiceProtectedPaths", payload,
                Map.of("data_dir", "/custom/kafka-data", "log_dir", "/custom/kafka-logs"));
        ReflectionTestUtils.invokeMethod(controller, "addOtherServicePaths", payload,
                "/custom/schema", "/custom/schema-config", "/custom/schema-logs", "/custom/schema-work", null);
        assertThat(payload).containsEntry("kafka_data_dir", "/custom/kafka-data")
                .containsEntry("kafka_log_dir", "/custom/kafka-logs")
                .containsEntry("other_install_dir", "/custom/schema");
    }

    private ClusterController.ServiceAssignmentReq node() {
        var node = new ClusterController.ServiceAssignmentReq();
        node.setHost_id("host-1");
        node.setRole("broker_controller");
        node.setListener_port(9092);
        node.setController_port(9093);
        node.setJmx_port(7071);
        return node;
    }

    private ClusterController.KafkaConnectAddonReq connect() {
        var addon = new ClusterController.KafkaConnectAddonReq();
        addon.setEnabled(true);
        addon.setHost_id("host-1");
        addon.setArtifact_id("connect-artifact");
        return addon;
    }

    @Test
    void rejectsConnectOnAnUnselectedHostAndConflictingPort() {
        var addon = connect();
        addon.setHost_id("host-2");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateConnectAddon", addon, List.of(node())))
                .hasMessageContaining("selected Kafka node");
        addon.setHost_id("host-1");
        addon.setPort(9092);
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateConnectAddon", addon, List.of(node())))
                .hasMessageContaining("conflicts");
    }

    @Test
    void validatesConnectPathsAndHeapBeforeCreatingDeployment() {
        var addon = connect();
        addon.setWorking_dir("relative/path");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateConnectAddon", addon, List.of(node())))
                .hasMessageContaining("absolute");
        addon.setWorking_dir("/var/lib/connect-custom");
        addon.setHeap_size("0G");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateConnectAddon", addon, List.of(node())))
                .hasMessageContaining("heap size");
        addon.setHeap_size("2G");
        assertThatCode(() -> ReflectionTestUtils.invokeMethod(controller, "validateConnectAddon", addon, List.of(node())))
                .doesNotThrowAnyException();
    }

    @Test
    void passesCustomConnectConfigurationToTheAgentPayload() {
        HostRepository hosts = mock(HostRepository.class);
        when(hosts.findById("host-1")).thenReturn(Optional.empty());
        ReflectionTestUtils.setField(controller, "hostRepository", hosts);
        var addon = connect();
        addon.setArtifact_url("https://artifacts.example/connect.tar.gz");
        addon.setChecksum("a".repeat(64));
        addon.setPort(18083);
        addon.setWorking_dir("/data/connect");
        addon.setPlugin_dir("/opt/plugins");
        addon.setHeap_size("2G");
        Map<String, Object> payload = ReflectionTestUtils.invokeMethod(controller, "connectAddonPayload",
                addon, UUID.randomUUID(), "PLAINTEXT://broker:9092", "/opt/kafka", 2);
        assertThat(payload).containsEntry("rest_port", 18083)
                .containsEntry("working_dir", "/data/connect")
                .containsEntry("plugin_dir", "/opt/plugins")
                .containsEntry("heap_size", "2G")
                .containsEntry("bootstrap_servers", "broker:9092")
                .containsEntry("replication_factor", 2)
                .containsEntry("min_free_disk_mb", 5120);
    }

    @Test
    void schedulesConnectChecksTopicsInstallationAndVerificationInOrder() {
        List<JobStep> steps = new ArrayList<>();
        ReflectionTestUtils.invokeMethod(controller, "appendKafkaConnectSteps", steps, "host-1");
        assertThat(steps).extracting(JobStep::getName).containsExactly(
                "Pre-check Kafka Connect", "Create or verify Kafka Connect internal topics",
                "Install Kafka Connect", "Verify Kafka Connect REST API", "Save Kafka Connect connection");
        assertThat(steps).allSatisfy(step -> assertThat(step.getTargetId()).isEqualTo("host-1"));
    }
}
