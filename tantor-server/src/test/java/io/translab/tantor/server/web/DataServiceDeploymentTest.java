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

    @Test
    void sharedSchemaDefaultsRequireNewAgentButStandaloneRemainsSupported() {
        var hosts = mock(HostRepository.class);
        var host = new io.translab.tantor.server.domain.Host();
        host.setAgentVersion("1.0.0-dataservices.4");
        when(hosts.findById("host-1")).thenReturn(Optional.of(host));
        ReflectionTestUtils.setField(controller, "hostRepository", hosts);
        var addon = new ClusterController.SchemaRegistryAddonReq();
        addon.setHost_id("host-1"); addon.setArtifact_id("unified");
        addon.setArtifact_url("https://artifacts.example/unified.tar.gz");
        Map<String, Object> payload = ReflectionTestUtils.invokeMethod(controller, "schemaAddonPayload",
                addon, UUID.randomUUID(), "broker:9092", "/custom/kafka", 2);
        assertThat(payload).containsEntry("install_dir", "/custom/kafka")
                .containsEntry("config_dir", "/custom/kafka/etc/schema-registry");
        host.setAgentVersion("1.0.0-dataservices.3");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "schemaAddonPayload",
                addon, UUID.randomUUID(), "broker:9092", "/custom/kafka", 2)).hasMessageContaining("dataservices.4");
        addon.setInstall_dir("/opt/tantor/schema-registry");
        assertThatCode(() -> ReflectionTestUtils.invokeMethod(controller, "schemaAddonPayload",
                addon, UUID.randomUUID(), "broker:9092", "/custom/kafka", 2)).doesNotThrowAnyException();
    }

    @Test
    void passesSchemaHeapRangeToTheAgentPayload() {
        var addon = new ClusterController.SchemaRegistryAddonReq();
        addon.setHost_id("host-1");
        addon.setArtifact_id("schema-artifact");
        addon.setArtifact_url("https://artifacts.example/schema.tar.gz");
        addon.setHeap_size("2G");
        addon.setHeap_xms("512M");
        addon.setHeap_xmx("2G");
        Map<String, Object> payload = ReflectionTestUtils.invokeMethod(controller, "schemaAddonPayload",
                addon, UUID.randomUUID(), "broker:9092", "/opt/kafka", 1);
        assertThat(payload).containsEntry("heap_xms", "512M").containsEntry("heap_xmx", "2G");
        addon.setHeap_xms("3G");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateSchemaAddon", addon, List.of(node())))
                .hasMessageContaining("Min Heap cannot exceed Max Heap");
    }

    @Test
    void preparesMultipleInstancesWithSharedIdentityAndHostSpecificSteps() throws Exception {
        var first = connect();
        first.setArtifact_url("https://artifacts.example/connect.tar.gz");
        var second = connect(); second.setHost_id("host-2"); second.setPort(18083);
        second.setArtifact_url("https://artifacts.example/connect.tar.gz");
        var schema = new ClusterController.SchemaRegistryAddonReq();
        schema.setHost_id("host-1"); schema.setArtifact_id("schema"); schema.setArtifact_url("https://artifacts.example/schema.tar.gz");
        var otherSchema = new ClusterController.SchemaRegistryAddonReq();
        otherSchema.setHost_id("host-2"); otherSchema.setArtifact_id("schema"); otherSchema.setArtifact_url("https://artifacts.example/schema.tar.gz");
        var addons = new ClusterController.AddonsReq();
        addons.setKafka_connect_workers(List.of(first, second)); addons.setSchema_registries(List.of(schema, otherSchema));
        var secondNode = node(); secondNode.setHost_id("host-2");
        var cluster = new io.translab.tantor.server.domain.Cluster(); cluster.setId(UUID.randomUUID());
        cluster.setBootstrapServers("broker1:9092,broker2:9092"); cluster.setServices(new ArrayList<>());
        List<Map<String, Object>> payloads = ReflectionTestUtils.invokeMethod(controller, "prepareDataServicePayloads", addons, List.of(node(), secondNode), cluster, Map.of());
        assertThat(payloads).hasSize(4);
        assertThat(payloads.get(2).get("group_id")).isEqualTo(payloads.get(3).get("group_id"));
        assertThat(payloads.get(2)).containsEntry("config_topic", "connect-configs").containsEntry("replication_factor", 2);
        assertThat(payloads.get(0).get("group_id")).isEqualTo(payloads.get(1).get("group_id"));
        List<JobStep> steps = new ArrayList<>();
        for (var payload : payloads) ReflectionTestUtils.invokeMethod(controller, "appendDataServiceSteps", steps, payload);
        assertThat(steps).hasSize(20);
        Map<?, ?> secondWorker = new ObjectMapper().readValue(steps.get(15).getPayload(), Map.class);
        assertThat((Map<String, Object>) secondWorker.get("serviceConfig")).containsEntry("host_id", "host-2").containsEntry("rest_port", 18083);
        assertThat(steps.get(15).getTargetId()).isEqualTo("host-2");
    }

    @Test
    void rejectsDuplicateHostsAndIndependentConnectGroups() {
        var second = connect(); var addons = new ClusterController.AddonsReq();
        addons.setKafka_connect_workers(List.of(connect(), second));
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateDataServiceRequests", addons, List.of(node())))
                .hasMessageContaining("one Connect worker");
        var secondNode = node(); secondNode.setHost_id("host-2");
        second.setHost_id("host-2"); second.setGroup_id("other-group");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateDataServiceRequests", addons, List.of(node(), secondNode)))
                .hasMessageContaining("same worker group");
    }

    @Test
    void addingWorkerPreservesExistingTopicsAndReplicationAndRejectsOccupiedHost() throws Exception {
        var cluster = new io.translab.tantor.server.domain.Cluster(); cluster.setId(UUID.randomUUID());
        cluster.setBootstrapServers("broker1:9092,broker2:9092");
        var existing = new io.translab.tantor.server.domain.ClusterServiceAssignment(); existing.setRole("kafka_connect"); existing.setHostId("host-1");
        existing.setConfigJson(new ObjectMapper().writeValueAsString(Map.of("group_id", "tantor-connect-" + cluster.getId(), "offset_topic", "old-offsets", "config_topic", "old-configs", "status_topic", "old-status", "replication_factor", 1)));
        cluster.setServices(new ArrayList<>(List.of(existing)));
        var worker = connect(); worker.setHost_id("host-2"); worker.setArtifact_url("https://artifacts.example/connect.tar.gz");
        var addons = new ClusterController.AddonsReq(); addons.setKafka_connect_workers(List.of(worker));
        var secondNode = node(); secondNode.setHost_id("host-2");
        List<Map<String, Object>> payloads = ReflectionTestUtils.invokeMethod(controller, "prepareDataServicePayloads", addons, List.of(node(), secondNode), cluster, Map.of());
        assertThat(payloads.get(0)).containsEntry("offset_topic", "old-offsets").containsEntry("config_topic", "old-configs")
                .containsEntry("status_topic", "old-status").containsEntry("replication_factor", 1);
        worker.setHost_id("host-1");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "prepareDataServicePayloads", addons, List.of(node(), secondNode), cluster, Map.of()))
                .hasMessageContaining("already assigned");
    }

    @Test
    void acceptsLegacyAndPluralDeploymentJson() throws Exception {
        var mapper = new ObjectMapper();
        var legacy = mapper.readValue("{\"kafka_connect\":{\"enabled\":true,\"host_id\":\"host-1\"}}", ClusterController.AddonsReq.class);
        List<?> workers = ReflectionTestUtils.invokeMethod(controller, "connectAddons", legacy);
        assertThat(workers).hasSize(1);
        var multi = mapper.readValue("{\"kafka_connect_workers\":[{\"host_id\":\"host-1\"},{\"host_id\":\"host-2\"}],\"schema_registries\":[{\"host_id\":\"host-1\"},{\"host_id\":\"host-2\"}]}", ClusterController.AddonsReq.class);
        assertThat(multi.getKafka_connect_workers()).hasSize(2);
        assertThat(multi.getSchema_registries()).hasSize(2);
    }

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
                .hasMessageContaining("Heap size");
        addon.setHeap_size("2G");
        addon.setHeap_xms("3G");
        addon.setHeap_xmx("2G");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "validateConnectAddon", addon, List.of(node())))
                .hasMessageContaining("Min Heap cannot exceed Max Heap");
        addon.setHeap_xms("512M");
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
        addon.setHeap_xms("512M");
        addon.setHeap_xmx("2G");
        Map<String, Object> payload = ReflectionTestUtils.invokeMethod(controller, "connectAddonPayload",
                addon, UUID.randomUUID(), "PLAINTEXT://broker:9092", "/opt/kafka", 2);
        assertThat(payload).containsEntry("rest_port", 18083)
                .containsEntry("working_dir", "/data/connect")
                .containsEntry("plugin_dir", "/opt/plugins")
                .containsEntry("heap_size", "2G")
                .containsEntry("heap_xms", "512M")
                .containsEntry("heap_xmx", "2G")
                .containsEntry("bootstrap_servers", "broker:9092")
                .containsEntry("replication_factor", 2)
                .containsEntry("min_free_disk_mb", 5120);
    }

    @Test
    void blankConnectPathsResolveToUnifiedKafkaAndRequireUpdatedAgent() {
        var hosts = mock(HostRepository.class);
        var host = new io.translab.tantor.server.domain.Host();
        host.setAgentVersion("1.0.0-prod.16-reporting.9-dataservices.3-configchange.3");
        when(hosts.findById("host-1")).thenReturn(Optional.of(host));
        ReflectionTestUtils.setField(controller, "hostRepository", hosts);
        var addon = connect();
        addon.setArtifact_url("https://artifacts.example/unified.tar.gz");
        Map<String, Object> payload = ReflectionTestUtils.invokeMethod(controller, "connectAddonPayload",
                addon, UUID.randomUUID(), "broker:9092", "/custom/kafka", 2);
        assertThat(payload).containsEntry("install_dir", "/custom/kafka")
                .containsEntry("config_dir", "/custom/kafka/config")
                .containsEntry("plugin_dir", "/custom/kafka/connect-plugins")
                .containsEntry("log_dir", "/var/log/tantor/kafka-connect")
                .containsEntry("working_dir", "/var/lib/tantor/kafka-connect");
        host.setAgentVersion("1.0.0-prod.16-reporting.9-dataservices.2-configchange.3");
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(controller, "connectAddonPayload",
                addon, UUID.randomUUID(), "broker:9092", "/custom/kafka", 2)).hasMessageContaining("dataservices.3");
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
