package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.DiscoveryAgent;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.ExternalClusterNode;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import io.translab.tantor.server.repository.DiscoveryAgentRepository;
import io.translab.tantor.server.repository.ExternalClusterNodeRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.security.TruststoreStorageService;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExternalClusterServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    void successfulAdminConnectionUsesAuthoritativeZooKeeperModeFromDiscoveryAgent() {
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        KafkaAdminService kafkaAdminService = mock(KafkaAdminService.class);
        ExternalClusterService service = service(
                externalClusterRepository,
                mock(DiscoveryAgentRepository.class),
                kafkaAdminService);

        Map<String, Object> activeBrokerController = new java.util.HashMap<>();
        activeBrokerController.put("id", 1);
        activeBrokerController.put("host", "192.168.3.150");
        activeBrokerController.put("isBroker", true);
        activeBrokerController.put("isController", true);
        Map<String, Object> inspection = new java.util.HashMap<>();
        inspection.put("connected", true);
        inspection.put("clusterId", "zk-cluster-id");
        inspection.put("mode", "auto-detected by Kafka client");
        inspection.put("brokers", List.of(activeBrokerController));

        when(kafkaAdminService.inspectBootstrapServers(any(ExternalCluster.class),
                org.mockito.ArgumentMatchers.eq(false)))
                .thenReturn(inspection);

        ExternalClusterService.ExternalDiscoveryReport report =
                new ExternalClusterService.ExternalDiscoveryReport();
        report.setName("legacy-zk");
        report.setHostname("192.168.3.150");
        report.setBootstrapServers("192.168.3.150:9092");
        report.setKafkaMode("ZooKeeper");
        report.setKafkaVersion("2.5.0");
        report.setRunning(true);

        Map<String, ExternalClusterService.ExternalDiscoveryReport> pending =
                (Map<String, ExternalClusterService.ExternalDiscoveryReport>)
                        ReflectionTestUtils.getField(service, "pendingDiscoveries");
        pending.put("discovery-150", report);

        ExternalClusterService.BootstrapExternalClusterRequest request =
                new ExternalClusterService.BootstrapExternalClusterRequest();
        request.setBootstrapServers("192.168.3.150:9092");
        request.setSecurityProtocol("PLAINTEXT");

        Map<String, Object> result = service.testBootstrap(request);

        assertThat(result).containsEntry("connected", true)
                .containsEntry("mode", "ZooKeeper")
                .containsEntry("kafkaMode", "ZooKeeper")
                .containsEntry("discoveryKey", "discovery-150");
        assertThat(activeBrokerController).containsEntry("isBroker", true)
                .containsEntry("isController", false);
    }

    @Test
    void brokerRecordsNormalizePreviouslyPersistedZooKeeperCombinedRole() {
        ExternalClusterRepository clusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        DiscoveryAgentRepository agentRepository = mock(DiscoveryAgentRepository.class);
        ExternalClusterService service = service(
                mock(ClusterRepository.class), clusterRepository, nodeRepository,
                agentRepository, mock(KafkaAdminService.class));

        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(UUID.randomUUID());
        cluster.setKafkaMode("ZooKeeper");

        ExternalClusterNode activeBrokerController = new ExternalClusterNode();
        activeBrokerController.setClusterId(cluster.getId());
        activeBrokerController.setNodeId(1);
        activeBrokerController.setHost("192.168.3.150");
        activeBrokerController.setIsBroker(true);
        activeBrokerController.setIsController(true);

        when(nodeRepository.findByClusterId(cluster.getId()))
                .thenReturn(List.of(activeBrokerController));
        when(agentRepository.findByClusterId(cluster.getId())).thenReturn(List.of());

        assertThat(service.brokerRecords(cluster)).singleElement().satisfies(record -> {
            assertThat(record.getNodeId()).isEqualTo(1);
            assertThat(record.getRole()).isEqualTo("broker");
        });
    }

    @Test
    void registrationDoesNotTurnUnknownAdminModeIntoKraft() {
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterService service = service(externalClusterRepository);
        when(externalClusterRepository.save(any(ExternalCluster.class))).thenAnswer(invocation -> {
            ExternalCluster cluster = invocation.getArgument(0);
            if (cluster.getId() == null) cluster.setId(UUID.randomUUID());
            return cluster;
        });

        ExternalClusterService.BootstrapExternalClusterRequest request =
                new ExternalClusterService.BootstrapExternalClusterRequest();
        request.setName("bootstrap-only");
        request.setBootstrapServers("192.168.3.150:9092");
        request.setClusterId("zk-cluster-id");
        request.setKafkaMode("auto-detected by Kafka client");

        ExternalCluster saved = service.registerBootstrapCluster(request);

        assertThat(saved.getKafkaMode()).isEqualTo("Unknown");
    }

    @Test
    void preservesPersistedKafkaClusterIdWhenFollowUpDiscoveryReportOmitsIt() {
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterService service = service(externalClusterRepository);

        ExternalCluster existing = new ExternalCluster();
        existing.setId(UUID.randomUUID());
        existing.setName("external-test");
        existing.setBootstrapServers("192.168.3.208:9092");
        existing.setKafkaClusterId("kafka-assigned-id");

        when(externalClusterRepository.findByBootstrapServersAndStatusNot(
                "192.168.3.208:9092", "DELETED")).thenReturn(Optional.of(existing));
        when(externalClusterRepository.save(existing)).thenReturn(existing);

        ExternalClusterService.ExternalDiscoveryReport report =
                new ExternalClusterService.ExternalDiscoveryReport();
        report.setName("external-test");
        report.setBootstrapServers("192.168.3.208:9092");
        report.setKafkaClusterId(null);
        report.setRunning(true);

        ExternalCluster saved = service.upsertDiscoveryCluster(report);

        assertThat(saved.getKafkaClusterId()).isEqualTo("kafka-assigned-id");
    }

    @Test
    void backfillsMissingKafkaClusterIdDuringHealthCheck() {
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        DiscoveryAgentRepository discoveryAgentRepository = mock(DiscoveryAgentRepository.class);
        KafkaAdminService kafkaAdminService = mock(KafkaAdminService.class);
        ExternalClusterService service = service(
                externalClusterRepository, discoveryAgentRepository, kafkaAdminService);

        ExternalCluster existing = new ExternalCluster();
        existing.setId(UUID.randomUUID());
        existing.setName("external-test");
        existing.setBootstrapServers("192.168.3.208:9092");
        existing.setKafkaClusterId(null);
        existing.setStatus("DEGRADED");

        when(externalClusterRepository.findByStatusNot("DELETED")).thenReturn(java.util.List.of(existing));
        when(kafkaAdminService.inspectBootstrapServers(existing, true))
                .thenReturn(java.util.Map.of("connected", true, "clusterId", "recovered-kafka-id"));
        when(discoveryAgentRepository.findByClusterId(existing.getId())).thenReturn(java.util.List.of());

        service.checkExternalClustersHealth();

        assertThat(existing.getKafkaClusterId()).isEqualTo("recovered-kafka-id");
        verify(externalClusterRepository).save(existing);
    }

    @Test
    void healthyAgentResolvesExternalHealthAlertEvenWhenClusterStatusWasAlreadySuccess() {
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        DiscoveryAgentRepository discoveryAgentRepository = mock(DiscoveryAgentRepository.class);
        KafkaAdminService kafkaAdminService = mock(KafkaAdminService.class);
        ActivityAlertService alertService = mock(ActivityAlertService.class);
        ExternalClusterService service = service(
                externalClusterRepository,
                discoveryAgentRepository,
                kafkaAdminService,
                alertService);

        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(UUID.randomUUID());
        cluster.setName("external-test");
        cluster.setStatus("SUCCESS");
        cluster.setKafkaClusterId("kafka-id");

        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now());

        when(externalClusterRepository.findByStatusNot("DELETED")).thenReturn(List.of(cluster));
        when(kafkaAdminService.inspectBootstrapServers(cluster, true))
                .thenReturn(Map.of("connected", true, "clusterId", "kafka-id"));
        when(discoveryAgentRepository.findByClusterId(cluster.getId())).thenReturn(List.of(agent));

        service.checkExternalClustersHealth();

        verify(alertService).synchronizeExternalClusterHealth(
                cluster.getId(), cluster.getName(), "SUCCESS", 1, 1, List.of());
        verify(alertService).resolveOrphanedExternalClusterHealthAlerts(Set.of(cluster.getId()));
        verify(externalClusterRepository, never()).save(cluster);
    }

    @Test
    void scheduledHealthCheckUsesSavedSecurityForEveryBootstrapAndProtocol() {
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        DiscoveryAgentRepository discoveryAgentRepository = mock(DiscoveryAgentRepository.class);
        KafkaAdminService kafkaAdminService = mock(KafkaAdminService.class);
        ExternalClusterService service = service(
                externalClusterRepository, discoveryAgentRepository, kafkaAdminService);

        List<ExternalCluster> clusters = new java.util.ArrayList<>();
        for (String host : List.of("192.168.3.213", "192.168.3.229", "192.168.3.228")) {
            for (String protocol : List.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")) {
                ExternalCluster cluster = new ExternalCluster();
                cluster.setId(UUID.randomUUID());
                cluster.setName(host + "-" + protocol);
                cluster.setBootstrapServers(host + ":9092");
                cluster.setKafkaClusterId("cluster-id");
                cluster.setSecurityProtocol(protocol);
                cluster.setSaslMechanism(protocol.startsWith("SASL_") ? "SCRAM-SHA-512" : null);
                cluster.setSaslUsername(protocol.startsWith("SASL_") ? "admin" : null);
                cluster.setSaslPasswordEncrypted(protocol.startsWith("SASL_") ? "encrypted-password" : null);
                cluster.setTruststorePath(protocol.endsWith("SSL") ? "/security/" + host + ".p12" : null);
                cluster.setTruststoreType(protocol.endsWith("SSL") ? "PKCS12" : null);
                cluster.setStatus("DEGRADED");
                clusters.add(cluster);
            }
        }

        when(externalClusterRepository.findByStatusNot("DELETED")).thenReturn(clusters);
        when(kafkaAdminService.inspectBootstrapServers(any(ExternalCluster.class),
                org.mockito.ArgumentMatchers.eq(true)))
                .thenReturn(Map.of("connected", true, "clusterId", "cluster-id"));
        for (ExternalCluster cluster : clusters) {
            when(discoveryAgentRepository.findByClusterId(cluster.getId())).thenReturn(List.of());
        }

        service.checkExternalClustersHealth();

        for (ExternalCluster cluster : clusters) {
            verify(kafkaAdminService).inspectBootstrapServers(cluster, true);
        }
        verify(kafkaAdminService, never()).inspectBootstrapServers(any(String.class));
    }

    @Test
    void rejectsDuplicateActiveClusterNameBeforeRegisteringExternalCluster() {
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterService service = service(
                clusterRepository,
                externalClusterRepository,
                mock(ExternalClusterNodeRepository.class),
                mock(DiscoveryAgentRepository.class),
                mock(KafkaAdminService.class));

        when(clusterRepository.existsActiveByNormalizedName("Test")).thenReturn(true);

        ExternalClusterService.BootstrapExternalClusterRequest request =
                new ExternalClusterService.BootstrapExternalClusterRequest();
        request.setName(" Test ");
        request.setBootstrapServers("192.168.3.208:9092");
        request.setClusterId("different-kafka-id");

        assertThatThrownBy(() -> service.registerBootstrapCluster(request))
                .isInstanceOf(ClusterNameConflictException.class)
                .hasMessage("A cluster with this name already exists. Choose a different name.");

        verify(clusterRepository).existsActiveByNormalizedName("Test");
        verify(externalClusterRepository, never()).save(any(ExternalCluster.class));
    }

    @Test
    void reportsUnusedExternalClusterNameAsAvailable() {
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterService service = service(
                clusterRepository,
                externalClusterRepository,
                mock(ExternalClusterNodeRepository.class),
                mock(DiscoveryAgentRepository.class),
                mock(KafkaAdminService.class));

        assertThat(service.isClusterNameAvailable(" new-cluster ")).isTrue();
        verify(clusterRepository).existsActiveByNormalizedName("new-cluster");
        verify(externalClusterRepository).existsActiveByNormalizedName("new-cluster");
    }

    @Test
    void recreatingDeletedExternalClusterCreatesFreshRowAndTimestamp() {
        UUID deletedId = UUID.randomUUID();
        UUID freshId = UUID.randomUUID();
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        DiscoveryAgentRepository discoveryAgentRepository = mock(DiscoveryAgentRepository.class);
        ExternalClusterService service = service(
                clusterRepository,
                externalClusterRepository,
                nodeRepository,
                discoveryAgentRepository,
                mock(KafkaAdminService.class));

        ExternalCluster deleted = new ExternalCluster();
        deleted.setId(deletedId);
        deleted.setName("external-test");
        deleted.setBootstrapServers("192.168.3.208:9092");
        deleted.setKafkaClusterId("kafka-id");
        deleted.setStatus("DELETED");
        deleted.setCreatedAt(Instant.parse("2024-01-01T00:00:00Z"));

        when(externalClusterRepository.findByStatus("DELETED")).thenReturn(java.util.List.of(deleted));
        when(externalClusterRepository.findById(deletedId)).thenReturn(Optional.of(deleted));
        when(discoveryAgentRepository.findByClusterId(deletedId)).thenReturn(java.util.List.of());
        when(externalClusterRepository.findByStatusNot("DELETED")).thenReturn(java.util.List.of());
        when(externalClusterRepository.findByBootstrapServersAndStatusNot("192.168.3.208:9092", "DELETED"))
                .thenReturn(Optional.empty());
        when(externalClusterRepository.findByNameAndStatusNot("external-test", "DELETED"))
                .thenReturn(Optional.empty());
        when(externalClusterRepository.save(any(ExternalCluster.class))).thenAnswer(invocation -> {
            ExternalCluster saved = invocation.getArgument(0);
            if (saved.getId() == null) {
                saved.setId(freshId);
            }
            return saved;
        });

        ExternalClusterService.BootstrapExternalClusterRequest request =
                new ExternalClusterService.BootstrapExternalClusterRequest();
        request.setName("external-test");
        request.setBootstrapServers("192.168.3.208:9092");
        request.setClusterId("kafka-id");
        request.setKafkaVersion("4.1.0");

        ExternalCluster recreated = service.registerBootstrapCluster(request);

        assertThat(recreated).isNotSameAs(deleted);
        assertThat(recreated.getId()).isEqualTo(freshId);
        assertThat(recreated.getCreatedAt()).isAfter(deleted.getCreatedAt());
        verify(clusterRepository).purgeById(deletedId);
        verify(externalClusterRepository).delete(deleted);
    }
    @Test
    void physicallyDeletesExternalClusterAndItsInventoryMirror() {
        UUID clusterId = UUID.randomUUID();
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        DiscoveryAgentRepository discoveryAgentRepository = mock(DiscoveryAgentRepository.class);
        ExternalClusterService service = service(
                clusterRepository,
                externalClusterRepository,
                nodeRepository,
                discoveryAgentRepository,
                mock(KafkaAdminService.class));

        ExternalCluster existing = new ExternalCluster();
        existing.setId(clusterId);
        existing.setName("external-test");
        existing.setBootstrapServers("192.168.3.208:9092");
        existing.setKafkaClusterId("kafka-id");
        when(externalClusterRepository.findById(clusterId)).thenReturn(Optional.of(existing));
        when(discoveryAgentRepository.findByClusterId(clusterId)).thenReturn(java.util.List.of());

        assertThat(service.deleteExternalCluster(clusterId)).contains(existing);

        verify(nodeRepository).deleteByClusterId(clusterId);
        verify(clusterRepository).purgeById(clusterId);
        verify(externalClusterRepository).delete(existing);
        verify(externalClusterRepository).flush();
        verify(externalClusterRepository, never()).save(existing);
    }

    @Test
    void nodeIdPreventsSameHostControllerReportFromOverwritingBrokerPaths() {
        ExternalClusterService service = service(mock(ExternalClusterRepository.class));
        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setHostname("192.168.3.228");
        agent.setIpAddresses("[\"192.168.3.228\"]");

        ExternalClusterService.ExternalDiscoveryReport controllerReport =
                new ExternalClusterService.ExternalDiscoveryReport();
        controllerReport.setNodeId(102);
        controllerReport.setHostname("192.168.3.228");

        ExternalClusterNode broker = new ExternalClusterNode();
        broker.setNodeId(2);
        broker.setHost("192.168.3.228");
        ExternalClusterNode controller = new ExternalClusterNode();
        controller.setNodeId(102);
        controller.setHost("192.168.3.228");

        assertThat(service.matchesDiscoveryNode(broker, controllerReport, agent)).isFalse();
        assertThat(service.matchesDiscoveryNode(controller, controllerReport, agent)).isTrue();
    }

    @Test
    void discoveryReportEnrichesUnknownControllerHostAndPort() {
        ExternalClusterRepository clusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        ExternalClusterService service = service(
                mock(ClusterRepository.class), clusterRepository, nodeRepository,
                mock(DiscoveryAgentRepository.class), mock(KafkaAdminService.class));

        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(UUID.randomUUID());
        ExternalClusterNode controller = new ExternalClusterNode();
        controller.setClusterId(cluster.getId());
        controller.setNodeId(102);
        controller.setHost("unknown");
        controller.setPort(0);
        controller.setIsController(true);
        when(nodeRepository.findByClusterId(cluster.getId())).thenReturn(List.of(controller));

        ExternalClusterService.ExternalDiscoveryReport report =
                new ExternalClusterService.ExternalDiscoveryReport();
        report.setNodeId(102);
        report.setHostname("192.168.3.228");
        report.setListeners("CONTROLLER://192.168.3.228:9093");

        ReflectionTestUtils.invokeMethod(service, "applyDiscoveryReportToNodes",
                cluster, report, new DiscoveryAgent());

        assertThat(controller.getHost()).isEqualTo("192.168.3.228");
        assertThat(controller.getPort()).isEqualTo(9093);
        verify(nodeRepository).save(controller);
    }

    @Test
    void discoveryReportPersistsConfiguredJmxExporterPort() {
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        ExternalClusterService service = service(
                mock(ClusterRepository.class),
                mock(ExternalClusterRepository.class),
                nodeRepository,
                mock(DiscoveryAgentRepository.class),
                mock(KafkaAdminService.class));

        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setKafkaMode("KRaft");

        ExternalClusterNode broker = new ExternalClusterNode();
        broker.setClusterId(clusterId);
        broker.setNodeId(2);
        broker.setHost("192.168.3.164");
        broker.setIsBroker(true);
        when(nodeRepository.findByClusterId(clusterId)).thenReturn(List.of(broker));

        ExternalClusterService.ExternalDiscoveryReport report =
                new ExternalClusterService.ExternalDiscoveryReport();
        report.setNodeId(2);
        report.setHostname("192.168.3.164");
        report.setJmxExporterPort(17071);

        ReflectionTestUtils.invokeMethod(service, "applyDiscoveryReportToNodes",
                cluster, report, new DiscoveryAgent());

        assertThat(broker.getJmxExporterPort()).isEqualTo(17071);
        verify(nodeRepository).save(broker);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testConnectionUsesMatchingDiscoveryReportsForDedicatedControllerEndpoints() {
        ExternalClusterService service = service(mock(ExternalClusterRepository.class));
        Map<String, ExternalClusterService.ExternalDiscoveryReport> pending =
                (Map<String, ExternalClusterService.ExternalDiscoveryReport>) ReflectionTestUtils.getField(
                        service, "pendingDiscoveries");

        List<Map<String, Object>> nodes = new java.util.ArrayList<>();
        for (int brokerId = 1; brokerId <= 3; brokerId++) {
            Map<String, Object> broker = new java.util.HashMap<>();
            broker.put("id", brokerId);
            broker.put("host", "192.168.3." + (brokerId == 1 ? "229" : brokerId == 2 ? "228" : "213"));
            broker.put("port", 9092);
            broker.put("isBroker", true);
            broker.put("isController", false);
            nodes.add(broker);
        }

        int[] controllerIds = {101, 102, 103};
        String[] controllerHosts = {"192.168.3.229", "192.168.3.228", "192.168.3.213"};
        for (int i = 0; i < controllerIds.length; i++) {
            Map<String, Object> controller = new java.util.HashMap<>();
            controller.put("id", controllerIds[i]);
            controller.put("host", "unknown");
            controller.put("port", 0);
            controller.put("isBroker", false);
            controller.put("isController", true);
            nodes.add(controller);

            ExternalClusterService.ExternalDiscoveryReport report =
                    new ExternalClusterService.ExternalDiscoveryReport();
            report.setKafkaClusterId("Mize1pYlS9u38RYMUDK4Ww");
            report.setNodeId(controllerIds[i]);
            report.setHostname("controller-" + controllerIds[i]);
            report.setProcessRoles("controller");
            report.setListeners("CONTROLLER://" + controllerHosts[i] + ":9093");
            report.setRunning(true);
            report.setLastSeen(OffsetDateTime.now().toString());
            pending.put("controller-" + controllerIds[i], report);
        }

        Map<String, Object> inspection = new java.util.HashMap<>();
        inspection.put("clusterId", "Mize1pYlS9u38RYMUDK4Ww");
        inspection.put("brokerCount", 3L);
        inspection.put("brokers", nodes);

        service.enrichTestConnectionNodesFromDiscovery(inspection);

        assertThat(inspection.get("brokerCount")).isEqualTo(3L);
        assertThat(nodes).filteredOn(node -> Integer.valueOf(102).equals(node.get("id")))
                .singleElement()
                .satisfies(node -> {
                    assertThat(node.get("host")).isEqualTo("192.168.3.228");
                    assertThat(node.get("port")).isEqualTo(9093);
                    assertThat(node.get("isBroker")).isEqualTo(false);
                    assertThat(node.get("isController")).isEqualTo(true);
                    assertThat(node.get("hasActiveAgent")).isEqualTo(true);
                });
    }

    @Test
    void testConnectionUsesPersistedInventoryWhenAgentsAreAlreadyLinked() {
        ExternalClusterRepository clusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        ExternalClusterService service = service(
                mock(ClusterRepository.class), clusterRepository, nodeRepository,
                mock(DiscoveryAgentRepository.class), mock(KafkaAdminService.class));

        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(UUID.randomUUID());
        cluster.setKafkaClusterId("Mize1pYlS9u38RYMUDK4Ww");
        ExternalClusterNode savedController = new ExternalClusterNode();
        savedController.setClusterId(cluster.getId());
        savedController.setNodeId(102);
        savedController.setHost("192.168.3.228");
        savedController.setPort(9093);
        savedController.setIsBroker(false);
        savedController.setIsController(true);
        when(clusterRepository.findByKafkaClusterId("Mize1pYlS9u38RYMUDK4Ww"))
                .thenReturn(Optional.of(cluster));
        when(nodeRepository.findByClusterId(cluster.getId())).thenReturn(List.of(savedController));

        Map<String, Object> controller = new java.util.HashMap<>();
        controller.put("id", 102);
        controller.put("host", "unknown");
        controller.put("port", 0);
        controller.put("isBroker", false);
        controller.put("isController", true);
        Map<String, Object> inspection = new java.util.HashMap<>();
        inspection.put("clusterId", "Mize1pYlS9u38RYMUDK4Ww");
        inspection.put("brokerCount", 3L);
        inspection.put("brokers", List.of(controller));

        service.enrichTestConnectionNodesFromDiscovery(inspection);

        assertThat(controller).containsEntry("host", "192.168.3.228")
                .containsEntry("port", 9093)
                .containsEntry("endpoint", "192.168.3.228:9093")
                .containsEntry("isBroker", false)
                .containsEntry("isController", true);
    }

    @Test
    void metricsUseNodeIdBeforeHostnameOrBootstrapFallback() {
        ExternalClusterRepository clusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        DiscoveryAgentRepository agentRepository = mock(DiscoveryAgentRepository.class);
        ExternalClusterService service = service(
                mock(ClusterRepository.class), clusterRepository, nodeRepository,
                agentRepository, mock(KafkaAdminService.class));

        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setStatus("SUCCESS");
        cluster.setBootstrapServers("192.168.3.228:9092");
        ExternalClusterNode nodeOne = new ExternalClusterNode();
        nodeOne.setClusterId(clusterId);
        nodeOne.setNodeId(1);
        nodeOne.setHost("192.168.3.229");
        ExternalClusterNode nodeTwo = new ExternalClusterNode();
        nodeTwo.setClusterId(clusterId);
        nodeTwo.setNodeId(2);
        nodeTwo.setHost("192.168.3.228");

        DiscoveryAgent linkedAgent = new DiscoveryAgent();
        linkedAgent.setId("host-229");
        linkedAgent.setHostname("192.168.3.229");
        linkedAgent.setClusterId(clusterId);
        when(agentRepository.findById("host-229")).thenReturn(Optional.of(linkedAgent));
        when(agentRepository.findByHostname("192.168.3.229")).thenReturn(Optional.of(linkedAgent));
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(nodeRepository.findByClusterId(clusterId)).thenReturn(List.of(nodeOne, nodeTwo));
        when(nodeRepository.findByClusterIdAndNodeId(clusterId, 1)).thenReturn(Optional.of(nodeOne));

        ExternalClusterService.ExternalBrokerMetricsDto metrics =
                new ExternalClusterService.ExternalBrokerMetricsDto();
        metrics.setHostId("host-229");
        metrics.setNodeId(1);
        metrics.setHostname("192.168.3.229");
        metrics.setBootstrap("192.168.3.229:9092");
        metrics.setDiskUsedBytes(7_784_919_040L);
        metrics.setDiskTotalBytes(44_286_992_384L);
        metrics.setMessagesInPerSec(0.1);
        metrics.setBytesInPerSec(7.7);

        service.receiveMetrics("external", metrics);

        assertThat(nodeOne.getDiskUsedBytes()).isEqualTo(7_784_919_040L);
        assertThat(nodeOne.getDiskTotalBytes()).isEqualTo(44_286_992_384L);
        assertThat(nodeOne.getMessagesInPerSec()).isEqualTo(0.1);
        assertThat(nodeOne.getBytesInPerSec()).isEqualTo(7.7);
        assertThat(nodeTwo.getDiskUsedBytes()).isNull();
        verify(nodeRepository).save(nodeOne);
    }

    @Test
    void linkedAgentReportUpdatesItsClusterWhenReportIdentityDoesNotMatchBootstrapOrName() {
        ExternalClusterRepository clusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        DiscoveryAgentRepository agentRepository = mock(DiscoveryAgentRepository.class);
        ExternalClusterService service = service(
                mock(ClusterRepository.class), clusterRepository, nodeRepository,
                agentRepository, mock(KafkaAdminService.class));

        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setName("test-ext");
        cluster.setBootstrapServers("192.168.3.164:9092");
        cluster.setKafkaMode("KRaft");
        cluster.setProcessRoles("controller");

        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setId("discovery-broker-229");
        agent.setClusterId(clusterId);

        ExternalClusterNode broker = new ExternalClusterNode();
        broker.setClusterId(clusterId);
        broker.setNodeId(1);
        broker.setHost("192.168.3.229");
        broker.setIsBroker(true);

        when(agentRepository.findById(agent.getId())).thenReturn(Optional.of(agent));
        when(agentRepository.save(agent)).thenReturn(agent);
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(clusterRepository.save(cluster)).thenReturn(cluster);
        when(nodeRepository.findByClusterId(clusterId)).thenReturn(List.of(broker));

        ExternalClusterService.ExternalDiscoveryReport report =
                new ExternalClusterService.ExternalDiscoveryReport();
        report.setHostId(agent.getId());
        report.setName("production-kafka");
        report.setBootstrapServers("192.168.3.229:9092");
        report.setKafkaClusterId("");
        report.setNodeId(1);
        report.setKafkaMode("ZooKeeper");
        report.setKafkaVersion("2.5.0");
        report.setProcessRoles("");
        report.setCpuUsagePct(12.5);
        report.setMemoryUsedMb(2048L);
        report.setMemoryTotalMb(8192L);
        report.setRunning(true);

        Map<String, Object> result = service.recordDiscoveryReport(report);

        assertThat(result).containsEntry("status", "registered")
                .containsEntry("id", clusterId);
        assertThat(broker.getCpuUsagePct()).isEqualTo(12.5);
        assertThat(broker.getMemoryUsedMb()).isEqualTo(2048L);
        assertThat(broker.getMemoryTotalMb()).isEqualTo(8192L);
        assertThat(cluster.getKafkaMode()).isEqualTo("ZooKeeper");
        assertThat(cluster.getKafkaVersion()).isEqualTo("2.5.0");
        assertThat(cluster.getProcessRoles()).isNull();
        verify(clusterRepository).save(cluster);
        verify(nodeRepository).save(broker);
        verify(clusterRepository, never()).findByBootstrapServersAndStatusNot(
                "192.168.3.229:9092", "DELETED");
    }

    @Test
    void unlinkedAgentUsesKnownTopologyHostWhenClusterNameAndBootstrapDiffer() {
        ExternalClusterRepository clusterRepository = mock(ExternalClusterRepository.class);
        ExternalClusterNodeRepository nodeRepository = mock(ExternalClusterNodeRepository.class);
        DiscoveryAgentRepository agentRepository = mock(DiscoveryAgentRepository.class);
        ExternalClusterService service = service(
                mock(ClusterRepository.class), clusterRepository, nodeRepository,
                agentRepository, mock(KafkaAdminService.class));

        UUID clusterId = UUID.randomUUID();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setName("test");
        cluster.setBootstrapServers("192.168.3.229:9092");

        ExternalClusterNode broker = new ExternalClusterNode();
        broker.setClusterId(clusterId);
        broker.setNodeId(2);
        broker.setHost("192.168.3.164");
        broker.setIsBroker(true);

        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setId("host-164");
        agent.setHostname("192.168.3.164");

        when(agentRepository.findById(agent.getId())).thenReturn(Optional.of(agent));
        when(agentRepository.save(agent)).thenReturn(agent);
        when(clusterRepository.findByBootstrapServersAndStatusNot(
                "192.168.3.164:9092", "DELETED")).thenReturn(Optional.empty());
        when(clusterRepository.findByStatusNot("DELETED")).thenReturn(List.of(cluster));
        when(clusterRepository.findByNameAndStatusNot("production-kafka", "DELETED"))
                .thenReturn(Optional.empty());
        when(nodeRepository.findByClusterId(clusterId)).thenReturn(List.of(broker));

        ExternalClusterService.ExternalDiscoveryReport report =
                new ExternalClusterService.ExternalDiscoveryReport();
        report.setHostId(agent.getId());
        report.setAgentName("tantor-agent-192.168.3.164");
        report.setName("production-kafka");
        report.setHostname("192.168.3.164");
        report.setIpAddresses("[\"192.168.3.164\"]");
        report.setBootstrapServers("192.168.3.164:9092");
        report.setKafkaClusterId("");
        report.setNodeId(2);
        report.setInstallPath("/opt/kafka_2.13-4.1.0");
        report.setConfigFile("/opt/kafka_2.13-4.1.0/config/broker.properties");
        report.setRunning(true);

        Map<String, Object> result = service.recordDiscoveryReport(report);

        assertThat(result).containsEntry("status", "registered")
                .containsEntry("id", clusterId);
        assertThat(agent.getClusterId()).isEqualTo(clusterId);
        assertThat(broker.getInstallDir()).isEqualTo("/opt/kafka_2.13-4.1.0");
        assertThat(broker.getConfigFile())
                .isEqualTo("/opt/kafka_2.13-4.1.0/config/broker.properties");
        verify(nodeRepository).save(broker);
    }

    @Test
    void staleDiscoveryAgentUsesConfiguredTimeoutAndReturnsOrangeDisconnectedState() {
        ExternalClusterRepository externalClusterRepository = mock(ExternalClusterRepository.class);
        DiscoveryAgentRepository discoveryAgentRepository = mock(DiscoveryAgentRepository.class);
        ExternalClusterService service = service(
                externalClusterRepository, discoveryAgentRepository, mock(KafkaAdminService.class));
        ReflectionTestUtils.setField(service, "discoveryAgentHeartbeatTimeoutSeconds", 45L);

        DiscoveryAgent agent = new DiscoveryAgent();
        agent.setId("agent-1");
        agent.setAgentName("external-agent");
        agent.setStatus("ONLINE");
        agent.setLastHeartbeat(OffsetDateTime.now().minusSeconds(60));
        when(discoveryAgentRepository.findAll()).thenReturn(List.of(agent));

        Map<String, Object> summary = service.listDiscoveryAgents().getFirst();

        assertThat(service.agentStaleSeconds()).isEqualTo(45L);
        assertThat(summary.get("fresh")).isEqualTo(false);
        assertThat(summary.get("health")).isEqualTo("orange");
        assertThat(summary.get("stateLabel")).isEqualTo("Agent disconnected - no recent heartbeat");
    }

    private ExternalClusterService service(ExternalClusterRepository externalClusterRepository) {
        return service(
                mock(ClusterRepository.class),
                externalClusterRepository,
                mock(ExternalClusterNodeRepository.class),
                mock(DiscoveryAgentRepository.class),
                mock(KafkaAdminService.class));
    }

    private ExternalClusterService service(
            ExternalClusterRepository externalClusterRepository,
            DiscoveryAgentRepository discoveryAgentRepository,
            KafkaAdminService kafkaAdminService) {
        return service(
                externalClusterRepository,
                discoveryAgentRepository,
                kafkaAdminService,
                mock(ActivityAlertService.class));
    }

    private ExternalClusterService service(
            ExternalClusterRepository externalClusterRepository,
            DiscoveryAgentRepository discoveryAgentRepository,
            KafkaAdminService kafkaAdminService,
            ActivityAlertService activityAlertService) {
        return service(
                mock(ClusterRepository.class),
                externalClusterRepository,
                mock(ExternalClusterNodeRepository.class),
                discoveryAgentRepository,
                kafkaAdminService,
                activityAlertService);
    }

    private ExternalClusterService service(
            ClusterRepository clusterRepository,
            ExternalClusterRepository externalClusterRepository,
            ExternalClusterNodeRepository nodeRepository,
            DiscoveryAgentRepository discoveryAgentRepository,
            KafkaAdminService kafkaAdminService) {
        return service(clusterRepository, externalClusterRepository, nodeRepository,
                discoveryAgentRepository, kafkaAdminService, mock(ActivityAlertService.class));
    }

    private ExternalClusterService service(
            ClusterRepository clusterRepository,
            ExternalClusterRepository externalClusterRepository,
            ExternalClusterNodeRepository nodeRepository,
            DiscoveryAgentRepository discoveryAgentRepository,
            KafkaAdminService kafkaAdminService,
            ActivityAlertService activityAlertService) {
        return new ExternalClusterService(
                clusterRepository,
                externalClusterRepository,
                nodeRepository,
                mock(ClusterServiceAssignmentRepository.class),
                mock(HostRepository.class),
                discoveryAgentRepository,
                kafkaAdminService,
                new ObjectMapper(),
                activityAlertService,
                mock(AuditService.class),
                mock(EncryptionService.class),
                mock(TruststoreStorageService.class),
                mock(PrometheusMonitoringService.class)
        );
    }
}
