package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.server.ResponseStatusException;

class ExternalAddNodeReadinessServiceTest {
  final ExternalAddNodeInspectionService inspections = mock(ExternalAddNodeInspectionService.class);
  final ExternalClusterRepository clusters = mock(ExternalClusterRepository.class);
  final ExternalClusterNodeRepository nodes = mock(ExternalClusterNodeRepository.class);
  final DiscoveryAgentRepository agents = mock(DiscoveryAgentRepository.class);
  final ExternalKafkaManagement external = mock(ExternalKafkaManagement.class);
  final AddNodeKafkaReadiness live = mock(AddNodeKafkaReadiness.class);
  final KafkaAdminService kafka = mock(KafkaAdminService.class);
  final AddNodeArtifactCatalog artifacts = mock(AddNodeArtifactCatalog.class);
  final ExternalAddNodeConflictObservation conflicts =
      mock(ExternalAddNodeConflictObservation.class);
  final JdbcTemplate jdbc = mock(JdbcTemplate.class);
  final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  final AddNodeReadinessProperties policy = new AddNodeReadinessProperties();
  final AddNodeInspectionProperties scope = new AddNodeInspectionProperties();
  final ExternalAddNodeReadinessService service =
      new ExternalAddNodeReadinessService(
          inspections,
          clusters,
          nodes,
          agents,
          external,
          live,
          kafka,
          artifacts,
          policy,
          scope,
          conflicts,
          jdbc,
          mapper);
  final UUID clusterId = UUID.randomUUID(), previewId = UUID.randomUUID();
  final DiscoveryAgent agent = new DiscoveryAgent();
  final ExternalCluster cluster = new ExternalCluster();

  static AddNodeDeploymentSettings settings() {
    return new AddNodeDeploymentSettings(
        "/opt/tantor/kafka",
        "/opt/tantor/kafka/config",
        "/usr/lib/jvm/java-21-openjdk",
        "kafka",
        List.of("/data/broker/data"),
        "",
        "/var/log/tantor/broker",
        "1G",
        List.of("/data/controller/data"),
        "/data/controller/metadata",
        "/var/log/tantor/controller",
        "1G");
  }

  static ExternalAddNodeReadinessService.Request request() {
    return new ExternalAddNodeReadinessService.Request(
        "candidate", "192.0.2.10", "/opt/tantor/parcels", "", 9092, "rack1", settings());
  }

  @BeforeEach
  void setup() {
    cluster.setId(clusterId);
    cluster.setKafkaMode("KRaft");
    cluster.setKafkaVersion("4.3.0");
    cluster.setKafkaClusterId("existing-id");
    cluster.setBootstrapServers("192.0.2.20:9092");
    cluster.setSecurityProtocol("PLAINTEXT");
    cluster.setStatus("CONNECTED");
    agent.setId("candidate");
    agent.setHostname("candidate");
    agent.setVersion("3.4.0-fixture-an.1");
    agent.setStatus("ONLINE");
    agent.setLastHeartbeat(OffsetDateTime.now());
    agent.setIpAddresses("[\"192.0.2.10\"]");
    agent.setCanExecuteTasks(true);
    when(agents.findById(agent.getId())).thenReturn(Optional.of(agent));
    when(agents.findAll()).thenReturn(List.of(agent));
    when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
    when(nodes.findByClusterId(clusterId)).thenReturn(List.of());
    when(inspections.candidates(clusterId))
        .thenReturn(
            new ExternalAddNodeInspectionService.Review(
                "schema",
                clusterId,
                "HOST_INSPECTION_ONLY",
                false,
                List.of(
                    new ExternalAddNodeInspectionService.Candidate(
                        "candidate",
                        "candidate",
                        Instant.now(),
                        "BLOCKED",
                        false,
                        false,
                        List.of("Old agent or host ownership conflict"))),
                List.of()));
  }

  @Test
  void profileRejectsTraversalAndInvalidListener() {
    assertDoesNotThrow(() -> ExternalAddNodeReadinessService.validate(request()));
    var r = request();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ExternalAddNodeReadinessService.validate(
                new ExternalAddNodeReadinessService.Request(
                    r.agentId(),
                    "hostname",
                    r.parcelDir(),
                    "",
                    r.brokerPort(),
                    r.brokerRack(),
                    r.deploymentSettings())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ExternalAddNodeReadinessService.validate(
                new ExternalAddNodeReadinessService.Request(
                    r.agentId(),
                    r.hostIp(),
                    "/opt/../etc",
                    "",
                    9092,
                    "rack1",
                    r.deploymentSettings())));
  }

  @Test
  void hostBlockerCreatesNoKafkaConnectionOrTask() throws Exception {
    // create() reads back its persisted preview; a missing mock row is expected here.
    assertThrows(ResponseStatusException.class, () -> service.create(clusterId, request()));
    verifyNoInteractions(kafka, live, external, artifacts);
    verify(jdbc, never())
        .update(startsWith("insert into kf_external_add_node_tasks"), any(Object[].class));
    verify(jdbc)
        .update(
            startsWith("insert into kf_external_add_node_previews"),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any());
  }

  @Test
  void expiredPreviewCannotBecomePass() throws Exception {
    var row = new HashMap<String, Object>();
    row.put("request", mapper.writeValueAsString(request()));
    row.put("checks", "[]");
    row.put("context_hash", "a".repeat(64));
    row.put("created_at", Timestamp.from(Instant.now().minusSeconds(200)));
    row.put("expires_at", Timestamp.from(Instant.now().minusSeconds(1)));
    when(jdbc.queryForList(
            startsWith("select * from kf_external_add_node_previews"),
            any(UUID.class),
            any(UUID.class)))
        .thenReturn(List.of(row));
    var result = service.get(clusterId, previewId);
    assertEquals("EXPIRED", result.status());
    assertFalse(result.deploymentEnabled());
    assertEquals(33, result.checks().size());
    verifyNoInteractions(kafka, live, artifacts);
  }

  @Test
  void contextChangeInvalidatesPreviousPass() throws Exception {
    var row = new HashMap<String, Object>();
    row.put("request", mapper.writeValueAsString(request()));
    row.put("checks", "[]");
    row.put("context_hash", "previous-context");
    row.put("created_at", Timestamp.from(Instant.now()));
    row.put("expires_at", Timestamp.from(Instant.now().plusSeconds(120)));
    when(jdbc.queryForList(
            startsWith("select * from kf_external_add_node_previews"),
            any(UUID.class),
            any(UUID.class)))
        .thenReturn(List.of(row));
    var result = service.get(clusterId, previewId);
    assertEquals("STALE", result.status());
    assertFalse(result.deploymentEnabled());
    verifyNoInteractions(kafka, live, artifacts);
  }

  @Test
  void reportRejectsMissingDuplicateStaleAndWrongContextEvidence() {
    String hash = "a".repeat(64);
    Map<String, Object> request =
        Map.of("contextHash", hash, "contextStartedAt", Instant.now().minusSeconds(5).toString());
    var report =
        mapper
            .createObjectNode()
            .put("schemaVersion", "add-node/v1")
            .put("hostId", "candidate")
            .put("contextHash", hash);
    var checks = report.putArray("checks");
    for (String code : ExternalAddNodeReadinessService.AGENT_CODES)
      checks
          .addObject()
          .put("code", code)
          .put("status", "PASS")
          .put("observedAt", Instant.now().toString());
    var result =
        new ExternalAddNodeReadinessService.Result(UUID.randomUUID(), "SUCCESS", report, null);
    assertDoesNotThrow(
        () -> ExternalAddNodeReadinessService.validateReport(result, "candidate", request));
    checks.add(checks.get(0).deepCopy());
    assertThrows(
        ResponseStatusException.class,
        () -> ExternalAddNodeReadinessService.validateReport(result, "candidate", request));
    checks.remove(checks.size() - 1);
    ((com.fasterxml.jackson.databind.node.ObjectNode) checks.get(0))
        .put("observedAt", Instant.now().minusSeconds(300).toString());
    assertThrows(
        ResponseStatusException.class,
        () -> ExternalAddNodeReadinessService.validateReport(result, "candidate", request));
    report.put("contextHash", "wrong");
    assertThrows(
        ResponseStatusException.class,
        () -> ExternalAddNodeReadinessService.validateReport(result, "candidate", request));
  }

  @Test
  void unsupportedClusterAndOldAgentCannotDispatchInspection() {
    cluster.setKafkaVersion("3.9.0");
    assertThrows(ResponseStatusException.class, () -> service.create(clusterId, request()));
    cluster.setKafkaVersion("4.3.0");
    agent.setVersion("3.4.0-old");
    assertThrows(ResponseStatusException.class, () -> service.claim(agent.getId()));
    verifyNoInteractions(kafka, live, artifacts);
  }

  @Test
  void canonicalAddressesRejectLoopbackAndCompareIpv6() {
    assertEquals("", ExternalAddNodeReadinessService.canonical("127.0.0.1"));
    assertEquals("", ExternalAddNodeReadinessService.canonical("hostname"));
    assertEquals(
        ExternalAddNodeReadinessService.canonical("2001:db8::1"),
        ExternalAddNodeReadinessService.canonical("2001:db8:0:0:0:0:0:1"));
  }

  @Test
  void recoveryBindingSurvivesRestartAndCompatibleUpgradeButRejectsIdentityDrift() {
    String frozen = service.executionBinding(clusterId, request());
    agent.setLastHeartbeat(OffsetDateTime.now().plusSeconds(10));
    agent.setVersion("3.4.0-fixture-an.3");
    assertEquals(frozen, service.executionBinding(clusterId, request()));
    var unrelated = new DiscoveryAgent();
    unrelated.setId("unrelated");
    when(agents.findAll()).thenReturn(List.of(agent, unrelated));
    assertEquals(frozen, service.executionBinding(clusterId, request()));
    agent.setHostname("different-machine");
    assertNotEquals(frozen, service.executionBinding(clusterId, request()));
    agent.setHostname("candidate");
    agent.setClusterId(UUID.randomUUID());
    assertNotEquals(frozen, service.executionBinding(clusterId, request()));
    agent.setClusterId(null);
    cluster.setBootstrapServers("192.0.2.99:9092");
    assertNotEquals(frozen, service.executionBinding(clusterId, request()));
    agent.setIpAddresses("[\"192.0.2.99\"]");
    assertThrows(
        ResponseStatusException.class, () -> service.executionBinding(clusterId, request()));
  }
}
