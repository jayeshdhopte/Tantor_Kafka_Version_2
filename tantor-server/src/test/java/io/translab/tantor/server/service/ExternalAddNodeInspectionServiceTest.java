package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.*;
import org.springframework.web.server.ResponseStatusException;

class ExternalAddNodeInspectionServiceTest {
  final DiscoveryAgentRepository agents = mock(DiscoveryAgentRepository.class);
  final ExternalClusterRepository clusters = mock(ExternalClusterRepository.class);
  final ExternalClusterNodeRepository nodes = mock(ExternalClusterNodeRepository.class);
  final HostRepository internal = mock(HostRepository.class);
  final JdbcTemplate jdbc = mock(JdbcTemplate.class);
  final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  final AddNodeInspectionProperties scope = new AddNodeInspectionProperties();
  final ExternalAddNodeInspectionService service =
      new ExternalAddNodeInspectionService(agents, clusters, nodes, internal, scope, jdbc, mapper);
  final DiscoveryAgent agent = new DiscoveryAgent();
  final ExternalCluster cluster = new ExternalCluster();
  final String version = "3.4.0-fixture-an.1";

  ExternalAddNodeInspectionService.Observation observation(
      int processes, boolean complete, boolean execution) {
    return new ExternalAddNodeInspectionService.Observation(
        ExternalAddNodeInspectionService.SCHEMA,
        "external-fresh",
        version,
        Instant.now(),
        List.of("192.0.2.10"),
        true,
        true,
        complete,
        processes,
        0,
        0,
        execution);
  }

  @BeforeEach
  void setup() throws Exception {
    agent.setId("external-fresh");
    agent.setHostname("fresh-node");
    agent.setVersion(version);
    agent.setCanExecuteTasks(true);
    agent.setStatus("ONLINE");
    agent.setLastHeartbeat(OffsetDateTime.now());
    agent.setIpAddresses("[\"192.0.2.10\"]");
    cluster.setId(UUID.randomUUID());
    cluster.setStatus("CONNECTED");
    cluster.setKafkaMode("KRaft");
    cluster.setKafkaVersion("4.3.0");
    when(agents.findById(agent.getId())).thenReturn(Optional.of(agent));
    when(agents.findAll()).thenReturn(List.of(agent));
    when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));
    when(internal.findAll()).thenReturn(List.of());
    when(nodes.findAll()).thenReturn(List.of());
    scope.setAllowedHostIds(List.of(agent.getId()));
    report(observation(0, true, false));
  }

  @SuppressWarnings("unchecked")
  void report(ExternalAddNodeInspectionService.Observation report) throws Exception {
    when(jdbc.query(anyString(), any(RowMapper.class)))
        .thenAnswer(call -> List.of(Map.entry(agent.getId(), mapper.writeValueAsString(report))));
  }

  ExternalAddNodeInspectionService.Candidate candidate() {
    return service.candidates(cluster.getId()).candidates().getFirst();
  }

  @Test
  void clearHostIsObservationOnlyAndNeverExecutionAdmission() {
    var result = service.candidates(cluster.getId());
    assertFalse(result.executionEnabled());
    assertEquals("HOST_INSPECTION_ONLY", result.phase());
    assertEquals("HOST_OBSERVED", candidate().status());
    assertFalse(candidate().executionEnabled());
    verify(jdbc, never()).update(anyString(), any(Object[].class));
  }

  @Test
  void staleOrChangedHeartbeatCannotReuseInspection() {
    agent.setLastHeartbeat(OffsetDateTime.now().minusMinutes(3));
    assertEquals("BLOCKED", candidate().status());
    agent.setLastHeartbeat(OffsetDateTime.now());
    agent.setVersion("3.4.0-old");
    assertEquals("BLOCKED", candidate().status());
  }

  @Test
  void existingKafkaOrIncompleteInventoryBlocks() throws Exception {
    report(observation(1, true, false));
    assertEquals("BLOCKED", candidate().status());
    report(observation(0, false, false));
    assertEquals("BLOCKED", candidate().status());
  }

  @Test
  void assignedAgentAndInternalIdentityCannotBeFreshCandidate() {
    agent.setClusterId(cluster.getId());
    assertEquals("BLOCKED", candidate().status());
    agent.setClusterId(null);
    var host = new Host();
    host.setId("internal-other");
    host.setHostIp("192.0.2.10");
    when(internal.findAll()).thenReturn(List.of(host));
    assertEquals("BLOCKED", candidate().status());
  }

  @Test
  void reservedCandidateBlocksAnotherPreviewButAllowsItsOwnFinalChecks() {
    var guard = mock(ExternalBrokerAdditionGuard.class);
    org.springframework.test.util.ReflectionTestUtils.setField(
        service, "externalBrokerGuard", guard);
    when(guard.reservedAgentExcept(agent.getId(), null)).thenReturn(true);
    assertEquals("BLOCKED", candidate().status());
    UUID owner = UUID.randomUUID();
    when(guard.reservedAgentExcept(agent.getId(), owner)).thenReturn(false);
    assertEquals(
        "HOST_OBSERVED",
        service.candidates(cluster.getId(), owner).candidates().getFirst().status());
  }

  @Test
  void excludedAddressAndUnapprovedScopeBlock() {
    scope.setExcludedHostIps(List.of("192.0.2.10"));
    assertEquals("BLOCKED", candidate().status());
    scope.setExcludedHostIps(List.of());
    scope.setAllowedHostIds(List.of());
    assertEquals("BLOCKED", candidate().status());
  }

  @Test
  void incompatibleIdentityBlocks() {
    agent.setIpAddresses("[\"192.0.2.99\"]");
    assertEquals("BLOCKED", candidate().status());
  }

  @Test
  void executionClaimsMalformedAddressesAndExpiredReportsAreRejected() throws Exception {
    assertThrows(ResponseStatusException.class, () -> service.record(observation(0, true, true)));
    var node = mapper.valueToTree(observation(0, true, false));
    ((com.fasterxml.jackson.databind.node.ObjectNode) node)
        .put("observedAt", Instant.now().minusSeconds(121).toString());
    var expired = mapper.treeToValue(node, ExternalAddNodeInspectionService.Observation.class);
    assertThrows(ResponseStatusException.class, () -> service.record(expired));
    ((com.fasterxml.jackson.databind.node.ObjectNode) node)
        .put("observedAt", Instant.now().toString());
    ((com.fasterxml.jackson.databind.node.ObjectNode) node)
        .set("addresses", mapper.valueToTree(List.of("hostname.example")));
    var dns = mapper.treeToValue(node, ExternalAddNodeInspectionService.Observation.class);
    assertThrows(ResponseStatusException.class, () -> service.record(dns));
  }

  @Test
  void reportRequiresMatchingLiveAgentVersion() {
    agent.setVersion("3.4.0-old");
    assertThrows(ResponseStatusException.class, () -> service.record(observation(0, true, false)));
  }

  @Test
  void sameHostWithADifferentDiscoveryIdentityIsNotFresh() {
    var n = new ExternalClusterNode();
    n.setAgentId("old-external-id");
    n.setHost("192.0.2.10");
    when(nodes.findAll()).thenReturn(List.of(n));
    assertEquals("BLOCKED", candidate().status());
  }

  @Test
  void validReportPersistsOnlyBoundedObservation() {
    service.record(observation(0, true, false));
    verify(jdbc)
        .update(
            startsWith("INSERT INTO kf_external_add_node_observations"),
            eq(agent.getId()),
            any(java.sql.Timestamp.class),
            anyString());
    verifyNoInteractions(internal, nodes);
  }

  @Test
  void equivalentIpv6AddressesCannotBypassExclusions() throws Exception {
    var r =
        new ExternalAddNodeInspectionService.Observation(
            ExternalAddNodeInspectionService.SCHEMA,
            agent.getId(),
            version,
            Instant.now(),
            List.of("2001:db8::10"),
            true,
            true,
            true,
            0,
            0,
            0,
            false);
    report(r);
    agent.setIpAddresses("[\"2001:db8:0:0:0:0:0:10\"]");
    assertEquals("HOST_OBSERVED", candidate().status());
    scope.setExcludedHostIps(List.of("2001:db8:0:0:0:0:0:10"));
    assertEquals("BLOCKED", candidate().status());
  }
}
