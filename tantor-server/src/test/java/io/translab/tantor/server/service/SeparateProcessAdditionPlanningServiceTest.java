package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeReadinessProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.*;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SeparateProcessAdditionPlanningServiceTest {
    final ClusterRepository clusters = mock(ClusterRepository.class);
    final HostRepository hosts = mock(HostRepository.class);
    final ClusterServiceAssignmentRepository assignments = mock(ClusterServiceAssignmentRepository.class);
    final HostStatusService connectivity = mock(HostStatusService.class);
    final JobService jobs = mock(JobService.class);
    final AddNodeKafkaReadiness kafka = mock(AddNodeKafkaReadiness.class);
    final AddNodeArtifactCatalog artifacts = mock(AddNodeArtifactCatalog.class);
    final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    final ControllerAdditionPlanningService controllers = new ControllerAdditionPlanningService(clusters, hosts, assignments, connectivity, jobs, kafka, artifacts, new AddNodeReadinessProperties(), mapper);
    final SeparateProcessAdditionPlanningService planning = new SeparateProcessAdditionPlanningService(controllers, clusters, mapper);
    final Cluster cluster = new Cluster();
    final Host host = new Host();

    @BeforeEach void setup() {
        cluster.setId(UUID.randomUUID()); cluster.setMode("kraft"); cluster.setStatus("SUCCESS");
        cluster.setKafkaVersion("4.3.0"); cluster.setKafkaClusterId("abcdefghijklmnopqrstuv"); cluster.setConfigJson("{\"listener_port\":9092}");
        members(1, 2, 101);
        host.setId("fresh-pair"); host.setHostIp("192.0.2.20"); host.setAgentVersion("addnode.5");
        when(hosts.findById(host.getId())).thenReturn(Optional.of(host));
        when(clusters.findWithServicesById(cluster.getId())).thenReturn(Optional.of(cluster));
        when(connectivity.isInfrastructureHost(host)).thenReturn(true);
        when(connectivity.agentConnectivityStatus(host)).thenReturn("ONLINE");
        when(artifacts.resolve("4.3.0", "")).thenReturn(new AddNodeArtifactCatalog.Artifact("approved", "4.3.0", "a".repeat(64)));
        observe("DYNAMIC", Set.of(1, 2, 101), List.of("192.0.2.1:9093"));
    }
    void members(int broker1, int broker2, int controller) {
        var services = new ArrayList<ClusterServiceAssignment>();
        for (int i = 0; i < 3; i++) {
            var member = new ClusterServiceAssignment(); member.setHostId("existing-" + i); member.setRole(i == 2 ? "controller" : "broker");
            member.setNodeId(new int[]{broker1, broker2, controller}[i]); member.setConfigJson("{}"); services.add(member);
        }
        cluster.setServices(services); cluster.setNodeIds(List.of(broker1, broker2, controller));
    }
    void observe(String mode, Set<Integer> ids, List<String> endpoints) {
        var checks = List.of("C01", "C02", "C03", "C05", "C06", "C07", "C08", "B09").stream()
                .map(code -> new Check(code, code, "COMMON", "PASS", "expected", "observed", "", Instant.now(), "KAFKA")).toList();
        when(kafka.observe(eq(cluster), anyMap())).thenReturn(new AddNodeKafkaReadiness.Observation(checks, ids, List.of("192.0.2.1:9092"), endpoints, mode));
    }
    SeparateProcessAdditionPlanningService.Review review() { return planning.review(cluster.getId(), host.getId(), "rack1"); }
    String status(SeparateProcessAdditionPlanningService.Review review, String code) { return review.checks().stream().filter(c -> c.code().equals(code)).findFirst().orElseThrow().status(); }

    @Test void dynamicPairHasTwoRoleOnlyProcessesAndNeverCreatesJobsOrAssignments() {
        var r = review(); assertEquals("PLANNING_ONLY", r.status()); assertFalse(r.executionEnabled()); assertFalse(r.maintenanceRequired());
        assertEquals(cluster.getKafkaClusterId(), r.kafkaClusterId()); assertEquals("4.3.0", r.kafkaVersion()); assertEquals(2, r.processes().size());
        var controller = r.processes().get(0); var broker = r.processes().get(1);
        assertEquals("controller", controller.processRoles()); assertEquals("broker", broker.processRoles());
        assertEquals(102, controller.nodeId()); assertEquals(3, broker.nodeId());
        assertEquals("controller.properties", controller.configurationFile()); assertEquals("broker.properties", broker.configurationFile());
        assertNotEquals(controller.serviceName(), broker.serviceName()); assertEquals(9093, controller.listenerPort()); assertEquals(9092, broker.listenerPort());
        assertTrue(Collections.disjoint(controller.requiredDirectories(), broker.requiredDirectories()));
        assertEquals("PASS", status(r, "S01")); assertEquals("UNKNOWN", status(r, "S03")); assertEquals("UNKNOWN", status(r, "S04"));
        assertTrue(r.processes().stream().allMatch(p -> p.evidenceStatus().equals("NOT_INSPECTED")));
        assertEquals("P_PUBLISH", r.steps().getLast().code());
        assertTrue(r.steps().stream().filter(s -> s.code().equals("P4")).findFirst().orElseThrow().name().contains("after controller verification"));
        assertTrue(r.recoveryRules().stream().anyMatch(rule -> rule.checkpoint().equals("Controller verified; broker failed") && rule.requirement().contains("both reservations")));
        verify(clusters, never()).save(any()); verify(hosts, never()).save(any()); verify(assignments, never()).save(any());
        verify(jobs, never()).createJob(any(), anyList()); verify(jobs, never()).assertClusterMutable(any());
    }
    @Test void staticCombinedPlanFormatsBothCandidateTreesBeforeControllerAndBrokerStart() {
        observe("STATIC", Set.of(1, 2, 101), List.of("192.0.2.1:9093")); var r = review();
        assertTrue(r.maintenanceRequired()); assertFalse(r.executionEnabled());
        var codes = r.steps().stream().map(ControllerAdditionPlanningService.Step::code).toList();
        assertTrue(codes.indexOf("S4") < codes.indexOf("S5")); assertTrue(codes.indexOf("S5") < codes.indexOf("P_BROKER"));
        assertTrue(r.steps().stream().anyMatch(s -> s.code().equals("S4") && s.name().contains("two candidate storage trees")));
        assertEquals(1, r.steps().stream().filter(s -> s.name().startsWith("Publish")).count()); assertEquals("P_PUBLISH", codes.getLast());
    }
    @Test void unknownQuorumNeverInventsAWorkflow() {
        observe("UNKNOWN", Set.of(1, 2, 101), List.of()); var r = review(); assertTrue(r.steps().isEmpty()); assertFalse(r.executionEnabled());
    }
    @Test void observedIdentityCollisionBlocksInsteadOfSkippingBrokerThree() {
        observe("DYNAMIC", Set.of(1, 2, 3, 101), List.of("192.0.2.1:9093")); var r = review();
        assertEquals(3, r.processes().get(1).nodeId()); assertEquals("FAIL", status(r, "S01"));
        assertTrue(r.blockers().stream().anyMatch(b -> b.contains("already owned")));
    }
    @Test void theTwoNewIdsCannotCollideEvenWhenNeitherWasPreviouslyAllocated() {
        members(1, 100, 50); observe("DYNAMIC", Set.of(1, 100, 50), List.of("192.0.2.1:9093")); var r = review();
        assertEquals(101, r.processes().get(0).nodeId()); assertEquals(101, r.processes().get(1).nodeId());
        assertEquals("FAIL", status(r, "S01")); assertTrue(r.blockers().stream().anyMatch(b -> b.contains("proposals collide")));
    }
    @Test void exhaustedIdentitySpaceProducesNoNegativeProcessId() {
        members(1, Integer.MAX_VALUE, 101); observe("DYNAMIC", Set.of(1, Integer.MAX_VALUE, 101), List.of("192.0.2.1:9093"));
        var r = review(); assertTrue(r.processes().stream().allMatch(p -> p.nodeId() == null)); assertEquals("FAIL", status(r, "S01"));
    }
    @Test void overlappingOrInconsistentControllerPortsAreBlocked() {
        observe("DYNAMIC", Set.of(1, 2, 101), List.of("192.0.2.1:9092")); assertEquals("FAIL", status(review(), "S02"));
        observe("DYNAMIC", Set.of(1, 2, 101), List.of("192.0.2.1:9093", "192.0.2.2:9094")); assertEquals("FAIL", status(review(), "S02"));
        cluster.setConfigJson("{\"listener_port\":9092,\"controller_port\":\"invalid\"}");
        observe("DYNAMIC", Set.of(1, 2, 101), List.of("192.0.2.1:9093")); assertEquals("FAIL", status(review(), "S02"));
    }
    @Test void contextBindsBothRolesRackAndLiveControllerEndpoints() {
        var first = review(); assertNotEquals(first.contextHash(), planning.review(cluster.getId(), host.getId(), "rack2").contextHash());
        observe("DYNAMIC", Set.of(1, 2, 101), List.of("192.0.2.10:9093")); assertNotEquals(first.contextHash(), review().contextHash());
        assertTrue(first.expiresAt().isAfter(first.observedAt()));
    }
    @Test void occupiedHostAndExistingClusterJobRetainBlockers() {
        host.setClusterId(cluster.getId()); when(jobs.getActiveClusterJobs()).thenReturn(List.of(Map.of("clusterId", cluster.getId().toString())));
        var r = review(); assertTrue(r.blockers().stream().anyMatch(b -> b.contains("fresh unassigned")));
        assertTrue(r.blockers().stream().anyMatch(b -> b.contains("retained reservation"))); assertFalse(r.executionEnabled());
    }
    @Test void invalidRackAndExternalClusterAreRejectedWithoutKafkaObservation() {
        assertThrows(org.springframework.web.server.ResponseStatusException.class, () -> planning.review(cluster.getId(), host.getId(), "rack/unsafe"));
        cluster.setOriginType("EXTERNAL"); assertThrows(org.springframework.web.server.ResponseStatusException.class, this::review); verifyNoInteractions(kafka);
    }
}
