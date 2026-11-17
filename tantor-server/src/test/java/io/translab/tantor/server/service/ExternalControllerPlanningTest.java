package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Contract fixtures exercise the actual public planner and strict shared native-plan validation.
 */
class ExternalControllerPlanningTest {
  final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  final ExternalAddNodeReadinessService ready = mock(ExternalAddNodeReadinessService.class);
  final ExternalControllerTasks tasks = mock(ExternalControllerTasks.class);
  final ExternalClusterRepository clusters = mock(ExternalClusterRepository.class);
  final ExternalClusterNodeRepository nodes = mock(ExternalClusterNodeRepository.class);
  final AddNodeArtifactCatalog catalog = mock(AddNodeArtifactCatalog.class);
  final KafkaExpansionQuorum quorum =
      spy(new KafkaExpansionQuorum(new AddNodeInspectionProperties()));
  final JdbcTemplate jdbc = mock(JdbcTemplate.class);
  final JobService jobs = mock(JobService.class);
  final List<Map<String, Object>> observations = new ArrayList<>();
  final Map<UUID, JsonNode> reports = new HashMap<>();
  final KafkaExpansionPlan.Frozen base;
  final ExternalControllerPlanning planner;
  KafkaExpansionPlan.Frozen queued;
  final Map<String, Object> previewRow;

  ExternalControllerPlanningTest() throws Exception {
    base = ExternalControllerRecoveryTest.frozen();
    var p = base.plan();
    var common =
        new ExternalAddNodeReadinessService.Request(
            p.candidateHostId(),
            p.candidateIp(),
            p.parcelDir(),
            p.artifactId().toString(),
            9092,
            "",
            p.deploymentSettings());
    var runtime =
        new KafkaExpansionRuntimeInspection(
            mock(AddNodeProcessInspectionService.class, CALLS_REAL_METHODS),
            mock(HostParcelRepository.class),
            mock(TaskRepository.class),
            catalog,
            new ArtifactRepositoryProperties(),
            mapper);
    planner =
        new ExternalControllerPlanning(
            ready,
            tasks,
            mock(DiscoveryAgentRepository.class),
            clusters,
            nodes,
            mock(ExternalKafkaManagement.class),
            catalog,
            quorum,
            mock(ExternalControllerRelease.class),
            runtime,
            new AddNodeReadinessProperties(),
            new KafkaExpansionProperties(),
            new KafkaDeploymentProperties(),
            new ArtifactRepositoryProperties(),
            new AddNodeInspectionProperties(),
            jobs,
            jdbc,
            mapper);
    var row = new HashMap<String, Object>();
    previewRow = row;
    row.put("id", p.operationId());
    row.put("cluster_id", p.clusterId());
    row.put("agent_id", p.candidateHostId());
    row.put("context_hash", p.contextHash());
    row.put("binding_hash", "b".repeat(64));
    row.put(
        "request",
        mapper.writeValueAsString(
            new ExternalControllerPlanning.Request(UUID.randomUUID(), 9093, Map.of())));
    row.put("common_request", mapper.writeValueAsString(common));
    row.put("expires_at", Timestamp.from(Instant.now().plusSeconds(120)));
    when(jdbc.queryForMap(anyString(), eq(p.operationId()), eq(p.clusterId()))).thenReturn(row);
    when(jdbc.queryForList(anyString(), eq(p.operationId()))).thenReturn(observations);
    when(ready.executionBinding(p.clusterId(), common)).thenReturn("b".repeat(64));
    when(tasks.parse(any(), any()))
        .thenAnswer(
            call ->
                mapper.readValue(
                    String.valueOf((Object) call.getArgument(0)), (Class<?>) call.getArgument(1)));
    when(tasks.json(any())).thenAnswer(call -> mapper.writeValueAsString(call.getArgument(0)));
    when(tasks.report(any())).thenAnswer(call -> reports.get(call.getArgument(0)));
    when(tasks.parameters(any(), eq("VALIDATE"), isNull()))
        .thenAnswer(
            call -> {
              queued = call.getArgument(0);
              return Map.of("expansion_plan", queued.raw(), "plan_digest", queued.digest());
            });
    var cluster = new ExternalCluster();
    cluster.setId(p.clusterId());
    cluster.setKafkaClusterId(p.kafkaClusterId());
    cluster.setKafkaVersion(p.kafkaVersion());
    when(clusters.findById(p.clusterId())).thenReturn(Optional.of(cluster));
    when(catalog.resolve(p.kafkaVersion(), p.artifactId().toString()))
        .thenReturn(
            new AddNodeArtifactCatalog.Artifact(
                p.artifactId().toString(), p.kafkaVersion(), p.artifactSha256()));
    var memberNodes = new ArrayList<ExternalClusterNode>();
    for (var member : p.participants()) {
      var n = new ExternalClusterNode();
      n.setId(member.serviceIds().getFirst());
      n.setAgentId(member.hostId());
      n.setHost(member.hostIp());
      n.setNodeId(member.nodeId());
      n.setIsBroker(member.role().contains("broker"));
      n.setIsController(member.role().contains("controller"));
      n.setJmxExporterPort(0);
      memberNodes.add(n);
      when(ready.participantAddress(p.clusterId(), member.hostId(), member.hostIp()))
          .thenReturn(member.hostIp());
      UUID task = UUID.randomUUID(), inspection = UUID.randomUUID();
      Instant now = Instant.now();
      var requestIdentity =
          new AddNodeProcessInspectionService.Request(
              "add-node-process/v1",
              inspection,
              member.hostId(),
              p.contextHash(),
              p.kafkaClusterId(),
              p.kafkaVersion(),
              member.nodeId(),
              member.role(),
              member.unit(),
              member.configPath(),
              now,
              now.plusSeconds(120));
      var request =
          new KafkaExpansionRuntimeInspection.Request(
              "add-node-runtime-inspection/v1",
              requestIdentity,
              p.parcelDir(),
              member.runtimeHome(),
              p.artifactSha256(),
              List.copyOf(member.javaAgents().values()));
      var identity =
          new AddNodeProcessInspectionService.Evidence(
              "add-node-process/v1",
              inspection,
              member.hostId(),
              p.contextHash(),
              p.kafkaClusterId(),
              p.kafkaVersion(),
              member.nodeId(),
              member.role(),
              member.unit(),
              member.configPath(),
              member.configSha256(),
              member.unitSha256(),
              member.pid(),
              member.startTicks(),
              true,
              member.storage().stream()
                  .map(
                      s ->
                          new AddNodeProcessInspectionService.Metadata(
                              s.path(), p.kafkaClusterId(), member.nodeId(), s.directoryId()))
                  .toList(),
              true,
              false,
              false,
              now,
              List.of("VERSION_NOT_VERIFIED"));
      var runtimeReport =
          new KafkaExpansionRuntimeInspection.Report(
              "add-node-runtime-inspection/v1",
              identity,
              new KafkaExpansionRuntimeInspection.RuntimeProof(
                  "add-node-runtime/v1",
                  p.artifactSha256(),
                  p.kafkaVersion(),
                  member.runtimeHome(),
                  "a".repeat(64),
                  member.artifactProofSha256(),
                  2),
              member.javaAgents(),
              member.resolvedPaths(),
              p.process("controller").inherited(),
              member.controllerPort(),
              now);
      ObjectNode report = mapper.valueToTree(runtimeReport);
      report.set("inherited", mapper.valueToTree(runtimeReport.inherited()));
      reports.put(task, report);
      observations.add(
          Map.of(
              "id",
              task,
              "command",
              "RUNTIME",
              "status",
              "SUCCESS",
              "agent_id",
              member.hostId(),
              "parameters",
              mapper.writeValueAsString(
                  Map.of(
                      "service_id",
                      n.getId().toString(),
                      "runtime_inspection_request",
                      mapper.writeValueAsString(request)))));
    }
    when(nodes.findByClusterId(p.clusterId())).thenReturn(memberNodes);
    var replicas = new TreeMap<Integer, KafkaExpansionQuorum.Replica>();
    var endpoints = new TreeMap<Integer, List<String>>();
    for (var voter : p.voters()) {
      replicas.put(
          voter.nodeId(),
          new KafkaExpansionQuorum.Replica(
              voter.nodeId(), voter.directoryId(), 100L, Instant.now().toEpochMilli()));
      endpoints.put(
          voter.nodeId(),
          List.of(ExternalControllerPlanning.endpoint(voter.hostIp(), voter.port())));
    }
    doReturn(
            new KafkaExpansionQuorum.Snapshot(
                p.kafkaClusterId(),
                p.features(),
                p.voters().getFirst().nodeId(),
                100,
                replicas,
                Map.of(),
                endpoints,
                Instant.now()))
        .when(quorum)
        .observe(anyList());
  }

  @Test
  void realRuntimeEvidenceProducesOnlyAControllerProfileAndReadOnlyValidationTask() {
    var view = planner.get(base.plan().clusterId(), base.plan().operationId());
    assertEquals("CHECKING", view.status());
    assertEquals(102, view.proposedControllerId());
    assertEquals("controller.service", view.serviceName());
    assertEquals("/opt/tantor/kafka/config/controller.properties", view.configPath());
    assertFalse(view.executionEnabled());
    assertNotNull(queued);
    assertEquals(2, queued.plan().participants().size());
    assertEquals(KafkaExpansionWorkflow.Layout.CONTROLLER, queued.plan().layout());
    assertEquals(base.plan().kafkaClusterId(), queued.plan().kafkaClusterId());
    assertEquals("4.3.0", queued.plan().kafkaVersion());
    verify(tasks)
        .enqueue(
            eq(base.plan().operationId()),
            isNull(),
            isNull(),
            eq(base.plan().candidateHostId()),
            eq("VALIDATE"),
            anyMap(),
            eq(0));
    verify(jobs, never()).createJob(any(), anyList());
  }

  @Test
  void separateReviewFreezesTwoProfilesWithoutCreatingAJob() throws Exception {
    previewRow.put(
        "request",
        mapper.writeValueAsString(
            new ExternalControllerPlanning.Request(UUID.randomUUID(), 9093, Map.of(), true)));
    var view = planner.get(base.plan().clusterId(), base.plan().operationId());
    assertTrue(view.separateProcesses());
    assertEquals(2, view.proposedBrokerId());
    assertEquals(102, view.proposedControllerId());
    assertEquals("broker.service", view.brokerServiceName());
    assertEquals(KafkaExpansionWorkflow.Layout.SEPARATE, queued.plan().layout());
    assertEquals(2, queued.plan().processes().size());
    verify(jobs, never()).createJob(any(), anyList());
  }

  @Test
  void separateCurrentScopeUsesOnlyItsOwnCandidateMutationOptIn() throws Exception {
    var f = ExternalSeparateProcessTest.frozen();
    var common =
        mapper.readValue(
            previewRow.get("common_request").toString(),
            ExternalAddNodeReadinessService.Request.class);
    planner.current(
        new ExternalControllerPlanning.Frozen(
            f, common, "b".repeat(64), Instant.now().plusSeconds(120)));
    verify(ready).requireSeparateAgent(f.plan().candidateHostId(), true);
    verify(ready).assertControllerScope(f.plan().clusterId(), f.plan().candidateHostId(), false);
    verify(ready, never()).requireControllerAgent(anyString(), eq(true));
  }

  @Test
  void missingParticipantCannotProduceACandidatePlan() {
    observations.removeLast();
    assertThrows(
        IllegalStateException.class,
        () -> planner.get(base.plan().clusterId(), base.plan().operationId()));
    verify(tasks, never())
        .enqueue(any(), any(), any(), anyString(), anyString(), anyMap(), anyInt());
  }
}
