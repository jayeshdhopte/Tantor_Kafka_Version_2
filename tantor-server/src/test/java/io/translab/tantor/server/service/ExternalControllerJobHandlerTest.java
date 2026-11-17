package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.translab.tantor.server.config.KafkaExpansionProperties;
import io.translab.tantor.server.domain.*;
import java.time.Instant;
import java.util.*;

class ExternalControllerJobHandlerTest {
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "CONTROLLER_ONLY",
        "BROKER_PREPARED",
        "BROKER_STARTED",
        "BROKER_FORMAT_INTENT",
        "BROKER_PARTIAL",
        "BROKER_ABSENT"
      })
  void recoveryRetainsVoterAndResumesOnlyIncompleteBrokerWork(String scenario) throws Exception {
    boolean separate = !"CONTROLLER_ONLY".equals(scenario);
    var mapper = new ObjectMapper().findAndRegisterModules();
    var nativePlan =
        separate ? ExternalSeparateProcessTest.frozen() : ExternalControllerRecoveryTest.frozen();
    var p = nativePlan.plan();
    var ledger = mock(ExternalControllerLedger.class);
    var planning = mock(ExternalControllerPlanning.class);
    var tasks = mock(ExternalControllerTasks.class);
    var live = mock(ExternalControllerLive.class);
    var jobs = mock(JobService.class);
    var evidence = new KafkaExpansionNativeEvidence(mapper);
    var job = new Job();
    job.setId(UUID.randomUUID());
    job.setType(JobType.EXTERNAL_CONTROLLER_ADDITION);
    job.setStatus(JobStatus.IN_PROGRESS);
    var op =
        new ExternalControllerLedger.Operation(
            p.operationId(),
            p.clusterId(),
            p.candidateHostId(),
            p.process("controller").nodeId(),
            job.getId(),
            "RECOVERY_REQUIRED",
            1,
            null,
            false);
    when(ledger.forJob(job.getId())).thenReturn(op);
    when(ledger.get(op.id())).thenReturn(op);
    when(ledger.claimedFormat(eq(op.id()), anyString())).thenReturn(true);
    var common =
        new ExternalAddNodeReadinessService.Request(
            p.candidateHostId(),
            p.candidateIp(),
            p.parcelDir(),
            p.artifactId().toString(),
            9092,
            "",
            p.deploymentSettings());
    var frozen =
        new ExternalControllerPlanning.Frozen(
            nativePlan, common, "a".repeat(64), Instant.now().minusSeconds(10));
    when(planning.owned(op.id())).thenReturn(frozen);
    when(planning.requestJson(op.id())).thenReturn("{}");
    when(tasks.parse("{}", ExternalControllerPlanning.Request.class))
        .thenReturn(new ExternalControllerPlanning.Request(UUID.randomUUID(), 9093, Map.of()));
    when(tasks.json(any())).thenAnswer(call -> mapper.writeValueAsString(call.getArgument(0)));
    when(tasks.parameters(eq(nativePlan), anyString(), any()))
        .thenAnswer(c -> Map.of("role", "controller"));
    when(tasks.parameters(eq(nativePlan), anyString(), any(), anyString()))
        .thenAnswer(
            c -> {
              if ("START".equals(c.getArgument(1))) {
                var proof = mapper.readTree((String) c.getArgument(2));
                assertTrue(proof.path("voterVerified").asBoolean());
              }
              return Map.of("role", c.getArgument(3));
            });
    var steps = new ArrayList<JobStep>();
    for (int i = 0; i < 6; i++) {
      var step = new JobStep();
      step.setId(UUID.randomUUID());
      step.setStepOrder(i + 1);
      step.setJob(job);
      steps.add(step);
    }
    when(jobs.getSteps(job.getId())).thenReturn(steps);
    var targets = new HashMap<UUID, String>();
    var actions = new ArrayList<String>();
    var roles = new HashMap<UUID, String>();
    String[] brokerPhase = {
      scenario.equals("BROKER_PREPARED")
          ? "PREPARED"
          : scenario.equals("BROKER_FORMAT_INTENT") ? "FORMAT_INTENT" : "STARTED"
    };
    when(tasks.enqueue(eq(op.id()), eq(op.id()), any(), anyString(), anyString(), anyMap(), eq(1)))
        .thenAnswer(
            call -> {
              UUID id = UUID.randomUUID();
              targets.put(id, call.getArgument(3));
              String action = call.getArgument(4);
              Map<String, String> params = call.getArgument(5);
              String role = params.getOrDefault("role", "controller");
              roles.put(id, role);
              if (role.equals("broker")) {
                if (action.equals("FORMAT") || action.equals("RECONCILE_FORMAT"))
                  brokerPhase[0] = "FORMATTED";
                if (action.equals("START")) brokerPhase[0] = "STARTED";
              }
              actions.add(action);
              return id;
            });
    when(tasks.get(any()))
        .thenAnswer(
            call ->
                new ExternalControllerTasks.View(
                    call.getArgument(0), "INSPECT", "SUCCESS", "fixture"));
    var report = ExternalControllerRecoveryTest.report(nativePlan);
    ((ObjectNode) report.path("receipt")).put("phase", "ADMISSION_INTENT");
    var observation =
        new KafkaExpansionNativeEvidence.Observation(
            report.path("receipt"),
            report.path("observation"),
            Instant.now(),
            "candidate:controller");
    when(tasks.observation(eq(nativePlan), any()))
        .thenAnswer(
            c -> {
              if (!"broker".equals(roles.get(c.getArgument(1)))) return observation;
              var r = ExternalSeparateProcessTest.report(nativePlan, "broker");
              ((ObjectNode) r.path("receipt")).put("phase", brokerPhase[0]);
              var state = (ObjectNode) r.path("observation");
              if (scenario.equals("BROKER_PARTIAL")) state.put("storageState", "PARTIAL");
              if (scenario.equals("BROKER_ABSENT")) {
                ((ObjectNode) r.path("receipt")).put("phase", "ABSENT");
                state.put("storageState", "MISSING").put("storageEmpty", true);
              }
              state
                  .put("serviceActive", brokerPhase[0].equals("STARTED"))
                  .put("runtimeVerified", brokerPhase[0].equals("STARTED"));
              if (brokerPhase[0].equals("PREPARED")) {
                state.put("storageState", "MISSING").put("storageEmpty", true);
              }
              return new KafkaExpansionNativeEvidence.Observation(
                  r.path("receipt"), state, Instant.now(), "candidate:broker");
            });
    when(tasks.report(any()))
        .thenAnswer(
            call -> {
              var member =
                  p.participants().stream()
                      .filter(m -> m.hostId().equals(targets.get(call.getArgument(0))))
                      .findFirst();
              if (member.isEmpty()) return report;
              var m = member.get();
              var identity =
                  new AddNodeProcessInspectionService.Evidence(
                      "add-node-process/v1",
                      UUID.randomUUID(),
                      m.hostId(),
                      p.contextHash(),
                      p.kafkaClusterId(),
                      p.kafkaVersion(),
                      m.nodeId(),
                      m.role(),
                      m.unit(),
                      m.configPath(),
                      m.configSha256(),
                      m.unitSha256(),
                      m.pid(),
                      m.startTicks(),
                      true,
                      m.storage().stream()
                          .map(
                              s ->
                                  new AddNodeProcessInspectionService.Metadata(
                                      s.path(), p.kafkaClusterId(), m.nodeId(), s.directoryId()))
                          .toList(),
                      true,
                      false,
                      false,
                      Instant.now(),
                      List.of("VERSION_NOT_VERIFIED"));
              return mapper.valueToTree(
                  new KafkaExpansionRuntimeInspection.Report(
                      "add-node-runtime-inspection/v1",
                      identity,
                      new KafkaExpansionRuntimeInspection.RuntimeProof(
                          "add-node-runtime/v1",
                          p.artifactSha256(),
                          p.kafkaVersion(),
                          m.runtimeHome(),
                          "a".repeat(64),
                          m.artifactProofSha256(),
                          2),
                      m.javaAgents(),
                      m.resolvedPaths(),
                      Map.of(),
                      m.controllerPort(),
                      Instant.now()));
            });
    String directory = observation.metadataDirectoryId();
    when(live.controller(eq(nativePlan), eq(directory), anyBoolean()))
        .thenAnswer(
            call ->
                new KafkaExpansionLiveVerifier.ControllerProof(
                    UUID.randomUUID(),
                    op.id(),
                    nativePlan.digest(),
                    op.nodeId(),
                    directory,
                    p.voterDigest(),
                    Instant.now(),
                    false,
                    true));
    when(live.health(p, true)).thenReturn("full stable controller health");
    when(live.health(p, true, true)).thenReturn("both stable process health");
    var handler =
        new ExternalControllerJobHandler(
            ledger, planning, tasks, live, evidence, jobs, new KafkaExpansionProperties(), mapper);
    if (Set.of("BROKER_PARTIAL", "BROKER_ABSENT").contains(scenario)) {
      assertThrows(IllegalStateException.class, () -> handler.execute(job));
      verify(ledger).failure(op.id(), 1);
      verify(ledger, never()).publish(any(), anyInt(), any());
      assertTrue(actions.stream().allMatch("INSPECT"::equals));
      return;
    }
    handler.execute(job);
    assertTrue(actions.contains("INSPECT"));
    if (!separate || scenario.equals("BROKER_STARTED"))
      assertTrue(actions.stream().allMatch(a -> Set.of("INSPECT", "RUNTIME").contains(a)));
    else {
      assertEquals(1, Collections.frequency(actions, "START"));
      assertEquals(
          scenario.equals("BROKER_PREPARED") ? 1 : 0, Collections.frequency(actions, "FORMAT"));
      assertEquals(
          scenario.equals("BROKER_FORMAT_INTENT") ? 1 : 0,
          Collections.frequency(actions, "RECONCILE_FORMAT"));
      assertFalse(actions.contains("PREPARE"));
      assertFalse(actions.contains("ADMIT"));
    }
    verify(ledger).publish(op.id(), 1, steps.get(5).getId());
    verify(live, never()).baseline(any());
    verify(ledger, never()).failure(any(), anyInt());
  }
}
