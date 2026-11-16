package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class ExternalBrokerAdditionTest {
  static ExternalBrokerAdditionPlanning.Payload payload(UUID id) {
    var settings = ExternalAddNodeReadinessServiceTest.settings();
    var request = ExternalAddNodeReadinessServiceTest.request();
    var plan =
        new BrokerAdditionPlan(
            "add-broker/v3",
            id,
            "candidate",
            "a".repeat(64),
            "DcrFbOf_S7WbFGoCwifutA",
            3,
            "192.0.2.10",
            9092,
            "4.3.0",
            settings.runtimeUser(),
            settings.javaHome(),
            request.parcelDir(),
            "b".repeat(64),
            settings.brokerDataPaths(),
            settings.brokerMetadataPath(),
            settings.brokerLogPath(),
            settings.installPath(),
            settings.brokerHeapXmx(),
            "DYNAMIC",
            List.of("192.0.2.20:9093"),
            "",
            "rack1",
            settings);
    plan.validate();
    return new ExternalBrokerAdditionPlanning.Payload(
        request,
        plan,
        "process.roles=broker\n",
        new BrokerAdditionLedger.RuntimeOptions("", "", 0, 100000),
        "101:directory",
        "artifact-id",
        "c".repeat(64));
  }

  @Test
  void externalReleaseIsIndependentAndDefaultsHeld() {
    var ready = mock(ExternalAddNodeReadinessService.class);
    var external = mock(ExternalKafkaManagement.class);
    var nodes = mock(ExternalClusterNodeRepository.class);
    var kafka = mock(BrokerAdditionKafka.class);
    var catalog = mock(AddNodeArtifactCatalog.class);
    var planning =
        new ExternalBrokerAdditionPlanning(
            ready,
            external,
            nodes,
            kafka,
            catalog,
            new ArtifactRepositoryProperties(),
            new KafkaDeploymentProperties(),
            new ObjectMapper(),
            mock(JdbcTemplate.class));
    assertFalse(planning.released());
    assertThrows(
        org.springframework.web.server.ResponseStatusException.class, planning::requireRelease);
    verifyNoInteractions(ready, external, nodes, kafka, catalog);
  }

  @Test
  void reviewDisablesSubmissionWhenOnlyReadinessCapabilityIsPresent() {
    var ready = mock(ExternalAddNodeReadinessService.class);
    var planning =
        spy(
            new ExternalBrokerAdditionPlanning(
                ready,
                mock(ExternalKafkaManagement.class),
                mock(ExternalClusterNodeRepository.class),
                mock(BrokerAdditionKafka.class),
                mock(AddNodeArtifactCatalog.class),
                new ArtifactRepositoryProperties(),
                new KafkaDeploymentProperties(),
                new ObjectMapper(),
                mock(JdbcTemplate.class)));
    org.springframework.test.util.ReflectionTestUtils.setField(planning, "enabled", true);
    org.springframework.test.util.ReflectionTestUtils.setField(planning, "readinessAccepted", true);
    UUID cluster = UUID.randomUUID(), preview = UUID.randomUUID();
    var payload = payload(UUID.randomUUID());
    doReturn(
            new ExternalBrokerAdditionPlanning.Frozen(
                new ExternalAddNodeReadinessService.Preview(
                    preview,
                    cluster,
                    "candidate",
                    "a".repeat(64),
                    Instant.now(),
                    Instant.now().plusSeconds(120),
                    "PASS",
                    List.of(),
                    false),
                payload))
        .when(planning)
        .freeze(cluster, preview, null, null);
    doThrow(new IllegalStateException("readiness-only agent"))
        .when(ready)
        .requireExecutionAgent("candidate");
    var result = planning.review(cluster, preview);
    assertFalse(result.executionEnabled());
    assertTrue(result.blockers().getFirst().contains("execution opt-in"));
  }

  @Test
  void receiptCannotChangeClusterNodeServiceConfigOrDigest() {
    var mapper = new ObjectMapper();
    var id = UUID.randomUUID();
    var payload = payload(id);
    var op =
        new ExternalBrokerAdditionLedger.Operation(
            id,
            UUID.randomUUID(),
            "candidate",
            UUID.randomUUID(),
            3,
            "STARTING",
            null,
            null,
            payload);
    var proof =
        mapper
            .createObjectNode()
            .put("schemaVersion", "add-broker/v1")
            .put("operationId", id.toString())
            .put("hostId", "candidate")
            .put("planDigest", payload.digest())
            .put("nodeId", 3)
            .put("kafkaClusterId", payload.plan().kafkaClusterId())
            .put("serviceName", "broker")
            .put("configPath", payload.plan().configPath())
            .put("phase", "STARTED")
            .put("storageState", "MATCHING")
            .put("serviceActive", true)
            .put("observedAt", Instant.now().toString());
    assertDoesNotThrow(
        () -> ExternalBrokerAdditionLedger.validateProof(op, "START_ADD_BROKER", proof));
    proof.put("nodeId", 4);
    assertThrows(
        org.springframework.web.server.ResponseStatusException.class,
        () -> ExternalBrokerAdditionLedger.validateProof(op, "START_ADD_BROKER", proof));
    proof.put("nodeId", 3);
    proof.put("serviceName", "controller");
    assertThrows(
        org.springframework.web.server.ResponseStatusException.class,
        () -> ExternalBrokerAdditionLedger.validateProof(op, "START_ADD_BROKER", proof));
    proof.put("serviceName", "broker");
    proof.put("storageState", "MISSING");
    assertThrows(
        org.springframework.web.server.ResponseStatusException.class,
        () -> ExternalBrokerAdditionLedger.validateProof(op, "START_ADD_BROKER", proof));
    proof.put("storageState", "MATCHING");
    proof.put("planDigest", "wrong");
    assertThrows(
        org.springframework.web.server.ResponseStatusException.class,
        () -> ExternalBrokerAdditionLedger.validateProof(op, "START_ADD_BROKER", proof));
  }

  @Test
  void failedFinalChecksNeverDispatchPreparationOrFormatting() {
    var ledger = mock(ExternalBrokerAdditionLedger.class);
    var planning = mock(ExternalBrokerAdditionPlanning.class);
    var ready = mock(ExternalAddNodeReadinessService.class);
    var jobs = mock(JobService.class);
    UUID id = UUID.randomUUID(), cluster = UUID.randomUUID();
    var job = new Job();
    job.setId(UUID.randomUUID());
    job.setType(JobType.EXTERNAL_BROKER_ADDITION);
    job.setStatus(JobStatus.IN_PROGRESS);
    var op =
        new ExternalBrokerAdditionLedger.Operation(
            id, cluster, "candidate", job.getId(), 3, "ACCEPTED", null, null, payload(id));
    when(ledger.forJob(job.getId())).thenReturn(op);
    var steps = new ArrayList<JobStep>();
    for (int i = 0; i < 5; i++) {
      var s = new JobStep();
      s.setId(UUID.randomUUID());
      s.setJob(job);
      steps.add(s);
    }
    when(jobs.getSteps(job.getId())).thenReturn(steps);
    when(ready.create(cluster, op.payload().request(), job.getId()))
        .thenReturn(
            new ExternalAddNodeReadinessService.Preview(
                UUID.randomUUID(),
                cluster,
                "candidate",
                "a".repeat(64),
                Instant.now(),
                Instant.now().plusSeconds(120),
                "BLOCKED",
                List.of(),
                false));
    var handler = new ExternalBrokerAdditionJobHandler(ledger, planning, ready, jobs);
    assertThrows(IllegalStateException.class, () -> handler.execute(job));
    verify(ledger).recovery(id);
    verify(ledger, never()).dispatch(any(), any(), anyString(), anyString());
  }

  @Test
  void phase3CheckpointCannotBecomeJobSuccessOrReplayWork() {
    var ledger = mock(ExternalBrokerAdditionLedger.class);
    var planning = mock(ExternalBrokerAdditionPlanning.class);
    var ready = mock(ExternalAddNodeReadinessService.class);
    var jobs = mock(JobService.class);
    var job = new Job();
    job.setId(UUID.randomUUID());
    job.setType(JobType.EXTERNAL_BROKER_ADDITION);
    job.setStatus(JobStatus.IN_PROGRESS);
    var id = UUID.randomUUID();
    when(ledger.forJob(job.getId()))
        .thenReturn(
            new ExternalBrokerAdditionLedger.Operation(
                id,
                UUID.randomUUID(),
                "candidate",
                job.getId(),
                3,
                "STARTED_PENDING_VERIFICATION",
                null,
                null,
                payload(id)));
    var handler = new ExternalBrokerAdditionJobHandler(ledger, planning, ready, jobs);
    when(jobs.getJob(job.getId())).thenReturn(job);
    new JobExecutor(jobs, List.of(handler)).execute(job.getId(), false);
    verify(jobs).updateJobStatus(job.getId(), JobStatus.PARTIAL_SUCCESS);
    verify(jobs, never()).updateJobStatus(job.getId(), JobStatus.SUCCESS);
    verifyNoInteractions(ready, planning);
    verify(ledger, never()).dispatch(any(), any(), anyString(), anyString());
  }
}
