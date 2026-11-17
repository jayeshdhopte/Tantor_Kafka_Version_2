package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.config.KafkaExpansionProperties;
import io.translab.tantor.server.domain.*;
import java.time.Instant;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Dynamic observer -> catch-up -> voter. Never dispatches a participant mutation. */
@Component
@RequiredArgsConstructor
public class ExternalControllerJobHandler implements JobHandler {
  private final ExternalControllerLedger ledger;
  private final ExternalControllerPlanning planning;
  private final ExternalControllerTasks tasks;
  private final ExternalControllerLive live;
  private final KafkaExpansionNativeEvidence nativeEvidence;
  private final JobService jobs;
  private final KafkaExpansionProperties policy;
  private final ObjectMapper mapper;

  public boolean supports(JobType type) {
    return type == JobType.EXTERNAL_CONTROLLER_ADDITION;
  }

  public void execute(Job job) {
    var op = ledger.forJob(job.getId());
    if ("SUCCEEDED".equals(op.state())) return;
    int generation = op.generation();
    var frozen = planning.owned(op.id());
    var steps = jobs.getSteps(job.getId());
    if (steps.size() != 6)
      throw new IllegalStateException("Incomplete external controller workflow.");
    try {
      planning.current(frozen);
      boolean separate =
          frozen.nativePlan().plan().layout() == KafkaExpansionWorkflow.Layout.SEPARATE;
      String brokerCheckpoint = "ACCEPTED";
      KafkaExpansionNativeEvidence.Observation brokerInspection = null;
      String checkpoint = "ACCEPTED";
      KafkaExpansionNativeEvidence.Observation inspection = null;
      if (!"ACCEPTED".equals(op.state())) {
        ledger.reconcile(op.id(), generation);
        inspection = inspect(op, generation, frozen, steps.get(5));
        checkpoint =
            ExternalControllerRecovery.checkpoint(
                inspection, ledger.claimedFormat(op.id(), "controller"));
      }
      if (separate && !"ACCEPTED".equals(op.state())) {
        brokerInspection = inspectRole(op, generation, frozen, steps.get(5), "broker");
        brokerCheckpoint =
            ExternalControllerRecovery.checkpoint(brokerInspection, claimedBrokerFormat(op.id()));
        if ("OBSERVER_STARTED".equals(checkpoint)
            && Set.of("ACCEPTED", "VALIDATING").contains(brokerCheckpoint))
          throw new IllegalStateException(
              "Controller is active without a proven prepared broker; retain both identities for"
                  + " investigation.");
        if (!"OBSERVER_STARTED".equals(checkpoint) && "OBSERVER_STARTED".equals(brokerCheckpoint))
          throw new IllegalStateException(
              "Broker is running without its verified controller; retain both processes.");
      }
      verifyParticipants(op, generation, frozen, steps.getFirst());
      if (!"OBSERVER_STARTED".equals(checkpoint)) live.baseline(frozen.nativePlan().plan());
      if (Set.of("ACCEPTED", "VALIDATING").contains(checkpoint)) {
        ledger.state(op.id(), generation, "VALIDATING");
        ledger.step(op.id(), generation, steps.getFirst().getId(), JobStepStatus.IN_PROGRESS, "");
        if ("ACCEPTED".equals(checkpoint)) {
          var proof = action(op, generation, frozen, steps.getFirst(), "VALIDATE", null);
          ExternalControllerTasks.validateReady(frozen.nativePlan(), proof.report());
        }
        ledger.step(
            op.id(),
            generation,
            steps.getFirst().getId(),
            JobStepStatus.SUCCESS,
            "Fresh candidate/participant runtime and dynamic baseline verified.");
        mutation(
            op,
            generation,
            frozen,
            steps.get(1),
            "PREPARE",
            "PREPARING",
            "PREPARED",
            KafkaExpansionWorkflow.Kind.PREPARE);
        checkpoint = "PREPARED";
      }
      // Prepare both isolated layouts while the candidate is still fresh.
      if (separate && Set.of("ACCEPTED", "VALIDATING").contains(brokerCheckpoint)) {
        mutateRole(
            op,
            generation,
            frozen,
            steps.get(1),
            "broker",
            "PREPARE",
            "PREPARING",
            "PREPARED",
            KafkaExpansionWorkflow.Kind.PREPARE,
            null);
        brokerCheckpoint = "PREPARED";
      }
      if ("PREPARED".equals(checkpoint)) {
        mutation(
            op,
            generation,
            frozen,
            steps.get(2),
            "FORMAT",
            "FORMATTING",
            "FORMATTED",
            KafkaExpansionWorkflow.Kind.FORMAT);
        checkpoint = "FORMATTED";
      } else if ("FORMATTED".equals(checkpoint)
          && inspection != null
          && "FORMAT_INTENT".equals(inspection.phase())) {
        // Adoption only: the native RECONCILE_FORMAT action cannot invoke a formatter.
        mutation(
            op,
            generation,
            frozen,
            steps.get(2),
            "RECONCILE_FORMAT",
            "FORMATTING",
            "FORMATTED",
            KafkaExpansionWorkflow.Kind.FORMAT);
      }
      if ("FORMATTED".equals(checkpoint)) {
        mutation(
            op,
            generation,
            frozen,
            steps.get(3),
            "START",
            "STARTING",
            "OBSERVER_STARTED",
            KafkaExpansionWorkflow.Kind.START);
        checkpoint = "OBSERVER_STARTED";
      }
      if (!"OBSERVER_STARTED".equals(checkpoint))
        throw new IllegalStateException("Controller checkpoint cannot continue.");
      for (int i = 0; i < 4; i++)
        ledger.step(
            op.id(),
            generation,
            steps.get(i).getId(),
            JobStepStatus.SUCCESS,
            "Fresh owned physical checkpoint verified; completed changes were not repeated.");
      ledger.state(op.id(), generation, "CATCHING_UP");
      ledger.step(op.id(), generation, steps.get(4).getId(), JobStepStatus.IN_PROGRESS, "");
      var caughtUp = awaitController(op, generation, frozen, steps.get(4), false);
      if (!caughtUp.voterVerified()) {
        ledger.state(op.id(), generation, "ADMITTING");
        var report = action(op, generation, frozen, steps.get(4), "ADMIT", tasks.json(caughtUp));
        var proof = tasks.observation(frozen.nativePlan(), report.task());
        nativeEvidence.runningCandidate(proof);
      }
      awaitController(op, generation, frozen, steps.get(4), true);
      ledger.state(op.id(), generation, "VOTER_VERIFIED");
      ledger.step(
          op.id(),
          generation,
          steps.get(4).getId(),
          JobStepStatus.SUCCESS,
          "Exact controller ID/directory and expanded voter membership independently verified.");
      ledger.state(op.id(), generation, "VERIFYING");
      ledger.step(op.id(), generation, steps.get(5).getId(), JobStepStatus.IN_PROGRESS, "");
      if (separate) {
        if ("PREPARED".equals(brokerCheckpoint)) {
          mutateRole(
              op,
              generation,
              frozen,
              steps.get(5),
              "broker",
              "FORMAT",
              "FORMATTING",
              "FORMATTED",
              KafkaExpansionWorkflow.Kind.FORMAT,
              null);
          brokerCheckpoint = "FORMATTED";
        } else if ("FORMATTED".equals(brokerCheckpoint)
            && brokerInspection != null
            && "FORMAT_INTENT".equals(brokerInspection.phase())) {
          mutateRole(
              op,
              generation,
              frozen,
              steps.get(5),
              "broker",
              "RECONCILE_FORMAT",
              "FORMATTING",
              "FORMATTED",
              KafkaExpansionWorkflow.Kind.FORMAT,
              null);
        }
        if ("FORMATTED".equals(brokerCheckpoint)) {
          var controllerProof = awaitController(op, generation, frozen, steps.get(5), true);
          mutateRole(
              op,
              generation,
              frozen,
              steps.get(5),
              "broker",
              "START",
              "STARTING",
              "OBSERVER_STARTED",
              KafkaExpansionWorkflow.Kind.START,
              tasks.json(controllerProof));
          brokerCheckpoint = "OBSERVER_STARTED";
        }
        if (!"OBSERVER_STARTED".equals(brokerCheckpoint))
          throw new IllegalStateException("Owned broker checkpoint cannot continue.");
        ledger.state(op.id(), generation, "VERIFYING");
        ledger.step(
            op.id(),
            generation,
            steps.get(5).getId(),
            JobStepStatus.IN_PROGRESS,
            "Verifying both processes before publication.");
      }
      verifyParticipants(op, generation, frozen, steps.get(5));
      var samples = new ArrayList<ExternalControllerLedger.Sample>();
      Instant deadline =
          Instant.now().plusSeconds(Math.max(30, policy.getRegistrationTimeoutSeconds()));
      while (samples.size() < 3) {
        try {
          UUID task = dispatch(op, generation, frozen, steps.get(5), "INSPECT", null);
          var report = await(task);
          var candidate = tasks.observation(frozen.nativePlan(), task);
          nativeEvidence.runningCandidate(candidate);
          var voter = live.controller(frozen.nativePlan(), candidate.metadataDirectoryId(), true);
          UUID brokerTask = null;
          if (separate) {
            brokerTask =
                dispatchRole(op, generation, frozen, steps.get(5), "broker", "INSPECT", null);
            await(brokerTask);
            var brokerProof = tasks.observation(frozen.nativePlan(), brokerTask);
            nativeEvidence.runningCandidate(brokerProof);
            ExternalControllerRecovery.checkpoint(brokerProof, true);
          }
          String health =
              separate
                  ? live.health(frozen.nativePlan().plan(), true, true)
                  : live.health(frozen.nativePlan().plan(), true);
          samples.add(
              new ExternalControllerLedger.Sample(task, voter, health, Instant.now(), brokerTask));
        } catch (KafkaExpansionQuorum.NotReady waiting) {
          samples.clear();
          if (!Instant.now().isBefore(deadline)) throw waiting;
        }
        if (samples.size() < 3) {
          if (!Instant.now().isBefore(deadline))
            throw new IllegalStateException("Stable controller verification timed out.");
          pause(5000);
        }
      }
      ledger.verified(
          op.id(),
          generation,
          new ExternalControllerLedger.Verified(
              frozen.nativePlan().digest(), List.copyOf(samples), Instant.now()));
      ledger.publish(op.id(), generation, steps.get(5).getId());
    } catch (Exception failure) {
      if (ledger.get(op.id()).generation() == generation) {
        ledger.failure(op.id(), generation);
        for (var step : jobs.getSteps(job.getId()))
          if (step.getStatus() == JobStepStatus.IN_PROGRESS) {
            ledger.step(
                op.id(),
                generation,
                step.getId(),
                JobStepStatus.FAILED,
                "Controller recovery required; preserve journal, storage and any admitted voter.");
            break;
          }
      }
      throw new IllegalStateException(
          "External controller addition requires owned reconciliation. No blind format/admission or"
              + " automatic removal is performed.",
          failure);
    }
  }

  private void mutation(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step,
      String action,
      String intent,
      String completed,
      KafkaExpansionWorkflow.Kind kind) {
    ledger.state(op.id(), generation, intent);
    ledger.step(op.id(), generation, step.getId(), JobStepStatus.IN_PROGRESS, "");
    var report = action(op, generation, frozen, step, action, null);
    var observation = tasks.observation(frozen.nativePlan(), report.task());
    nativeEvidence.completed(
        frozen.nativePlan().plan(),
        new KafkaExpansionWorkflow.Step(action, kind, "candidate:controller", "controller"),
        observation,
        false);
    ledger.state(op.id(), generation, completed);
    ledger.step(
        op.id(),
        generation,
        step.getId(),
        JobStepStatus.SUCCESS,
        "Owned " + completed + " controller checkpoint verified.");
  }

  private KafkaExpansionNativeEvidence.Observation inspectRole(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step,
      String role) {
    UUID id = dispatchRole(op, generation, frozen, step, role, "INSPECT", null);
    await(id);
    return tasks.observation(frozen.nativePlan(), id);
  }

  private UUID dispatchRole(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step,
      String role,
      String action,
      String proof) {
    planning.current(frozen);
    return tasks.enqueue(
        op.id(),
        op.id(),
        step.getId(),
        op.agentId(),
        action,
        tasks.parameters(frozen.nativePlan(), action, proof, role),
        generation);
  }

  private void mutateRole(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step,
      String role,
      String action,
      String intent,
      String completed,
      KafkaExpansionWorkflow.Kind kind,
      String controllerProof) {
    ledger.state(op.id(), generation, intent);
    ledger.step(op.id(), generation, step.getId(), JobStepStatus.IN_PROGRESS, "");
    UUID id = dispatchRole(op, generation, frozen, step, role, action, controllerProof);
    await(id);
    var proof = tasks.observation(frozen.nativePlan(), id);
    nativeEvidence.completed(
        frozen.nativePlan().plan(),
        new KafkaExpansionWorkflow.Step(action, kind, "candidate:" + role, role),
        proof,
        false);
    ledger.state(op.id(), generation, completed);
    ledger.step(
        op.id(),
        generation,
        step.getId(),
        step.getStepOrder() == 6 ? JobStepStatus.IN_PROGRESS : JobStepStatus.SUCCESS,
        "Owned " + role + " " + completed + " verified; final publication is still pending.");
  }

  private boolean claimedBrokerFormat(UUID id) {
    return ledger.claimedFormat(id, "broker");
  }

  private KafkaExpansionLiveVerifier.ControllerProof awaitController(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step,
      boolean voter) {
    Instant deadline =
        Instant.now().plusSeconds(Math.max(10, policy.getRegistrationTimeoutSeconds()));
    while (true) {
      try {
        var observation = inspect(op, generation, frozen, step);
        nativeEvidence.runningCandidate(observation);
        return live.controller(frozen.nativePlan(), observation.metadataDirectoryId(), voter);
      } catch (KafkaExpansionQuorum.NotReady waiting) {
        if (!Instant.now().isBefore(deadline)) throw waiting;
        pause(5000);
      }
    }
  }

  private KafkaExpansionNativeEvidence.Observation inspect(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step) {
    return tasks.observation(
        frozen.nativePlan(), action(op, generation, frozen, step, "INSPECT", null).task());
  }

  private record Reply(UUID task, JsonNode report) {}

  private Reply action(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step,
      String action,
      String proof) {
    UUID id = dispatch(op, generation, frozen, step, action, proof);
    return new Reply(id, await(id));
  }

  private UUID dispatch(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step,
      String action,
      String proof) {
    planning.current(frozen);
    return tasks.enqueue(
        op.id(),
        op.id(),
        step.getId(),
        op.agentId(),
        action,
        tasks.parameters(frozen.nativePlan(), action, proof),
        generation);
  }

  private void verifyParticipants(
      ExternalControllerLedger.Operation op,
      int generation,
      ExternalControllerPlanning.Frozen frozen,
      JobStep step) {
    var pending = new LinkedHashMap<UUID, KafkaExpansionPlan.Participant>();
    var p = frozen.nativePlan().plan();
    var request = tasks.parse(jdbcRequest(op.id()), ExternalControllerPlanning.Request.class);
    for (var member : p.participants()) {
      var identity =
          new AddNodeProcessInspectionService.Request(
              "add-node-process/v1",
              UUID.randomUUID(),
              member.hostId(),
              p.contextHash(),
              p.kafkaClusterId(),
              p.kafkaVersion(),
              member.nodeId(),
              member.role(),
              member.unit(),
              member.configPath(),
              Instant.now(),
              Instant.now().plusSeconds(170));
      var inspection =
          new KafkaExpansionRuntimeInspection.Request(
              "add-node-runtime-inspection/v1",
              identity,
              request.participantParcelDirs().getOrDefault(member.hostId(), p.parcelDir()),
              member.runtimeHome(),
              p.artifactSha256(),
              List.copyOf(new HashSet<>(member.javaAgents().values())));
      pending.put(
          tasks.enqueue(
              op.id(),
              op.id(),
              step.getId(),
              member.hostId(),
              "RUNTIME",
              Map.of("runtime_inspection_request", tasks.json(inspection)),
              generation),
          member);
    }
    for (var entry : pending.entrySet()) {
      var report =
          mapper.convertValue(await(entry.getKey()), KafkaExpansionRuntimeInspection.Report.class);
      var member = entry.getValue();
      var identity = report.identity();
      if (identity.pid() != member.pid()
          || !identity.processStartTicks().equals(member.startTicks())
          || !identity.configSha256().equals(member.configSha256())
          || !identity.unitSha256().equals(member.unitSha256())
          || !report.runtime().proofSha256().equals(member.artifactProofSha256())
          || !report.javaAgents().equals(member.javaAgents())
          || !report.resolvedPaths().equals(member.resolvedPaths()))
        throw new IllegalStateException(
            "An existing participant runtime/configuration changed after review.");
    }
  }

  private String jdbcRequest(UUID id) {
    return planning.requestJson(id);
  }

  private JsonNode await(UUID id) {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(180);
    var task = tasks.get(id);
    while (Set.of("PENDING", "RUNNING").contains(task.status()) && System.nanoTime() < deadline) {
      pause(1000);
      task = tasks.get(id);
    }
    if (!"SUCCESS".equals(task.status()))
      throw new IllegalStateException(
          "Controller action/observation did not return verified success.");
    return tasks.report(id);
  }

  private static void pause(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Controller execution interrupted.", e);
    }
  }
}
