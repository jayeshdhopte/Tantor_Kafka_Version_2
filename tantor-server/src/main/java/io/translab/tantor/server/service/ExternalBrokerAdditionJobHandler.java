package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.*;
import java.time.Instant;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Durable broker checkpoints; only physical inspection can authorize resumed work. */
@Component
@RequiredArgsConstructor
public class ExternalBrokerAdditionJobHandler implements JobHandler {
  private final ExternalBrokerAdditionLedger ledger;
  private final ExternalBrokerAdditionPlanning planning;
  private final ExternalAddNodeReadinessService readiness;
  private final JobService jobs;
  private final ExternalBrokerRecoveryService recovery;
  private final ExternalBrokerVerification verification;

  @Value("${tantor.external-add-node.registration-timeout-seconds:240}")
  private int registrationTimeoutSeconds = 240;

  public boolean supports(JobType type) {
    return type == JobType.EXTERNAL_BROKER_ADDITION;
  }

  public void execute(Job job) {
    var op = ledger.forJob(job.getId());
    int generation = op.generation();
    if ("SUCCEEDED".equals(op.state())) return;
    try {
      var steps = jobs.getSteps(job.getId());
      if (steps.size() != 5)
        throw new IllegalStateException("Incomplete external broker workflow.");
      if (!Set.of("ACCEPTED", "VALIDATING").contains(op.state())) {
        recovery.beginReconciliation(op.id(), generation);
        var task =
            ledger.dispatch(
                op.id(), steps.get(4).getId(), "INSPECT_ADD_BROKER", "RECONCILING", generation);
        var inspection = await(task);
        String checkpoint = recovery.reconciled(op.id(), generation, inspection.id());
        int count =
            switch (checkpoint) {
              case "VALIDATING" -> 1;
              case "PREPARED" -> 2;
              case "FORMATTED" -> 3;
              case "STARTED_PENDING_VERIFICATION" -> 4;
              default -> 0;
            };
        for (int i = 0; i < count; i++)
          recovery.step(
              op.id(),
              generation,
              steps.get(i).getId(),
              JobStepStatus.SUCCESS,
              "Reconciled fresh owned receipt; completed candidate changes were not repeated.");
      }
      op = current(op.id(), generation);
      boolean ownedPartial = "VALIDATING".equals(op.state()) && hasReconciledPreparation(op.id());
      if ("ACCEPTED".equals(op.state()) || "VALIDATING".equals(op.state()) && !ownedPartial)
        validate(op, steps.getFirst());
      op = current(op.id(), generation);
      if ("VALIDATING".equals(op.state()))
        mutation(op, steps.get(1), "PREPARE_ADD_BROKER", "PREPARING", "PREPARED");
      op = current(op.id(), generation);
      if ("PREPARED".equals(op.state()))
        mutation(op, steps.get(2), "FORMAT_ADD_BROKER", "FORMATTING", "FORMATTED");
      op = current(op.id(), generation);
      if ("FORMATTED".equals(op.state()))
        mutation(op, steps.get(3), "START_ADD_BROKER", "STARTING", "STARTED_PENDING_VERIFICATION");
      op = current(op.id(), generation);
      recovery.step(op.id(), generation, steps.get(4).getId(), JobStepStatus.IN_PROGRESS, "");
      recovery.beginVerification(op.id(), generation);
      var samples = new ArrayList<ExternalBrokerVerification.Sample>();
      long deadline =
          System.nanoTime()
              + java.util.concurrent.TimeUnit.SECONDS.toNanos(
                  Math.max(30, registrationTimeoutSeconds));
      while (samples.size() < 3) {
        try {
          var task =
              ledger.dispatch(
                  op.id(), steps.get(4).getId(), "INSPECT_ADD_BROKER", "VERIFYING", generation);
          var done = await(task);
          samples.add(verification.sample(op, ledger.result(done).report()));
        } catch (Exception failure) {
          ExternalBrokerAdditionLedger.fence(ledger.get(op.id()), generation);
          if (Thread.currentThread().isInterrupted()) throw failure;
          samples.clear();
          if (System.nanoTime() >= deadline) throw failure;
        }
        if (samples.size() < 3) {
          if (System.nanoTime() >= deadline)
            throw new IllegalStateException("Stable broker verification timed out.");
          pause(5000);
        }
      }
      var evidence =
          new ExternalBrokerVerification.Evidence(
              op.id(), op.payload().digest(), List.copyOf(samples), Instant.now());
      recovery.verified(op.id(), generation, evidence);
      recovery.publish(op.id(), generation, steps.get(4).getId());
    } catch (Exception e) {
      if (ledger.get(op.id()).generation() == generation) {
        ledger.recovery(op.id(), generation);
        jobs.getSteps(job.getId()).stream()
            .filter(s -> s.getStatus() == JobStepStatus.IN_PROGRESS)
            .findFirst()
            .ifPresent(
                s ->
                    recovery.step(
                        ledger.forJob(job.getId()).id(),
                        generation,
                        s.getId(),
                        JobStepStatus.FAILED,
                        "External broker work requires reconciliation; reservations and receipts"
                            + " retained."));
      }
      throw new IllegalStateException(
          "External broker addition requires owned reconciliation; no formatter is blindly"
              + " replayed.",
          e);
    }
  }

  private ExternalBrokerAdditionLedger.Operation current(UUID id, int generation) {
    var op = ledger.get(id);
    ExternalBrokerAdditionLedger.fence(op, generation);
    return op;
  }

  private boolean hasReconciledPreparation(UUID id) {
    return recovery.hasPreparationInspection(id);
  }

  private void validate(ExternalBrokerAdditionLedger.Operation op, JobStep step) {
    int generation = op.generation();
    recovery.step(op.id(), op.generation(), step.getId(), JobStepStatus.IN_PROGRESS, "");
    ledger.validating(op.id(), generation);
    // A new observation is required after interruption; expired previews are not reused.
    var preview = readiness.create(op.clusterId(), op.payload().request(), op.jobId());
    ledger.finalPreview(op.id(), preview.id(), generation);
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(150);
    while ("CHECKING".equals(preview.status()) && System.nanoTime() < deadline) {
      pause(1000);
      preview = readiness.get(op.clusterId(), preview.id(), op.jobId());
    }
    if (!"PASS".equals(preview.status()) || !preview.expiresAt().isAfter(Instant.now()))
      throw new IllegalStateException("Fresh final readiness did not pass.");
    var frozen = planning.freeze(op.clusterId(), preview.id(), op.jobId(), op.id()).payload();
    if (!frozen.digest().equals(op.payload().digest())
        || !frozen.voterBaseline().equals(op.payload().voterBaseline())
        || !frozen.bindingHash().equals(op.payload().bindingHash()))
      throw new IllegalStateException("Final observations differ from the reviewed plan.");
    await(
        ledger.dispatch(
            op.id(), step.getId(), "VALIDATE_ADD_BROKER_PLAN", "VALIDATING", generation));
    recovery.step(
        op.id(),
        generation,
        step.getId(),
        JobStepStatus.SUCCESS,
        "Fresh readiness, frozen settings and compatible broker capabilities verified.");
  }

  private void mutation(
      ExternalBrokerAdditionLedger.Operation op,
      JobStep step,
      String command,
      String intent,
      String complete) {
    recovery.step(op.id(), op.generation(), step.getId(), JobStepStatus.IN_PROGRESS, "");
    var task = ledger.dispatch(op.id(), step.getId(), command, intent, op.generation());
    var done = await(task);
    var result = ledger.result(done);
    ExternalBrokerAdditionLedger.validateProof(op, command, result.report());
    ledger.checkpoint(op.id(), complete, result.report().toString(), op.generation());
    recovery.step(
        op.id(), op.generation(), step.getId(), JobStepStatus.SUCCESS, result.report().toString());
  }

  private ExternalBrokerAdditionLedger.TaskView await(ExternalBrokerAdditionLedger.TaskView task) {
    long end = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(240);
    while (Set.of("PENDING", "RUNNING").contains(task.status()) && System.nanoTime() < end) {
      pause(1000);
      task = ledger.task(task.id());
    }
    if (!"SUCCESS".equals(task.status()))
      throw new IllegalStateException("Candidate action did not return verified success.");
    return task;
  }

  private static void pause(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("External broker workflow interrupted.", e);
    }
  }
}
