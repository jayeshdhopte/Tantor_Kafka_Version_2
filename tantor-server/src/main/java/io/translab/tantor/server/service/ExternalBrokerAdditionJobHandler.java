package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.*;
import java.time.Instant;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Phase 3 stops at a durable started checkpoint; Phase 4 owns verified publication. */
@Component
@RequiredArgsConstructor
public class ExternalBrokerAdditionJobHandler implements JobHandler {
  private final ExternalBrokerAdditionLedger ledger;
  private final ExternalBrokerAdditionPlanning planning;
  private final ExternalAddNodeReadinessService readiness;
  private final JobService jobs;

  public boolean supports(JobType type) {
    return type == JobType.EXTERNAL_BROKER_ADDITION;
  }

  public void execute(Job job) {
    var op = ledger.forJob(job.getId());
    if ("STARTED_PENDING_VERIFICATION".equals(op.state()))
      throw new ExternalBrokerAwaitingVerification();
    if (!Set.of("ACCEPTED", "VALIDATING").contains(op.state()))
      throw new IllegalStateException(
          "External broker operation requires Phase 4 reconciliation; no mutation is replayed.");
    try {
      var steps = jobs.getSteps(job.getId());
      if (steps.size() != 5)
        throw new IllegalStateException("Incomplete external broker workflow.");
      var validation = steps.getFirst();
      jobs.startStep(validation.getId());
      ledger.validating(op.id());
      var finalPreview =
          op.finalPreviewId() == null
              ? readiness.create(op.clusterId(), op.payload().request(), job.getId())
              : readiness.get(op.clusterId(), op.finalPreviewId(), job.getId());
      ledger.finalPreview(op.id(), finalPreview.id());
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(150);
      while ("CHECKING".equals(finalPreview.status()) && System.nanoTime() < deadline) {
        pause();
        finalPreview = readiness.get(op.clusterId(), finalPreview.id(), job.getId());
      }
      if (!"PASS".equals(finalPreview.status()) || !finalPreview.expiresAt().isAfter(Instant.now()))
        throw new IllegalStateException("Fresh final external broker readiness did not pass.");
      var finalPayload =
          planning.freeze(op.clusterId(), finalPreview.id(), job.getId(), op.id()).payload();
      if (!finalPayload.digest().equals(op.payload().digest())
          || !finalPayload.voterBaseline().equals(op.payload().voterBaseline()))
        throw new IllegalStateException("Final observations differ from the frozen reviewed plan.");
      var validationTask =
          ledger.dispatch(op.id(), validation.getId(), "VALIDATE_ADD_BROKER_PLAN", "VALIDATING");
      await(validationTask);
      jobs.completeStep(
          validation.getId(),
          "Fresh readiness, existing Kafka ID/version, frozen settings, approved payload and"
              + " external broker capabilities verified.");
      mutation(op, steps.get(1), "PREPARE_ADD_BROKER", "PREPARING", "PREPARED");
      mutation(op, steps.get(2), "FORMAT_ADD_BROKER", "FORMATTING", "FORMATTED");
      mutation(op, steps.get(3), "START_ADD_BROKER", "STARTING", "STARTED_PENDING_VERIFICATION");
    } catch (Exception e) {
      ledger.recovery(op.id());
      jobs.getSteps(job.getId()).stream()
          .filter(s -> s.getStatus() == JobStepStatus.IN_PROGRESS)
          .findFirst()
          .ifPresent(
              s ->
                  jobs.failStep(
                      s.getId(),
                      "External broker execution interrupted; preserve reservations and"
                          + " receipts."));
      throw new IllegalStateException(
          "External broker addition requires reconciliation. No storage formatting or startup is"
              + " blindly repeated.",
          e);
    }
    throw new ExternalBrokerAwaitingVerification();
  }

  private void mutation(
      ExternalBrokerAdditionLedger.Operation op,
      JobStep step,
      String command,
      String intent,
      String complete) {
    jobs.startStep(step.getId());
    var task = ledger.dispatch(op.id(), step.getId(), command, intent);
    var done = await(task);
    var result = ledger.result(done);
    ExternalBrokerAdditionLedger.validateProof(op, command, result.report());
    ledger.checkpoint(op.id(), complete, result.report().toString());
    jobs.completeStep(step.getId(), result.report().toString());
  }

  private ExternalBrokerAdditionLedger.TaskView await(
      ExternalBrokerAdditionLedger.TaskView initial) {
    long end = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(240);
    var task = initial;
    while (Set.of("PENDING", "RUNNING").contains(task.status()) && System.nanoTime() < end) {
      pause();
      task = ledger.task(task.id());
    }
    if (!"SUCCESS".equals(task.status()))
      throw new IllegalStateException("External broker task did not return verified success.");
    return task;
  }

  private static void pause() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("External broker workflow interrupted.", e);
    }
  }
}
