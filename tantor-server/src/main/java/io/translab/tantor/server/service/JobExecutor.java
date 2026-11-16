package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStatus;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobExecutor {

  private final JobService jobService;
  private final List<JobHandler> jobHandlers;

  @Async
  public void execute(UUID jobId, boolean rollback) {
    Job job = jobService.getJob(jobId);
    Integer attempt = job.getRetryCount();
    JobHandler handler =
        jobHandlers.stream()
            .filter(candidate -> candidate.supports(job.getType()))
            .findFirst()
            .orElse(null);

    if (handler == null) {
      fail(
          job,
          rollback,
          new IllegalStateException("No handler found for job type " + job.getType()),
          attempt);
      return;
    }

    jobService.appendLog(
        jobId, rollback ? "Rollback execution started." : "Job picked up by execution engine.");
    try {
      if (rollback) {
        handler.rollback(job);
        jobService.updateJobStatus(jobId, JobStatus.ROLLED_BACK);
        jobService.appendLog(jobId, "Rollback completed successfully.");
      } else {
        handler.execute(job);
        if (externalFinished(job, attempt)) return;
        if (JobService.isExternalAddNode(job.getType()))
          throw new IllegalStateException(
              "External broker handler returned without verified publication.");
        jobService.updateJobStatus(jobId, JobStatus.SUCCESS);
        jobService.appendLog(jobId, "Job execution completed successfully.");
      }
    } catch (ExternalBrokerAwaitingVerification waiting) {
      if (externalFinished(job, attempt)) return;
      if (rollback
          || !JobService.isExternalAddNode(job.getType())) {
        fail(job, rollback, waiting, attempt);
        return;
      }
      jobService.finishExternalBrokerAttempt(
          jobId, attempt, JobStatus.PARTIAL_SUCCESS, waiting.getMessage());
    } catch (Exception e) {
      fail(job, rollback, e, attempt);
    }
  }

  private boolean externalFinished(Job job, Integer attempt) {
    if (!JobService.isExternalAddNode(job.getType()))
      return false;
    Job current = jobService.getJob(job.getId());
    return current.getStatus() == JobStatus.SUCCESS
        || !java.util.Objects.equals(attempt, current.getRetryCount());
  }

  private void fail(Job job, boolean rollback, Exception error, Integer attempt) {
    if (externalFinished(job, attempt)) return;
    log.error("{} failed for job {}", rollback ? "Rollback" : "Execution", job.getId(), error);
    JobStatus status =
        rollback
            ? JobStatus.ROLLBACK_FAILED
            : jobService.hasSuccessfulSteps(job.getId())
                ? JobStatus.PARTIAL_SUCCESS
                : JobStatus.FAILED;
    String message =
        (rollback ? "Rollback failed: " : "Job execution failed: ") + error.getMessage();
    if (JobService.isExternalAddNode(job.getType())) {
      jobService.finishExternalBrokerAttempt(job.getId(), attempt, status, message);
      return;
    }
    jobService.updateJobStatus(job.getId(), status);
    jobService.appendLog(job.getId(), message);
  }
}
