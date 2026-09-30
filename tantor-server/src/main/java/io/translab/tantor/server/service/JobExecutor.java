package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobExecutor {

    private final JobService jobService;
    private final List<JobHandler> jobHandlers;
    @Async
    public void execute(UUID jobId, boolean rollback) {
        Job job = jobService.getJob(jobId);
        JobHandler handler = jobHandlers.stream()
                .filter(candidate -> candidate.supports(job.getType()))
                .findFirst()
                .orElse(null);

        if (handler == null) {
            fail(job, rollback, new IllegalStateException("No handler found for job type " + job.getType()));
            return;
        }

        jobService.appendLog(jobId, rollback ? "Rollback execution started." : "Job picked up by execution engine.");
        try {
            if (rollback) {
                handler.rollback(job);
                jobService.updateJobStatus(jobId, JobStatus.ROLLED_BACK);
                jobService.appendLog(jobId, "Rollback completed successfully.");
            } else {
                handler.execute(job);
                jobService.updateJobStatus(jobId, JobStatus.SUCCESS);
                jobService.appendLog(jobId, "Job execution completed successfully.");
            }
        } catch (Exception e) {
            fail(job, rollback, e);
        }
    }

    private void fail(Job job, boolean rollback, Exception error) {
        log.error("{} failed for job {}", rollback ? "Rollback" : "Execution", job.getId(), error);
        JobStatus status = rollback
                ? JobStatus.ROLLBACK_FAILED
                : jobService.hasSuccessfulSteps(job.getId()) ? JobStatus.PARTIAL_SUCCESS : JobStatus.FAILED;
        jobService.updateJobStatus(job.getId(), status);
        jobService.appendLog(job.getId(), (rollback ? "Rollback failed: " : "Job execution failed: ") + error.getMessage());
    }
}
