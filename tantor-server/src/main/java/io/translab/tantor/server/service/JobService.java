package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.JobRepository;
import io.translab.tantor.server.repository.JobStepRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobRepository;
    private final JobStepRepository jobStepRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Job createJob(Job job, List<JobStep> steps) {
        if (job.getRequestedBy() == null || job.getRequestedBy().isBlank()) {
            job.setRequestedBy(currentRequester());
        }
        Job saved = jobRepository.saveAndFlush(job);
        for (JobStep step : steps) {
            step.setJob(saved);
        }
        jobStepRepository.saveAll(steps);
        refreshProgress(saved.getId());
        return saved;
    }

    public Job createJob(Job job) {
        return createJob(job, List.of());
    }

    public Job getJob(UUID id) {
        return jobRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Job not found: " + id));
    }

    public List<Job> getAllJobs() {
        return jobRepository.findAllByOrderByCreatedAtDesc();
    }

    public List<JobStep> getSteps(UUID jobId) {
        return jobStepRepository.findByJobIdOrderByStepOrderAsc(jobId);
    }

    @Transactional
    public Optional<ClaimedJob> claimNextJob() {
        Optional<Job> normal = jobRepository.findFirstByStatusOrderByCreatedAtAsc(JobStatus.PENDING);
        if (normal.isPresent()) {
            Job job = normal.get();
            job.setStatus(JobStatus.IN_PROGRESS);
            job.setStartTime(Instant.now());
            job.setEndTime(null);
            jobRepository.save(job);
            return Optional.of(new ClaimedJob(job.getId(), false));
        }

        Optional<Job> rollback = jobRepository.findFirstByStatusOrderByCreatedAtAsc(JobStatus.ROLLBACK_PENDING);
        if (rollback.isPresent()) {
            Job job = rollback.get();
            job.setStatus(JobStatus.ROLLING_BACK);
            job.setEndTime(null);
            jobRepository.save(job);
            return Optional.of(new ClaimedJob(job.getId(), true));
        }
        return Optional.empty();
    }

    @Transactional
    public Job updateJobStatus(UUID id, JobStatus status) {
        Job job = getJob(id);
        job.setStatus(status);
        if (status == JobStatus.IN_PROGRESS && job.getStartTime() == null) {
            job.setStartTime(Instant.now());
        }
        if (isTerminal(status)) {
            job.setEndTime(Instant.now());
        }
        return jobRepository.save(job);
    }

    @Transactional
    public void appendLog(UUID id, String logLine) {
        Job job = getJob(id);
        String currentLogs = job.getLogs() == null ? "" : job.getLogs();
        job.setLogs(currentLogs + "[" + Instant.now() + "] " + logLine + "\n");
        jobRepository.save(job);
    }

    @Transactional
    public JobStep startStep(UUID stepId) {
        JobStep step = getStep(stepId);
        step.setStatus(JobStepStatus.IN_PROGRESS);
        step.setStartTime(Instant.now());
        step.setEndTime(null);
        JobStep saved = jobStepRepository.save(step);
        refreshProgress(step.getJob().getId());
        return saved;
    }

    @Transactional
    public void attachAgentTask(UUID stepId, UUID taskId) {
        JobStep step = getStep(stepId);
        step.setAgentTaskId(taskId);
        jobStepRepository.save(step);
    }

    @Transactional
    public void attachExternalServiceTask(UUID stepId, UUID taskId) {
        JobStep step = getStep(stepId);
        step.setExternalServiceTaskId(taskId);
        jobStepRepository.save(step);
    }

    @Transactional
    public void completeStep(UUID stepId, String output) {
        finishStep(stepId, JobStepStatus.SUCCESS, output);
    }

    @Transactional
    public void failStep(UUID stepId, String output) {
        finishStep(stepId, JobStepStatus.FAILED, output);
    }

    @Transactional
    public void rolledBackStep(UUID stepId, String output) {
        finishStep(stepId, JobStepStatus.ROLLED_BACK, output);
    }

    @Transactional
    public void rollbackFailedStep(UUID stepId, String output) {
        finishStep(stepId, JobStepStatus.ROLLBACK_FAILED, output);
    }

    @Transactional
    public void appendStepLog(UUID stepId, String output) {
        if (output == null || output.isBlank()) return;
        JobStep step = getStep(stepId);
        String current = step.getLogs() == null ? "" : step.getLogs();
        step.setLogs(current + "[" + Instant.now() + "] " + output + "\n");
        jobStepRepository.save(step);
    }

    @Transactional
    public Job retryJob(UUID id) {
        Job job = getJob(id);
        if (job.getStatus() != JobStatus.FAILED
                && job.getStatus() != JobStatus.PARTIAL_SUCCESS
                && job.getStatus() != JobStatus.ROLLBACK_FAILED) {
            throw new RuntimeException("Only failed or partially successful jobs can be retried.");
        }

        boolean retryRollback = job.getStatus() == JobStatus.ROLLBACK_FAILED;
        if (!retryRollback) {
            for (JobStep step : getSteps(id)) {
                if (step.getStatus() != JobStepStatus.SUCCESS) {
                    step.setStatus(JobStepStatus.PENDING);
                    step.setAgentTaskId(null);
                    step.setExternalServiceTaskId(null);
                    step.setStartTime(null);
                    step.setEndTime(null);
                    step.setRetryCount(step.getRetryCount() + 1);
                    jobStepRepository.save(step);
                }
            }
        }

        job.setStatus(retryRollback ? JobStatus.ROLLBACK_PENDING : JobStatus.PENDING);
        job.setRetryCount(job.getRetryCount() + 1);
        job.setEndTime(null);
        appendLog(id, retryRollback ? "Rollback retry requested." : "Retry requested; completed steps will be preserved.");
        refreshProgress(id);
        return jobRepository.save(job);
    }

    @Transactional
    public Job requestRollback(UUID id) {
        Job job = getJob(id);
        if (!Boolean.TRUE.equals(job.getRollbackSupported())) {
            throw new RuntimeException("Rollback is not supported for this job type.");
        }
        if (job.getStatus() != JobStatus.SUCCESS
                && job.getStatus() != JobStatus.FAILED
                && job.getStatus() != JobStatus.PARTIAL_SUCCESS) {
            throw new RuntimeException("Rollback can only be requested for a completed or failed job.");
        }
        job.setStatus(JobStatus.ROLLBACK_PENDING);
        job.setEndTime(null);
        appendLog(id, "Rollback requested.");
        return jobRepository.save(job);
    }

    @Transactional
    public void refreshProgress(UUID jobId) {
        List<JobStep> steps = getSteps(jobId);
        long completed = steps.stream().filter(step -> step.getStatus() == JobStepStatus.SUCCESS
                || step.getStatus() == JobStepStatus.ROLLED_BACK
                || step.getStatus() == JobStepStatus.SKIPPED).count();
        String message = steps.stream()
                .filter(step -> step.getStatus() == JobStepStatus.IN_PROGRESS)
                .map(JobStep::getName)
                .findFirst()
                .orElse(completed == steps.size() && !steps.isEmpty() ? "All steps completed." : "Waiting for execution.");
        try {
            Job job = getJob(jobId);
            job.setProgress(objectMapper.writeValueAsString(Map.of(
                    "currentStep", completed,
                    "totalSteps", steps.size(),
                    "message", message
            )));
            jobRepository.save(job);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to update job progress", e);
        }
    }

    public boolean hasSuccessfulSteps(UUID jobId) {
        return getSteps(jobId).stream().anyMatch(step -> step.getStatus() == JobStepStatus.SUCCESS);
    }

    @Transactional
    public void recoverInterruptedJobs() {
        for (Job job : jobRepository.findByStatus(JobStatus.IN_PROGRESS)) {
            job.setStatus(JobStatus.PENDING);
            job.setEndTime(null);
            jobRepository.save(job);
            appendLog(job.getId(), "Backend restarted; job returned to the durable execution queue.");
        }
        for (Job job : jobRepository.findByStatus(JobStatus.ROLLING_BACK)) {
            job.setStatus(JobStatus.ROLLBACK_PENDING);
            job.setEndTime(null);
            jobRepository.save(job);
            appendLog(job.getId(), "Backend restarted; rollback returned to the durable execution queue.");
        }
        for (JobStep step : jobStepRepository.findAll()) {
            if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() == null) {
                step.setStatus(JobStepStatus.PENDING);
                jobStepRepository.save(step);
            }
        }
    }

    private void finishStep(UUID stepId, JobStepStatus status, String output) {
        JobStep step = getStep(stepId);
        step.setStatus(status);
        step.setEndTime(Instant.now());
        if (output != null && !output.isBlank()) {
            String current = step.getLogs() == null ? "" : step.getLogs();
            step.setLogs(current + output + (output.endsWith("\n") ? "" : "\n"));
        }
        jobStepRepository.save(step);
        refreshProgress(step.getJob().getId());
    }

    private JobStep getStep(UUID stepId) {
        return jobStepRepository.findById(stepId)
                .orElseThrow(() -> new RuntimeException("Job step not found: " + stepId));
    }

    private String currentRequester() {
        return io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
    }

    private boolean isTerminal(JobStatus status) {
        return status == JobStatus.SUCCESS
                || status == JobStatus.FAILED
                || status == JobStatus.PARTIAL_SUCCESS
                || status == JobStatus.ROLLED_BACK
                || status == JobStatus.ROLLBACK_FAILED;
    }

    public record ClaimedJob(UUID jobId, boolean rollback) {}
}
