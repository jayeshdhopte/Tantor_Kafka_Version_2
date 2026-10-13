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
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobRepository;
    private final JobStepRepository jobStepRepository;
    private final ObjectMapper objectMapper;
    private final io.translab.tantor.server.repository.ClusterRepository clusterRepository;
    private final io.translab.tantor.server.repository.ExternalClusterRepository externalClusterRepository;

    @Transactional
    public Job createJob(Job job, List<JobStep> steps) {
        lockAndCheck(job.getResourceKey(), job.getType(), null);
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

    @Transactional
    public Job createJob(Job job) {
        return createJob(job, List.of());
    }

    @Transactional
    public void assertClusterMutable(UUID clusterId) {
        lockAndCheck("cluster:" + clusterId, null, null);
    }

    @Transactional
    public void assertClusterDeletionAllowed(UUID clusterId) {
        lockAndCheck("cluster:" + clusterId, JobType.CLUSTER_DELETION, null);
    }

    private void lockAndCheck(String resource, JobType incoming, UUID excluded) {
        if (resource == null || !resource.startsWith("cluster:")) return;
        UUID clusterId = UUID.fromString(resource.substring("cluster:".length()));
        if (clusterRepository.findForServiceUpdate(clusterId).isEmpty())
            externalClusterRepository.findForServiceUpdate(clusterId);
        var active = new java.util.ArrayList<>(jobRepository.findByResourceKeyAndStatusIn(resource,
                List.of(JobStatus.PENDING, JobStatus.IN_PROGRESS, JobStatus.ROLLBACK_PENDING, JobStatus.ROLLING_BACK)));
        // Older external service jobs used a separate key; keep them in the same operation lock.
        active.addAll(jobRepository.findByResourceKeyAndStatusIn("external-services:" + clusterId,
                List.of(JobStatus.PENDING, JobStatus.IN_PROGRESS, JobStatus.ROLLBACK_PENDING, JobStatus.ROLLING_BACK)));
        var outstanding = jobRepository.findWithUnfinishedRestartTasks().stream()
                .filter(job -> resource.equals(job.getResourceKey()) && !Objects.equals(job.getId(), excluded)).findFirst();
        if (outstanding.isPresent()) throw new ClusterOperationConflictException("An agent task is still pending or running for job " + outstanding.get().getId() + ". Resume that job before another cluster operation.");
        for (Job other : active) {
            if (!other.getId().equals(excluded) && (incoming == JobType.ROLLING_RESTART || incoming == JobType.EXTERNAL_ROLLING_RESTART
                    || incoming == JobType.EXTERNAL_SERVICES || incoming == JobType.ROLLING_CONFIG_UPDATE
                    || incoming == JobType.CONFIG_CHANGE || incoming == JobType.TARGETED_CONFIG_CHANGE
                    || other.getType() == JobType.ROLLING_RESTART || other.getType() == JobType.EXTERNAL_ROLLING_RESTART
                    || other.getType() == JobType.EXTERNAL_SERVICES || other.getType() == JobType.ROLLING_CONFIG_UPDATE
                    || other.getType() == JobType.CONFIG_CHANGE || other.getType() == JobType.TARGETED_CONFIG_CHANGE
                    || incoming == JobType.CLUSTER_DELETION || other.getType() == JobType.CLUSTER_DELETION))
                throw new ClusterOperationConflictException("Another operation is active on this cluster. Wait for job " + other.getId() + " to finish.");
        }
    }

    public Job getJob(UUID id) {
        return jobRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Job not found: " + id));
    }

    public List<Job> getAllJobs() {
        return jobRepository.findAllByOrderByCreatedAtDesc();
    }

    public List<Map<String, String>> getActiveClusterJobs() {
        Map<UUID, Job> active = new java.util.LinkedHashMap<>();
        java.util.stream.Stream.of(JobStatus.PENDING, JobStatus.IN_PROGRESS, JobStatus.ROLLBACK_PENDING, JobStatus.ROLLING_BACK)
                .flatMap(status -> jobRepository.findByStatus(status).stream()).forEach(job -> active.put(job.getId(), job));
        jobRepository.findWithUnfinishedRestartTasks().forEach(job -> active.putIfAbsent(job.getId(), job));
        return active.values().stream().filter(job -> job.getResourceKey() != null &&
                        (job.getResourceKey().startsWith("cluster:") || job.getResourceKey().startsWith("external-services:")))
                .map(job -> Map.of("id", job.getId().toString(),
                        "clusterId", job.getResourceKey().substring(job.getResourceKey().indexOf(':') + 1),
                        "type", job.getType().name(), "status", isTerminal(job.getStatus()) ? "WAITING_AGENT" : job.getStatus().name(),
                        "operation", job.getType() == JobType.DEPLOYMENT && isServiceAddition(job) ? "ADD_SERVICE" : job.getType().name(),
                        "progress", isTerminal(job.getStatus()) ? "{\"message\":\"Waiting for the previous agent task to finish.\"}"
                                : job.getProgress() == null ? "{}" : job.getProgress())).toList();
    }

    private boolean isServiceAddition(Job job) {
        try {
            return objectMapper.readTree(job.getPayload()).path("servicesOnly").asBoolean(false);
        } catch (Exception ignored) {
            return false;
        }
    }

    public List<JobStep> getSteps(UUID jobId) {
        return jobStepRepository.findByJobIdOrderByStepOrderAsc(jobId);
    }

    @Transactional
    public Optional<ClaimedJob> claimNextJob() {
        Optional<Job> normal = jobRepository.findByStatusOrderByCreatedAtAsc(JobStatus.PENDING).stream()
                .filter(job -> !resourceBusy(job)).findFirst();
        if (normal.isPresent()) {
            Job job = normal.get();
            job.setStatus(JobStatus.IN_PROGRESS);
            job.setStartTime(Instant.now());
            job.setEndTime(null);
            jobRepository.save(job);
            return Optional.of(new ClaimedJob(job.getId(), false));
        }

        Optional<Job> rollback = jobRepository.findByStatusOrderByCreatedAtAsc(JobStatus.ROLLBACK_PENDING).stream()
                .filter(job -> !resourceBusy(job)).findFirst();
        if (rollback.isPresent()) {
            Job job = rollback.get();
            job.setStatus(JobStatus.ROLLING_BACK);
            job.setEndTime(null);
            jobRepository.save(job);
            return Optional.of(new ClaimedJob(job.getId(), true));
        }
        return Optional.empty();
    }

    private boolean resourceBusy(Job job) {
        return job.getResourceKey() != null && !job.getResourceKey().isBlank()
                && !jobRepository.findByResourceKeyAndStatusIn(job.getResourceKey(),
                List.of(JobStatus.IN_PROGRESS, JobStatus.ROLLING_BACK)).isEmpty();
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
    public void reportRollingRestartPhase(UUID stepId, String message) {
        JobStep step = getStep(stepId);
        if (step.getJob().getType() != JobType.ROLLING_RESTART || step.getStatus() != JobStepStatus.IN_PROGRESS)
            throw new IllegalStateException("Restart phase requires an active rolling restart step.");
        step.setLogs(message);
        jobStepRepository.save(step);
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
    public void recordExternalDiscoveryTask(UUID stepId, UUID taskId) {
        JobStep step = getStep(stepId);
        try {
            Map<String, Object> payload = step.getPayload() == null || step.getPayload().isBlank()
                    ? new java.util.LinkedHashMap<>()
                    : new java.util.LinkedHashMap<>(objectMapper.readValue(step.getPayload(), new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));
            payload.put("externalDiscoveryTaskId", taskId.toString());
            step.setPayload(objectMapper.writeValueAsString(payload));
            jobStepRepository.save(step);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Cannot record external discovery task", e);
        }
    }

    @Transactional
    public void attachExternalServiceTask(UUID stepId, UUID taskId) {
        JobStep step = getStep(stepId);
        step.setExternalServiceTaskId(taskId);
        jobStepRepository.save(step);
    }

    @Transactional
    public void recordRollingRestartBaseline(UUID stepId, UUID taskId, Map<String, Object> baseline) {
        JobStep step = getStep(stepId);
        if (step.getJob().getType() != JobType.ROLLING_RESTART)
            throw new IllegalArgumentException("Service baseline is only valid for rolling restart jobs.");
        try {
            Map<String, Object> payload = objectMapper.readValue(step.getPayload(), new com.fasterxml.jackson.core.type.TypeReference<>() {});
            payload.put("baselineAgentTaskId", taskId.toString());
            payload.put("baselineServiceState", new java.util.LinkedHashMap<>(baseline));
            step.setPayload(objectMapper.writeValueAsString(payload));
            jobStepRepository.save(step);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot persist Kafka service baseline", e);
        }
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
        Job job = jobRepository.findForUpdateById(id).orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        if (job.getStatus() != JobStatus.FAILED
                && job.getStatus() != JobStatus.PARTIAL_SUCCESS
                && job.getStatus() != JobStatus.ROLLBACK_FAILED) {
            throw new RuntimeException("Only failed or partially successful jobs can be retried.");
        }

        lockAndCheck(job.getResourceKey(), job.getType(), id);
        boolean retryRollback = job.getStatus() == JobStatus.ROLLBACK_FAILED;
        if (!retryRollback) {
            for (JobStep step : getSteps(id)) {
                if (step.getStatus() != JobStepStatus.SUCCESS) {
                    step.setStatus(JobStepStatus.PENDING);
                    if (job.getType() != JobType.ROLLING_RESTART && job.getType() != JobType.EXTERNAL_ROLLING_RESTART && job.getType() != JobType.CLUSTER_DELETION) step.setAgentTaskId(null);
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
        Job job = jobRepository.findForUpdateById(id).orElseThrow(() -> new IllegalArgumentException("Job not found: " + id));
        lockAndCheck(job.getResourceKey(), job.getType(), id);
        if (job.getType() == JobType.ROLLING_RESTART || !Boolean.TRUE.equals(job.getRollbackSupported())) {
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
