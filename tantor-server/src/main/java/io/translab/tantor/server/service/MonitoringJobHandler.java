package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class MonitoringJobHandler implements JobHandler {

    private final JobService jobService;
    private final DeploymentService deploymentService;
    private final AgentTaskAwaiter taskAwaiter;
    private final ObjectMapper objectMapper;

    @Override
    public boolean supports(JobType type) {
        return type == JobType.MONITORING_ENABLEMENT;
    }

    @Override
    public void execute(Job job) {
        UUID clusterId = UUID.fromString(String.valueOf(readMap(job.getPayload()).get("clusterId")));
        for (JobStep step : jobService.getSteps(job.getId())) {
            if (step.getStatus() == JobStepStatus.SUCCESS) continue;
            Map<String, Object> payload = readMap(step.getPayload());
            try {
                if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                    Task completed = taskAwaiter.await(step.getAgentTaskId());
                    jobService.completeStep(step.getId(), completed.getLogOutput());
                    continue;
                }
                jobService.startStep(step.getId());
                UUID taskId = deploymentService.installMonitoring(
                        clusterId,
                        required(payload, "hostId"),
                        value(payload, "installDir"),
                        required(payload, "prometheusUrl"),
                        required(payload, "grafanaUrl")
                );
                jobService.attachAgentTask(step.getId(), taskId);
                Task task = taskAwaiter.await(taskId);
                jobService.completeStep(step.getId(), task.getLogOutput());
            } catch (Exception e) {
                jobService.failStep(step.getId(), e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public void rollback(Job job) {
        UUID clusterId = UUID.fromString(String.valueOf(readMap(job.getPayload()).get("clusterId")));
        for (JobStep step : jobService.getSteps(job.getId())) {
            if (step.getStatus() != JobStepStatus.SUCCESS
                    && step.getStatus() != JobStepStatus.ROLLBACK_FAILED
                    && step.getStatus() != JobStepStatus.IN_PROGRESS) continue;
            Map<String, Object> payload = readMap(step.getPayload());
            try {
                if (step.getStatus() == JobStepStatus.IN_PROGRESS && step.getAgentTaskId() != null) {
                    Task completed = taskAwaiter.await(step.getAgentTaskId());
                    jobService.rolledBackStep(step.getId(), completed.getLogOutput());
                    continue;
                }
                jobService.startStep(step.getId());
                UUID taskId = deploymentService.removeMonitoring(
                        clusterId, required(payload, "hostId"), value(payload, "installDir"));
                jobService.attachAgentTask(step.getId(), taskId);
                Task task = taskAwaiter.await(taskId);
                jobService.rolledBackStep(step.getId(), task.getLogOutput());
            } catch (Exception e) {
                jobService.rollbackFailedStep(step.getId(), e.getMessage());
                throw e;
            }
        }
    }

    private Map<String, Object> readMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid monitoring job payload", e);
        }
    }

    private String required(Map<String, Object> payload, String key) {
        String value = value(payload, key);
        if (value.isBlank()) throw new IllegalArgumentException("Missing monitoring payload field: " + key);
        return value;
    }

    private String value(Map<String, Object> payload, String key) {
        Object value = payload.get(key);
        return value == null ? "" : String.valueOf(value);
    }
}
