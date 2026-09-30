package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class OnboardingJobHandler implements JobHandler {

    private final JobService jobService;
    private final HostRepository hostRepository;
    private final ObjectMapper objectMapper;

    @Override
    public boolean supports(JobType type) {
        return type == JobType.ONBOARDING;
    }

    @Override
    public void execute(Job job) {
        JobStep step = onlyStep(job);
        if (step.getStatus() == JobStepStatus.SUCCESS) return;
        jobService.startStep(step.getId());
        String hostId = payload(step).get("hostId").toString();
        try {
            var host = hostRepository.findById(hostId)
                    .orElseThrow(() -> new RuntimeException("Host not found: " + hostId));
            host.setStatus("ONLINE");
            hostRepository.save(host);
            jobService.completeStep(step.getId(), "Host approved and connected.");
        } catch (Exception e) {
            jobService.failStep(step.getId(), e.getMessage());
            throw e;
        }
    }

    @Override
    public void rollback(Job job) {
        JobStep step = onlyStep(job);
        String hostId = payload(step).get("hostId").toString();
        try {
            hostRepository.findById(hostId).ifPresent(host -> {
                if (host.getClusterId() != null) {
                    throw new IllegalStateException("Host is assigned to a cluster and cannot be disconnected by rollback.");
                }
                host.setStatus("PENDING");
                hostRepository.save(host);
            });
            jobService.rolledBackStep(step.getId(), "Host returned to discovered state.");
        } catch (Exception e) {
            jobService.rollbackFailedStep(step.getId(), e.getMessage());
            throw e;
        }
    }

    private JobStep onlyStep(Job job) {
        return jobService.getSteps(job.getId()).stream().findFirst()
                .orElseThrow(() -> new RuntimeException("Onboarding job has no step."));
    }

    private Map<String, Object> payload(JobStep step) {
        try {
            return objectMapper.readValue(step.getPayload(), new TypeReference<>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid onboarding payload", e);
        }
    }
}
