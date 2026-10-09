package io.translab.tantor.server.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RollingRestartTaskDispatcher {
    private final DeploymentService deploymentService;
    private final JobService jobService;

    /** The agent must never see a restart task before its job step records it. */
    @Transactional
    public UUID dispatch(UUID clusterId, String hostId, String unit, UUID stepId) {
        UUID taskId = deploymentService.restartService(clusterId, hostId, unit);
        jobService.attachAgentTask(stepId, taskId);
        return taskId;
    }
}
