package io.translab.tantor.server.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JobEngineCoordinator {

    private final JobService jobService;
    private final JobExecutor jobExecutor;

    @Scheduled(fixedDelayString = "${tantor.jobs.poll-delay-ms:2000}")
    public void dispatchClaimedJobs() {
        for (int i = 0; i < 10; i++) {
            var claim = jobService.claimNextJob();
            if (claim.isEmpty()) return;
            jobExecutor.execute(claim.get().jobId(), claim.get().rollback());
        }
    }
}
