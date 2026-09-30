package io.translab.tantor.server.service;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JobRecoveryService {

    private final JobService jobService;

    @EventListener(ApplicationReadyEvent.class)
    public void recoverInterruptedJobs() {
        jobService.recoverInterruptedJobs();
    }
}
