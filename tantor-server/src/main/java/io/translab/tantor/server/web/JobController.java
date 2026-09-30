package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/ui/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @GetMapping
    public ResponseEntity<List<Job>> listJobs() {
        return ResponseEntity.ok(jobService.getAllJobs());
    }

    @GetMapping("/{id}")
    public ResponseEntity<Job> getJob(@PathVariable UUID id) {
        try {
            return ResponseEntity.ok(jobService.getJob(id));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/{id}/steps")
    public ResponseEntity<List<JobStep>> getJobSteps(@PathVariable UUID id) {
        try {
            jobService.getJob(id);
            return ResponseEntity.ok(jobService.getSteps(id));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<?> retryJob(@PathVariable UUID id) {
        try {
            return ResponseEntity.ok(jobService.retryJob(id));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(java.util.Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{id}/rollback")
    public ResponseEntity<?> rollbackJob(@PathVariable UUID id) {
        try {
            return ResponseEntity.ok(jobService.requestRollback(id));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(java.util.Map.of("error", e.getMessage()));
        }
    }
}
