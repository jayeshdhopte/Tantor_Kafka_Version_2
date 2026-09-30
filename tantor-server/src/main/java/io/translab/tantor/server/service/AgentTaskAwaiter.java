package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class AgentTaskAwaiter {

    private final TaskRepository taskRepository;

    @Value("${tantor.jobs.task-timeout-seconds:1800}")
    private long timeoutSeconds;

    public Task await(UUID taskId) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(timeoutSeconds));
        while (Instant.now().isBefore(deadline)) {
            Task task = taskRepository.findById(taskId)
                    .orElseThrow(() -> new RuntimeException("Agent task not found: " + taskId));
            String status = String.valueOf(task.getStatus()).toUpperCase();
            if ("SUCCESS".equals(status)) return task;
            if ("FAILED".equals(status) || "CANCELLED".equals(status)) {
                String reason = task.getErrorMsg() == null || task.getErrorMsg().isBlank()
                        ? task.getLogOutput()
                        : task.getErrorMsg();
                throw new RuntimeException("Agent task " + taskId + " failed: " + reason);
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for agent task " + taskId, e);
            }
        }
        throw new RuntimeException("Timed out waiting for agent task " + taskId);
    }
}
