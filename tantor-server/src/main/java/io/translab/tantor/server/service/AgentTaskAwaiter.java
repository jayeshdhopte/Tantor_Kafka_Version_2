package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Task;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class AgentTaskAwaiter {

    private final JdbcTemplate jdbcTemplate;

    @Value("${tantor.jobs.task-timeout-seconds:1800}")
    private long timeoutSeconds;

    public Task await(UUID taskId) {
        return await(taskId, timeoutSeconds);
    }

    public Task await(UUID taskId, long limitSeconds) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(limitSeconds));
        while (Instant.now().isBefore(deadline)) {
            // A request-scoped JPA persistence context can retain the task's original
            // PENDING state while the agent reports SUCCESS in another request.
            List<Task> tasks = jdbcTemplate.query(
                    "SELECT status, log_output, error_msg FROM kf_tasks WHERE id = ?",
                    (rs, rowNum) -> {
                        Task result = new Task();
                        result.setId(taskId);
                        result.setStatus(rs.getString("status"));
                        result.setLogOutput(rs.getString("log_output"));
                        result.setErrorMsg(rs.getString("error_msg"));
                        return result;
                    }, taskId);
            Task task = tasks.stream().findFirst()
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
