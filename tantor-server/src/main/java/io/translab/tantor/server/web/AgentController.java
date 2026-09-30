package io.translab.tantor.server.web;

import io.translab.tantor.server.dto.HostHeartbeatDto;
import io.translab.tantor.server.dto.HostRegistrationDto;
import io.translab.tantor.server.dto.TaskDto;
import io.translab.tantor.server.dto.TaskResultDto;
import io.translab.tantor.server.service.AgentService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/agents")
@RequiredArgsConstructor
public class AgentController {
    
    private final AgentService agentService;

    @PostMapping("/register")
    public ResponseEntity<Void> registerHost(@RequestBody HostRegistrationDto registrationDto, HttpServletRequest request) {
        if (!agentService.registerHost(registrationDto, sourceIp(request))) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
        return ResponseEntity.ok().build();
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Void> heartbeat(@RequestBody HostHeartbeatDto heartbeatDto, HttpServletRequest request) {
        AgentService.HeartbeatResult result = agentService.processHeartbeat(heartbeatDto, sourceIp(request));
        if (result == AgentService.HeartbeatResult.ACCEPTED) {
            return ResponseEntity.ok().build();
        }
        if (result == AgentService.HeartbeatResult.SOURCE_MISMATCH) {
            return ResponseEntity.status(HttpStatus.CONFLICT).build();
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping("/{hostId}/tasks")
    public ResponseEntity<List<TaskDto>> pollTasks(@PathVariable String hostId) {
        List<TaskDto> tasks = agentService.getPendingTasks(hostId);
        if (tasks.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(tasks);
    }

    @PostMapping("/tasks/result")
    public ResponseEntity<Void> reportTaskResult(@RequestBody TaskResultDto resultDto) {
        agentService.processTaskResult(resultDto);
        return ResponseEntity.ok().build();
    }

    private String sourceIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        String realIp = request.getHeader("X-Real-IP");
        if (realIp != null && !realIp.isBlank()) {
            return realIp.trim();
        }
        return request.getRemoteAddr();
    }
}
