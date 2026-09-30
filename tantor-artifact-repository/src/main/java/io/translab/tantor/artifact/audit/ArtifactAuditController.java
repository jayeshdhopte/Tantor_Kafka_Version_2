package io.translab.tantor.artifact.audit;

import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/artifacts/audit")
public class ArtifactAuditController {
    private final ArtifactAuditService service;

    public ArtifactAuditController(ArtifactAuditService service) { this.service = service; }

    @GetMapping
    public Map<String, Object> recent(@RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "100") int size) {
        Page<ArtifactAuditLog> result = service.recent(page, size);
        return Map.of("events", result.getContent(), "total", result.getTotalElements(),
                "page", result.getNumber(), "totalPages", result.getTotalPages(), "integrity", service.integrity());
    }

    @GetMapping("/{resourceId}")
    public Map<String, Object> getByResource(@org.springframework.web.bind.annotation.PathVariable String resourceId) {
        java.util.List<ArtifactAuditLog> logs = service.getLogsForResource(resourceId);
        return Map.of("events", logs, "total", logs.size(), "integrity", service.integrity());
    }
}
