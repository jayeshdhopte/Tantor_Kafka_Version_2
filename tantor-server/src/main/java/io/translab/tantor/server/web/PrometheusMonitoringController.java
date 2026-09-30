package io.translab.tantor.server.web;

import io.translab.tantor.server.service.PrometheusMonitoringService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class PrometheusMonitoringController {

    private final PrometheusMonitoringService monitoringService;

    @GetMapping(value = "/internal/prometheus/targets", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object targets() {
        return monitoringService.prometheusTargets();
    }

    @GetMapping("/api/v1/monitoring/health")
    public Map<String, Object> health() {
        return Map.of("prometheusHealthy", monitoringService.prometheusHealthy());
    }

    @GetMapping("/api/v1/monitoring/clusters")
    public Object clusters(@RequestParam(required = false) String type) {
        return monitoringService.clusters(type);
    }

    @GetMapping("/api/v1/monitoring/clusters/{clusterId}/overview")
    public ResponseEntity<?> overview(
            @PathVariable UUID clusterId,
            @RequestParam(required = false) String nodeId
    ) {
        try {
            return ResponseEntity.ok(monitoringService.overview(clusterId, nodeId));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

}
