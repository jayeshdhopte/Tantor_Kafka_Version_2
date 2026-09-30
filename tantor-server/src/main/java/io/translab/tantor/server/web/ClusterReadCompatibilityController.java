package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.canonical.CanonicalClusterNodesResponse;
import io.translab.tantor.server.service.CanonicalClusterNodeResolver;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/clusters")
@RequiredArgsConstructor
public class ClusterReadCompatibilityController {

    private final ClusterController clusterController;
    private final CanonicalClusterNodeResolver canonicalClusterNodeResolver;

    @GetMapping("/{id}/overview")
    public ResponseEntity<?> overview(@PathVariable UUID id) {
        return clusterController.getClusterOverview(id);
    }

    @GetMapping("/{id}/nodes")
    public ResponseEntity<CanonicalClusterNodesResponse> nodes(@PathVariable UUID id) {
        return ResponseEntity.ok(canonicalClusterNodeResolver.resolve(id));
    }

    @GetMapping("/{id}/brokers")
    public ResponseEntity<?> brokers(@PathVariable UUID id) {
        ResponseEntity<Map<String, Object>> response = clusterController.getClusterBrokers(id);
        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
            return ResponseEntity.status(response.getStatusCode()).build();
        }
        Object brokers = response.getBody().get("brokers");
        return ResponseEntity.ok(brokers instanceof List<?> list ? list : List.of());
    }
}
