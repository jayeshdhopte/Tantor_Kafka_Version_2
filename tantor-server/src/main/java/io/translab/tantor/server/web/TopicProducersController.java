package io.translab.tantor.server.web;

import io.translab.tantor.server.service.TopicProducerDiscoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class TopicProducersController {
    private final TopicProducerDiscoveryService discovery;

    @GetMapping("/api/v1/clusters/{clusterId}/topics/{topicName}/producers")
    public ResponseEntity<TopicProducerDiscoveryService.Snapshot> getProducers(
            @PathVariable UUID clusterId, @PathVariable String topicName) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(discovery.get(clusterId, topicName));
    }
}
