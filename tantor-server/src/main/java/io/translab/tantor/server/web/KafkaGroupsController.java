package io.translab.tantor.server.web;

import io.translab.tantor.server.service.KafkaGroupInventoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/clusters/{clusterId}/groups")
public class KafkaGroupsController {
    private final KafkaGroupInventoryService inventory;
    @GetMapping
    public List<KafkaGroupInventoryService.Group> list(@PathVariable UUID clusterId) {
        return inventory.list(clusterId);
    }
    @ExceptionHandler(IllegalStateException.class)
    public org.springframework.http.ResponseEntity<?> unavailable() {
        return org.springframework.http.ResponseEntity.status(503).body(java.util.Map.of("message",
                "Group metadata is unavailable. Check broker connectivity and group permissions, then refresh."));
    }
}
