package io.translab.tantor.server.web;

import io.translab.tantor.server.dto.ConsumerGroupDetailDto;
import io.translab.tantor.server.dto.ConsumerGroupSummaryDto;
import io.translab.tantor.server.dto.PaginatedResponse;
import io.translab.tantor.server.service.ConsumerLagCacheService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/consumer-groups")
@RequiredArgsConstructor
public class ConsumerGroupsController {

    private final ConsumerLagCacheService consumerLagCacheService;

    @GetMapping
    public ResponseEntity<PaginatedResponse<ConsumerGroupSummaryDto>> getConsumerGroups(
            @PathVariable UUID clusterId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "groupId") String sortBy) {
        
        if (size > 500) size = 500;

        return ResponseEntity.ok(consumerLagCacheService.getPaginatedSummaries(clusterId, page, size, search, sortBy));
    }

    @GetMapping("/{groupId}")
    public ResponseEntity<ConsumerGroupDetailDto> getConsumerGroupDetail(
            @PathVariable UUID clusterId,
            @PathVariable String groupId) {
        
        ConsumerGroupDetailDto detail = consumerLagCacheService.getDetail(clusterId, groupId);
        if (detail == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(detail);
    }
}
