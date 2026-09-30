package io.translab.tantor.server.web;

import io.translab.tantor.server.service.KafkaAdminService;
import io.translab.tantor.server.service.TopicOperationsService;
import io.translab.tantor.server.service.ActivityAlertService;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}")
@RequiredArgsConstructor
public class TopicsController {

    private final KafkaAdminService kafkaAdminService;
    private final TopicOperationsService topicOperationsService;
    private final io.translab.tantor.server.service.PartitionCacheService partitionCacheService;
    private final ClusterRepository clusterRepository;
    private final AuditService auditService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;
    private final ActivityAlertService activityAlertService;

    @GetMapping("/topics")
    public ResponseEntity<io.translab.tantor.server.dto.PaginatedResponse<io.translab.tantor.server.dto.TopicSummaryDto>> listTopics(
            @PathVariable UUID clusterId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "name") String sortBy,
            @RequestParam(defaultValue = "false") boolean includeInternal) {
        
        // Prevent huge page requests
        if (size > 500) size = 500;
        
        return ResponseEntity.ok(kafkaAdminService.listTopicsPaginated(clusterId, page, size, search, sortBy, includeInternal));
    }

    @PostMapping("/topics")
    public ResponseEntity<Void> createTopic(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody TopicCreateRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.TOPIC_MUTATION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        kafkaAdminService.createTopic(
            clusterId, 
            request.getName(), 
            request.getPartitions(), 
            request.getReplicationFactor(), 
            request.getConfigs()
        );
        clusterChanged(clusterId, "TOPIC_CREATED", Map.of("topic", request.getName(),
                "partitions", request.getPartitions(), "replicationFactor", request.getReplicationFactor()));
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Void> deleteTopic(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String topicName) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.TOPIC_MUTATION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        kafkaAdminService.deleteTopic(clusterId, topicName);
        clusterChanged(clusterId, "TOPIC_DELETED", Map.of("topic", topicName));
        return ResponseEntity.ok().build();
    }

    @GetMapping("/topics/{topicName}")
    public ResponseEntity<Map<String, Object>> getTopic(
            @PathVariable UUID clusterId, @PathVariable String topicName) {
        return ResponseEntity.ok(topicOperationsService.getTopicDetails(clusterId, topicName));
    }

    @GetMapping("/topics/{topicName}/messages")
    public ResponseEntity<Map<String, Object>> getMessages(
            @PathVariable UUID clusterId, @PathVariable String topicName,
            @RequestParam(required = false) List<Integer> partitions,
            @RequestParam(defaultValue = "newest") String order,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(required = false) String search) {
        return ResponseEntity.ok(topicOperationsService.getMessages(
                clusterId, topicName, partitions, order, limit, search));
    }

    @PostMapping("/topics/{topicName}/messages")
    public ResponseEntity<Map<String, Object>> produceMessage(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId, @PathVariable String topicName,
            @RequestBody ProduceMessageRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.PRODUCE_MESSAGE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        return ResponseEntity.ok(topicOperationsService.produceMessage(
                clusterId, topicName, request.getPartition(), request.getKey(),
                request.getValue(), request.getHeaders()));
    }

    @DeleteMapping("/topics/{topicName}/messages")
    public ResponseEntity<Void> clearMessages(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String topicName) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.TOPIC_MUTATION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        topicOperationsService.clearTopic(clusterId, topicName);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/topics/{topicName}/recreate")
    public ResponseEntity<Void> recreateTopic(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String topicName) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.TOPIC_MUTATION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        topicOperationsService.recreateTopic(clusterId, topicName);
        clusterChanged(clusterId, "TOPIC_RECREATED", Map.of("topic", topicName));
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/topics/{topicName}/consumers")
    public ResponseEntity<List<Map<String, Object>>> getConsumers(
            @PathVariable UUID clusterId, @PathVariable String topicName) {
        return ResponseEntity.ok(topicOperationsService.getTopicConsumers(clusterId, topicName));
    }

    @GetMapping("/topics/{topicName}/configs")
    public ResponseEntity<List<Map<String, Object>>> getConfigs(
            @PathVariable UUID clusterId, @PathVariable String topicName) {
        return ResponseEntity.ok(topicOperationsService.getTopicConfigs(clusterId, topicName));
    }

    @PutMapping("/topics/{topicName}/configs/{key}")
    public ResponseEntity<Void> alterConfig(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId, @PathVariable String topicName,
            @PathVariable String key, @RequestBody ConfigValueRequest request) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.TOPIC_MUTATION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        topicOperationsService.alterTopicConfig(clusterId, topicName, key, request.getValue());
        clusterChanged(clusterId, "TOPIC_CONFIG_CHANGED", Map.of("topic", topicName, "key", key));
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/topics/{topicName}/configs/{key}")
    public ResponseEntity<Void> resetConfig(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId, @PathVariable String topicName, @PathVariable String key) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.TOPIC_MUTATION)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        topicOperationsService.resetTopicConfig(clusterId, topicName, key);
        clusterChanged(clusterId, "TOPIC_CONFIG_RESET", Map.of("topic", topicName, "key", key));
        return ResponseEntity.noContent().build();
    }

    private void clusterChanged(UUID clusterId, String action, Map<String, Object> details) {
        String clusterName = clusterRepository.findById(clusterId).map(cluster -> {
            cluster.setUpdatedBy(io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
            clusterRepository.save(cluster);
            return cluster.getName();
        }).orElse(clusterId.toString());
        auditService.record("CLUSTER_CHANGE", action, "CLUSTER", clusterId.toString(), clusterId,
                "SUCCESS", null, null, null, details);

        String topicName = String.valueOf(details.getOrDefault("topic", "unknown"));
        String feedAction = switch (action) {
            case "TOPIC_CREATED" -> "CREATE";
            case "TOPIC_DELETED" -> "DELETE";
            case "TOPIC_RECREATED" -> "RECREATE";
            case "TOPIC_CONFIG_CHANGED" -> "UPDATE";
            case "TOPIC_CONFIG_RESET" -> "RESET";
            default -> "EXECUTE";
        };
        String message = switch (action) {
            case "TOPIC_CREATED" -> "Created topic " + topicName + " in cluster " + clusterName;
            case "TOPIC_DELETED" -> "Deleted topic " + topicName + " from cluster " + clusterName;
            case "TOPIC_RECREATED" -> "Recreated topic " + topicName + " in cluster " + clusterName;
            case "TOPIC_CONFIG_CHANGED" -> "Updated configuration for topic " + topicName + " in cluster " + clusterName;
            case "TOPIC_CONFIG_RESET" -> "Reset configuration for topic " + topicName + " in cluster " + clusterName;
            default -> "Updated topic " + topicName + " in cluster " + clusterName;
        };
        activityAlertService.logAudit("INFO", "TOPIC", feedAction, message,
                "TOPIC", topicName, clusterId, null, null, "SUCCESS", null, null);
    }

    @GetMapping("/topics/{topicName}/statistics")
    public ResponseEntity<Map<String, Object>> getStatistics(
            @PathVariable UUID clusterId, @PathVariable String topicName,
            @RequestParam(defaultValue = "10000") int limit) {
        return ResponseEntity.ok(topicOperationsService.analyzeTopic(clusterId, topicName, limit));
    }

    @GetMapping("/topics/{topicName}/acls")
    public ResponseEntity<List<Map<String, Object>>> getAcls(
            @PathVariable UUID clusterId, @PathVariable String topicName) {
        return ResponseEntity.ok(topicOperationsService.getTopicAcls(clusterId, topicName));
    }

    @GetMapping("/partitions")
    public ResponseEntity<io.translab.tantor.server.dto.PaginatedResponse<io.translab.tantor.server.dto.PartitionSummaryDto>> listPartitions(
            @PathVariable UUID clusterId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "topicName") String sortBy) {
        
        if (size > 500) size = 500;
        return ResponseEntity.ok(partitionCacheService.getPaginatedPartitions(clusterId, page, size, search, sortBy));
    }

    @Data
    public static class TopicCreateRequest {
        private String name;
        private int partitions = 1;
        private short replicationFactor = 1;
        private Map<String, String> configs;
    }

    @Data
    public static class ProduceMessageRequest {
        private Integer partition;
        private String key;
        private String value;
        private Map<String, String> headers;
    }

    @Data
    public static class ConfigValueRequest {
        private String value;
    }
}
