package io.translab.tantor.server.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.translab.tantor.server.domain.AuditLog;
import jakarta.persistence.criteria.Predicate;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class AuditService {
    private static final Set<String> SENSITIVE_KEYS = Set.of(
            "password", "passwordhash", "token", "secret", "credential", "authorization", "privatekey");

    private final AuditLogRepository repository;
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public UUID record(String category, String action, String resourceType, String resourceId,
                       UUID clusterId, String status, Object oldValue, Object newValue,
                       Object approval, Object details) {
        return recordAs(null, "MANAGEMENT_SERVER", null, category, action, resourceType, resourceId,
                clusterId, status, oldValue, newValue, approval, details);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public UUID recordAs(String actorOverride, String source, String ipOverride,
                         String category, String action, String resourceType, String resourceId,
                         UUID clusterId, String status, Object oldValue, Object newValue,
                         Object approval, Object details) {
        AuditLog event = new AuditLog();
        event.setCreatedBy(resolveActor(actorOverride));
        event.setOrigin(text(source, "MANAGEMENT_SERVER"));
        event.setCategory(text(category, "SYSTEM").toUpperCase(Locale.ROOT));
        event.setAction(text(action, "UNKNOWN").toUpperCase(Locale.ROOT));
        event.setResourceType(text(resourceType, "SYSTEM").toUpperCase(Locale.ROOT));
        event.setResourceId(resourceId);
        event.setResource(resourceId);
        event.setClusterId(clusterId);
        event.setStatus(auditStatus(status));
        event.setDetails(json(details));
        event.setCreatedTime(Instant.now());


        if ("ARTIFACT".equalsIgnoreCase(event.getResourceType()) && event.getResourceId() != null) {
            try {
                java.util.List<Object[]> hostInfo = repository.findArtifactHostInfo(event.getResourceId());
                if (hostInfo != null && !hostInfo.isEmpty()) {
                    Object[] row = hostInfo.get(0);
                    if (row[0] != null) event.setHostIp(row[0].toString());
                    if (row[1] != null) event.setHostName(row[1].toString());
                }
            } catch (Exception e) {
                // Ignore invalid UUID or DB errors
            }
        } else if ("HOST".equalsIgnoreCase(event.getResourceType()) && event.getResourceId() != null) {
            event.setHostId(event.getResourceId());
            try {
                java.util.List<Object[]> hostInfo = repository.findHostInfo(event.getResourceId());
                if (hostInfo != null && !hostInfo.isEmpty()) {
                    Object[] row = hostInfo.get(0);
                    if (row[0] != null) event.setHostIp(row[0].toString());
                    if (row[1] != null) {
                        event.setHostName(row[1].toString());
                        event.setResource(row[1].toString());
                    }
                }
            } catch (Exception e) {
                // Ignore lookup failures; audit recording must not block the action.
            }
        } else if ("CLUSTER".equalsIgnoreCase(event.getResourceType())) {
            UUID lookupClusterId = clusterId;
            if (lookupClusterId == null && event.getResourceId() != null) {
                try {
                    lookupClusterId = UUID.fromString(event.getResourceId());
                    event.setClusterId(lookupClusterId);
                } catch (Exception ignored) {
                    // Keep the raw resource id if it is not a UUID.
                }
            }
            if (lookupClusterId != null) {
                try {
                    java.util.List<Object[]> clusterInfo = repository.findClusterInfo(lookupClusterId.toString());
                    if (clusterInfo != null && !clusterInfo.isEmpty()) {
                        Object[] row = clusterInfo.get(0);
                        if (row[0] != null) event.setResource(row[0].toString());
                        if (row[1] != null && event.getHostIp() == null) {
                            event.setHostIp(firstHost(row[1].toString()));
                        }
                    }
                } catch (Exception e) {
                    // Ignore lookup failures; audit recording must not block the action.
                }
            }
        }

        return repository.saveAndFlush(event).getId();
    }

    public Page<AuditLog> search(String category, String action, String status, String resourceType,
                                 String actor, String search, Instant from, Instant to, int page, int size) {
        int safeSize = Math.min(Math.max(size, 1), 500);
        return repository.findAll((root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            equalIgnoreCase(predicates, cb, root.get("category"), category);
            equalIgnoreCase(predicates, cb, root.get("action"), action);
            equalIgnoreCase(predicates, cb, root.get("status"), status);
            equalIgnoreCase(predicates, cb, root.get("resourceType"), resourceType);
            predicates.add(cb.upper(root.get("status")).in("SUCCESS", "FAILED"));
            if (actor != null && !actor.isBlank()) predicates.add(cb.equal(root.get("createdBy"), actor));
            if (from != null) predicates.add(cb.greaterThanOrEqualTo(root.get("createdTime"), from));
            if (to != null) predicates.add(cb.lessThanOrEqualTo(root.get("createdTime"), to));
            if (search != null && !search.isBlank()) {
                String like = "%" + search.toLowerCase(Locale.ROOT) + "%";
                predicates.add(cb.or(
                        cb.like(cb.lower(root.get("resourceId")), like),
                        cb.like(cb.lower(root.get("resource")), like),
                        cb.like(cb.lower(root.get("hostName")), like),
                        cb.like(cb.lower(root.get("hostIp")), like),
                        cb.like(cb.lower(root.get("createdBy")), like),
                        cb.like(cb.lower(root.get("action")), like),
                        cb.like(cb.lower(root.get("resourceType")), like)));
            }
            return cb.and(predicates.toArray(Predicate[]::new));
        }, PageRequest.of(Math.max(page, 0), safeSize, Sort.by(Sort.Direction.DESC, "createdTime", "id")));
    }

    public Map<String, Object> summary() {
        return Map.of(
                "total", repository.count(),
                "successful", repository.countByStatus("SUCCESS"),
                "failed", repository.countByStatus("FAILED"),
                "approvals", repository.countByCategory("APPROVAL"),
                "integrity", "NOT_ENABLED"
        );
    }

    public String kafkaClusterId(AuditLog event) {
        String fromCluster = kafkaClusterIdFromClusterReference(event);
        if (fromCluster != null) return fromCluster;
        return kafkaClusterIdFromDetails(event);
    }

    public String displayResourceId(AuditLog event) {
        if (isType(event, "ARTIFACT")) return blankToNull(event.getResourceId());
        if (isType(event, "HOST")) return firstNonBlank(event.getHostId(), event.getResourceId());
        if (isType(event, "CLUSTER")) return firstNonBlank(event.getResourceId(), id(event.getClusterId()));
        if (event.getClusterId() != null) return event.getClusterId().toString();
        return firstNonBlank(event.getResourceId(), event.getHostId(), event.getResource());
    }

    public String verifyIntegrity() {
        return "NOT_ENABLED";
    }

    private void equalIgnoreCase(List<Predicate> predicates, jakarta.persistence.criteria.CriteriaBuilder cb,
                                 jakarta.persistence.criteria.Path<String> path, String value) {
        if (value != null && !value.isBlank()) predicates.add(cb.equal(cb.upper(path), value.toUpperCase(Locale.ROOT)));
    }

    private String json(Object value) {
        if (value == null) return null;
        try {
            JsonNode node = objectMapper.valueToTree(value);
            redact(node);
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            return "{\"captureError\":\"Unable to serialize audit value\"}";
        }
    }

    private void redact(JsonNode node) {
        if (node == null) return;
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            List<String> fields = new ArrayList<>();
            object.fieldNames().forEachRemaining(fields::add);
            for (String field : fields) {
                String normalized = field.replace("_", "").replace("-", "").toLowerCase(Locale.ROOT);
                if (SENSITIVE_KEYS.stream().anyMatch(normalized::contains)) object.put(field, "[REDACTED]");
                else redact(object.get(field));
            }
        } else if (node.isArray()) {
            node.forEach(this::redact);
        }
    }

    public String currentActor() {
        return io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
    }

    private String text(String value, String fallback) { return value == null || value.isBlank() ? fallback : value; }

    private String resolveActor(String actorOverride) {
        if (actorOverride == null || actorOverride.isBlank()) {
            return currentActor();
        }
        if (actorOverride.startsWith("agent:")) {
            String hostId = actorOverride.substring("agent:".length());
            try {
                java.util.List<Object[]> rows = repository.findHostAgentInfo(hostId);
                if (rows != null && !rows.isEmpty()) {
                    Object[] row = rows.get(0);
                    String name = row[0] == null || row[0].toString().isBlank()
                            ? (row[2] == null ? hostId : row[2].toString())
                            : row[0].toString();
                    String ip = row[1] == null ? "" : row[1].toString();
                    return ip.isBlank() ? name : name + " (" + ip + ")";
                }
            } catch (Exception ignored) {
                // Keep audit writes non-blocking.
            }
        }
        return actorOverride;
    }

    private String firstHost(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) return null;
        String first = bootstrapServers.split(",")[0].trim();
        int colon = first.indexOf(':');
        return colon > 0 ? first.substring(0, colon) : first;
    }

    private String kafkaClusterIdFromClusterReference(AuditLog event) {
        UUID lookupClusterId = event.getClusterId();
        if (lookupClusterId == null && isType(event, "CLUSTER") && event.getResourceId() != null) {
            try {
                lookupClusterId = UUID.fromString(event.getResourceId());
            } catch (Exception ignored) {
                return null;
            }
        }
        if (lookupClusterId == null) return null;
        try {
            java.util.List<Object[]> clusterInfo = repository.findClusterInfo(lookupClusterId.toString());
            if (clusterInfo != null && !clusterInfo.isEmpty()) {
                Object[] row = clusterInfo.get(0);
                if (row.length > 2) return blankToNull(row[2] == null ? null : row[2].toString());
            }
        } catch (Exception ignored) {
            // Audit display must not fail because a referenced cluster was deleted.
        }
        return null;
    }

    private String kafkaClusterIdFromDetails(AuditLog event) {
        if (event.getDetails() == null || event.getDetails().isBlank()) return null;
        try {
            JsonNode node = objectMapper.readTree(event.getDetails());
            return firstNonBlank(
                    textField(node, "kafkaClusterId"),
                    textField(node, "kafka_cluster_id"),
                    textField(node, "cluster_uuid"),
                    textField(node, "clusterUniqueId")
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private String textField(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        return value == null || value.isNull() ? null : blankToNull(value.asText());
    }

    private boolean isType(AuditLog event, String type) {
        return event.getResourceType() != null && event.getResourceType().equalsIgnoreCase(type);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            String normalized = blankToNull(value);
            if (normalized != null) return normalized;
        }
        return null;
    }

    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private String id(UUID value) {
        return value == null ? null : value.toString();
    }

    private String auditStatus(String value) {
        String normalized = text(value, "SUCCESS").toUpperCase(Locale.ROOT);
        return normalized.contains("FAIL") || normalized.equals("ERROR") ? "FAILED" : "SUCCESS";
    }
}
