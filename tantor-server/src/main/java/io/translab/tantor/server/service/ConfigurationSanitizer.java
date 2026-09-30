package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.translab.tantor.server.domain.ConfigVersion;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Removes credential-bearing configuration values before they leave the management API. */
@Component
@RequiredArgsConstructor
public class ConfigurationSanitizer {
    private static final Set<String> SENSITIVE_TOKENS = Set.of(
            "password", "secret", "token", "credential", "authorization", "jaas",
            "privatekey", "apikey", "accesskey", "keystorepass", "truststorepass");

    private final ObjectMapper objectMapper;

    public Map<String, Object> sanitize(Map<String, Object> source) {
        JsonNode node = objectMapper.valueToTree(source == null ? Map.of() : source);
        redact(node);
        return objectMapper.convertValue(node, new TypeReference<LinkedHashMap<String, Object>>() { });
    }

    public List<ConfigVersion> sanitizeVersions(List<ConfigVersion> versions) {
        if (versions == null) return List.of();
        return versions.stream().map(this::sanitize).toList();
    }

    private ConfigVersion sanitize(ConfigVersion source) {
        ConfigVersion safe = new ConfigVersion();
        safe.setId(source.getId());
        safe.setClusterId(source.getClusterId());
        safe.setServiceId(source.getServiceId());
        safe.setHostId(source.getHostId());
        safe.setComponent(source.getComponent());
        safe.setConfigFileName(source.getConfigFileName());
        safe.setConfigVersion(source.getConfigVersion());
        safe.setOldConfig(sanitizeJson(source.getOldConfig()));
        safe.setNewConfig(sanitizeJson(source.getNewConfig()));
        safe.setStatus(source.getStatus());
        safe.setApprovalRequired(source.getApprovalRequired());
        safe.setValidationResult(sanitizeJson(source.getValidationResult()));
        safe.setCreatedBy(source.getCreatedBy());
        safe.setApprovedBy(source.getApprovedBy());
        safe.setJobId(source.getJobId());
        safe.setCreatedAt(source.getCreatedAt());
        safe.setApprovedAt(source.getApprovedAt());
        safe.setAppliedAt(source.getAppliedAt());
        safe.setRollbackVersion(source.getRollbackVersion());
        return safe;
    }

    private String sanitizeJson(String value) {
        if (value == null || value.isBlank()) return value;
        try {
            JsonNode node = objectMapper.readTree(value);
            redact(node);
            return objectMapper.writeValueAsString(node);
        } catch (Exception ignored) {
            // A malformed configuration payload must never bypass redaction.
            return "[REDACTED: unreadable configuration]";
        }
    }

    private void redact(JsonNode node) {
        if (node == null) return;
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            List<String> fields = new ArrayList<>();
            object.fieldNames().forEachRemaining(fields::add);
            for (String field : fields) {
                if (isSensitive(field)) object.put(field, "[REDACTED]");
                else redact(object.get(field));
            }
        } else if (node.isArray()) {
            node.forEach(this::redact);
        }
    }

    private boolean isSensitive(String field) {
        String normalized = field == null ? "" : field.replaceAll("[^A-Za-z0-9]", "")
                .toLowerCase(Locale.ROOT);
        return SENSITIVE_TOKENS.stream().anyMatch(normalized::contains);
    }
}
