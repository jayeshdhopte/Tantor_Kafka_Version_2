package io.translab.tantor.server.domain.canonical;

import java.util.Objects;
import java.util.UUID;

/**
 * Stable cluster identity contract. The Kafka cluster id may be absent while a
 * cluster is being enrolled, but it must be present before a node identity can
 * be constructed.
 */
public record CanonicalClusterContract(
        UUID clusterUuid,
        String kafkaClusterId,
        CanonicalClusterType type,
        CanonicalKafkaMode mode) {

    public CanonicalClusterContract {
        Objects.requireNonNull(clusterUuid, "clusterUuid must not be null");
        kafkaClusterId = normalizeOptional(kafkaClusterId);
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
    }

    private static String normalizeOptional(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
