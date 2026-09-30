package io.translab.tantor.server.domain.canonical;

import java.util.Objects;
import java.util.UUID;

/**
 * The only supported identity key for matching a Kafka node. Hostname, IP,
 * display name and telemetry values are intentionally excluded.
 */
public record CanonicalNodeIdentity(
        UUID clusterUuid,
        String kafkaClusterId,
        int nodeId,
        CanonicalNodeRole role) {

    public CanonicalNodeIdentity {
        Objects.requireNonNull(clusterUuid, "clusterUuid must not be null");
        kafkaClusterId = requireText(kafkaClusterId, "kafkaClusterId");
        if (nodeId < 0) {
            throw new IllegalArgumentException("nodeId must be zero or greater");
        }
        Objects.requireNonNull(role, "role must not be null");
        if (role == CanonicalNodeRole.UNKNOWN) {
            throw new IllegalArgumentException("UNKNOWN cannot be used in a node identity");
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
