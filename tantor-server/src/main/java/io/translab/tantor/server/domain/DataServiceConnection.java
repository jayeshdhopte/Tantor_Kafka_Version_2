package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "kf_data_service_connections")
@Data
public class DataServiceConnection {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /**
     * UUID of the cluster (internal or external). No hard FK — may reference
     * either kf_clusters or kf_external_clusters.
     */
    @Column(name = "cluster_id", nullable = false)
    private UUID clusterId;

    /** 'SCHEMA_REGISTRY' or 'KAFKA_CONNECT' */
    @Column(name = "service_type", nullable = false)
    private String serviceType;

    /** Logical label for this saved connection, e.g. 'Default connection'. */
    @Column(name = "connection_name", nullable = false)
    private String connectionName = "Default connection";

    /** 'http' or 'https'. */
    @Column(name = "protocol", nullable = false)
    private String protocol = "http";

    /** Hostname or IP of the Schema Registry / Kafka Connect instance. */
    @Column(name = "host", nullable = false)
    private String host;

    /** TCP port. */
    @Column(name = "port", nullable = false)
    private Integer port;

    /** 'PEM' or 'PKCS12'. Null when no certificate is configured. */
    @Column(name = "certificate_type")
    private String certificateType;

    /**
     * Base64-encoded certificate content.
     * For PEM: base64(UTF-8 text of the PEM).
     * For PKCS12: base64 of the binary truststore file.
     * Never returned in API responses.
     */
    @Column(name = "certificate_data", columnDefinition = "TEXT")
    private String certificateData;


    /** AES-256/GCM encrypted truststore password. Never returned in API responses. */
    @Column(name = "truststore_password_encrypted")
    private String truststorePasswordEncrypted;


    /** Computed server-side from protocol + host + port. */
    @Column(name = "rest_endpoint")
    private String restEndpoint;

    /** UNKNOWN | ONLINE | OFFLINE | ERROR */
    @Column(name = "status", nullable = false)
    private String status = "UNKNOWN";

    /** Last error message from connectivity test. */
    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    /** When the last connectivity test was run. */
    @Column(name = "last_checked_at")
    private Instant lastCheckedAt;

    /** Soft-delete flag; only one active row per (cluster_id, service_type, connection_name). */
    @Column(name = "is_active", nullable = false)
    private Boolean isActive = true;

    /**
     * When true, this connection is auto-selected when no connectionId is supplied.
     * Only one default per (cluster_id, service_type) can be active at a time.
     */
    @Column(name = "is_default", nullable = false)
    private Boolean isDefault = false;

    @Column(name = "created_by", nullable = false)
    private String createdBy;

    @Column(name = "updated_by", nullable = false)
    private String updatedBy;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        if (createdAt == null) createdAt = now;
        updatedAt = now;
        if (createdBy == null || createdBy.isBlank()) createdBy = io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
        if (updatedBy == null || updatedBy.isBlank()) updatedBy = io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
        if (updatedBy == null || updatedBy.isBlank()) updatedBy = io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
    }
}
