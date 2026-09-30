package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "kf_external_clusters")
@Data
public class ExternalCluster {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "cluster_name", nullable = false)
    private String name;

    @Column(name = "bootstrap_servers", nullable = false)
    private String bootstrapServers;

    @Column(name = "kafka_version")
    private String kafkaVersion;

    @Column(name = "kafka_mode")
    private String kafkaMode;

    @Column(name = "kafka_cluster_id")
    private String kafkaClusterId;

    @Column(name = "security")
    private String security;

    @Column(name = "security_protocol")
    private String securityProtocol;

    @Column(name = "sasl_mechanism")
    private String saslMechanism;

    @Column(name = "sasl_username")
    private String saslUsername;

    @Column(name = "sasl_password_encrypted")
    private String saslPasswordEncrypted;

    @Column(name = "truststore_path")
    private String truststorePath;

    @Column(name = "truststore_content_encrypted", columnDefinition = "TEXT")
    private String truststoreContentEncrypted;

    @Column(name = "truststore_password_encrypted")
    private String truststorePasswordEncrypted;

    @Column(name = "truststore_type")
    private String truststoreType;

    @Column(name = "keystore_path")
    private String keystorePath;

    @Column(name = "keystore_content_encrypted", columnDefinition = "TEXT")
    private String keystoreContentEncrypted;

    @Column(name = "keystore_password_encrypted")
    private String keystorePasswordEncrypted;

    @Column(name = "key_password_encrypted")
    private String keyPasswordEncrypted;

    @Column(name = "keystore_type")
    private String keystoreType;

    @Column(name = "disable_hostname_verification")
    private Boolean disableHostnameVerification = false;

    @Column(name = "broker_count")
    private Integer brokerCount;

    @Column(name = "environment")
    private String environment;

    @Column(name = "install_path")
    private String installPath;

    @Column(name = "log_dirs")
    private String logDirs;

    @Column(name = "listeners")
    private String listeners;

    @Column(name = "advertised_listeners")
    private String advertisedListeners;

    @Column(name = "process_roles")
    private String processRoles;


    @Column(name = "cpu_usage_pct")
    private Double cpuUsagePct;

    @Column(name = "memory_used_mb")
    private Long memoryUsedMb;

    @Column(name = "memory_total_mb")
    private Long memoryTotalMb;

    @Column(name = "disk_used_gb")
    private Long diskUsedGb;

    @Column(name = "disk_total_gb")
    private Long diskTotalGb;

    @Column(name = "is_running")
    private Boolean isRunning;

    // e.g. "SUCCESS", "DEGRADED", "FAILED"
    @Column(name = "status")
    private String status = "PENDING";

    @Column(name = "created_by", nullable = false)
    private String createdBy;

    @Column(name = "updated_by", nullable = false)
    private String updatedBy;

    @Column(name = "created_at")
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at")
    private Instant updatedAt = Instant.now();

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
