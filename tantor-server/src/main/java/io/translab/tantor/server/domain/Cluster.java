package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.GenericGenerator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "kf_clusters")
@Data
public class Cluster {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /**
     * Immutable canonical cluster identity assigned and protected by the
     * database. It is read-only here until the Phase 3 resolver adopts it.
     */
    @Column(name = "canonical_cluster_uuid", nullable = false, insertable = false, updatable = false)
    private UUID canonicalClusterUuid;

    @Column(name = "cluster_name", nullable = false)
    private String name;

    @Column(name = "origin_type", nullable = false)
    private String originType = "INTERNAL";

    @Column(name = "kafka_cluster_id")
    private String kafkaClusterId;

    @Column(name = "install_directory")
    private String installDirectory;

    @Column(name = "config_directory")
    private String configDirectory;

    @Column(name = "data_directory")
    private String dataDirectory;

    @Column(name = "log_directory")
    private String logDirectory;

    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    @Column(name = "node_ids", columnDefinition = "jsonb", nullable = false)
    private List<Integer> nodeIds = new ArrayList<>();

    @Column(name = "created_by", nullable = false)
    private String createdBy;

    @Column(name = "updated_by", nullable = false)
    private String updatedBy;

    @Transient // user column was dropped in V62 migration — kept for API compatibility only
    private String user;

    @Column(name = "role")
    private String role;

    @Column(name = "config_path")
    private String configPath;

    @Column(name = "kafka_version", nullable = false)
    private String kafkaVersion;

    private String mode;
    private String environment;

    @Column(name = "bootstrap_servers")
    private String bootstrapServers;

    @Column(name = "external_broker_hosts_json", columnDefinition = "TEXT")
    private String externalBrokerHostsJson;

    @Column(name = "monitoring_enabled")
    private Boolean monitoringEnabled = true;

    @Column(name = "kafka_exporter_host")
    private String kafkaExporterHost;

    @Column(name = "kafka_exporter_port")
    private Integer kafkaExporterPort;

    @Column(name = "jmx_enabled")
    private Boolean jmxEnabled = true;

    @Column(name = "jmx_exporter_port")
    private Integer jmxExporterPort = 7071;

    @Column(name = "node_exporter_enabled")
    private Boolean nodeExporterEnabled = false;

    @Column(name = "node_exporter_port")
    private Integer nodeExporterPort = 9100;

    @Column(name = "config_json", columnDefinition = "TEXT")
    private String configJson;

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
        if (user == null || user.isBlank()) user = createdBy;
        if (nodeIds == null) nodeIds = new ArrayList<>();
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
        if (updatedBy == null || updatedBy.isBlank()) updatedBy = io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
        if (user == null || user.isBlank()) user = updatedBy;
    }

    @Column(name = "deleted_at")
    private Instant deletedAt;

    private String status = "PENDING"; // PENDING, RUNNING, VALIDATING, SUCCESS, FAILED, DELETING, DELETED

    @OneToMany(mappedBy = "cluster", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<ClusterServiceAssignment> services;
}
