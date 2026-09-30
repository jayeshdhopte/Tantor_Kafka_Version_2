package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "kf_config_versions")
@Getter
@Setter
public class ConfigVersion {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "cluster_id", nullable = false)
    private UUID clusterId;

    @Column(name = "service_id")
    private UUID serviceId;

    @Column(name = "host_id", nullable = false)
    private String hostId;

    @Column(nullable = false)
    private String component;

    @Column(name = "config_file_name", nullable = false, length = 500)
    private String configFileName;

    @Column(name = "config_version", nullable = false)
    private Integer configVersion;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "old_config", nullable = false, columnDefinition = "jsonb")
    private String oldConfig;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "new_config", nullable = false, columnDefinition = "jsonb")
    private String newConfig;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ConfigVersionStatus status;

    @Column(name = "approval_required", nullable = false)
    private Boolean approvalRequired = false;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "validation_result", nullable = false, columnDefinition = "jsonb")
    private String validationResult = "{}";

    @Column(name = "created_by", nullable = false)
    private String createdBy;

    @Column(name = "approved_by")
    private String approvedBy;

    @Column(name = "job_id")
    @JdbcTypeCode(SqlTypes.VARCHAR)
    private UUID jobId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "approved_at")
    private Instant approvedAt;

    @Column(name = "applied_at")
    private Instant appliedAt;

    @Column(name = "rollback_version")
    private Integer rollbackVersion;
}
