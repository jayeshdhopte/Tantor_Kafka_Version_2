package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "kf_cluster_audit_log")
@Getter
@Setter
public class ClusterAuditLog {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "cluster_id", nullable = false)
    private UUID clusterId;

    @Column(length = 50)
    private String severity;

    @Column(length = 255)
    private String title;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(nullable = false, length = 100)
    private String action;

    @Column(length = 255)
    private String event;

    @Column(length = 50)
    private String status;

    @Column(length = 100)
    private String origin;

    @Column(length = 100)
    private String resource;

    @Column(name = "resource_type", length = 50)
    private String resourceType = "CLUSTER";

    @Column(name = "cluster_name", length = 255)
    private String clusterName;

    @Column(name = "bootstrap_ip", length = 100)
    private String bootstrapIp;

    @Column(name = "log_path", length = 1024)
    private String logPath;

    @Column(length = 50)
    private String env;

    @Column(name = "kafka_version", length = 100)
    private String kafkaVersion;

    @Column(length = 100)
    private String mode;


    @Column(name = "created_by", length = 128)
    private String createdBy;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt = OffsetDateTime.now();

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String details;
}
