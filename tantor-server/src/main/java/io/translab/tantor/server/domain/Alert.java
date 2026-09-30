package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "kf_alerts")
@Data
public class Alert {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String severity; // INFO, WARNING, CRITICAL

    @Column(nullable = false)
    private String title;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "cluster_id")
    private UUID clusterId;

    @Column(name = "cluster_name_snapshot")
    private String clusterNameSnapshot;

    @Column(name = "kafka_cluster_id_snapshot")
    private String kafkaClusterIdSnapshot;

    @Column(name = "alert_key")
    private String alertKey;

    @Column(name = "host_id")
    private String hostId;

    /**
     * Snapshot of the concrete node IPs affected when a cluster-level alert
     * was raised. A cluster health event can affect more than one agent.
     */
    @Column(name = "affected_ips", columnDefinition = "TEXT")
    private String affectedIps;

    @Column(name = "host_ip_snapshot", columnDefinition = "TEXT")
    private String hostIpSnapshot;

    @Column(nullable = false)
    private String source = "stored";

    @Column(name = "error_log", columnDefinition = "TEXT")
    private String errorLog;

    @Column(nullable = false)
    private String status; // ACTIVE, RESOLVED

    @Column(name = "created_at")
    private Instant createdAt = Instant.now();

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "updated_at")
    private Instant updatedAt = Instant.now();

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        if (createdAt == null) createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }
}
