package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "kf_activity_logs")
@Data
public class ActivityLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String level;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    @Column(name = "cluster_id")
    private UUID clusterId;

    @Column(name = "created_at")
    private Instant createdAt = Instant.now();
    @Column(name = "event_type")
    private String eventType;

    private String action;
    private String actor;

    @Column(name = "resource_type")
    private String resourceType;

    @Column(name = "resource_id")
    private String resourceId;

    @Column(name = "old_value", columnDefinition = "TEXT")
    private String oldValue;

    @Column(name = "new_value", columnDefinition = "TEXT")
    private String newValue;

    @Column(name = "ip_address")
    private String ipAddress;

    @Column(name = "event_status")
    private String eventStatus;

    @Column(name = "approval_status")
    private String approvalStatus;

    @Column(columnDefinition = "TEXT")
    private String metadata;
}
