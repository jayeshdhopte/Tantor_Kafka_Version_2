package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "kf_host_alerts")
@Getter
@Setter
public class HostAlert {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false, length = 50)
    private String severity;

    @Column(nullable = false, length = 255)
    private String title;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(nullable = false, length = 50)
    private String status;

    @Column(name = "host_id", nullable = false, length = 255)
    private String hostId;

    @Column(name = "cluster_id")
    private UUID clusterId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt = OffsetDateTime.now();

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt = OffsetDateTime.now();

    @Column(name = "resolved_at")
    private OffsetDateTime resolvedAt;

    @Column(name = "log_path", length = 512)
    private String logPath;

    @Column(name = "host_name", length = 255)
    private String hostName;

    @Column(name = "host_ip", length = 100)
    private String hostIp;

    @Column(name = "agent_status", length = 50)
    private String agentStatus;

    @Column(name = "last_heartbeat")
    private OffsetDateTime lastHeartbeat;

    @Column(length = 50)
    private String env;

    @Column(name = "alert_user", length = 128)
    private String alertUser;

    @Column(name = "created_by", length = 128)
    private String createdBy;

    @Column(name = "resolved_by", length = 128)
    private String resolvedBy;

    @Column(name = "resource_type", length = 50)
    private String resourceType = "HOST";
}
