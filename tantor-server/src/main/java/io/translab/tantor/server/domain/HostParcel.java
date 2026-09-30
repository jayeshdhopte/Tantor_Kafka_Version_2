package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "kf_host_parcels")
@Getter
@Setter
public class HostParcel {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "host_id", nullable = false)
    private String hostId;

    @Column(name = "agent_id")
    private UUID agentId;

    @Column(name = "host_ip")
    private String hostIp;

    @Column(name = "artifact_id", nullable = false)
    private UUID artifactId;

    @Column(name = "service_type", nullable = false)
    private String serviceType;

    @Column(nullable = false)
    private String version;

    @Column(name = "file_name")
    private String fileName;

    private String checksum;

    @Column(nullable = false)
    private String action;

    @Column(name = "parcel_dir")
    private String parcelDir;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private boolean active;

    @Column(name = "last_task_id")
    private UUID lastTaskId;

    @Column(name = "error_msg")
    private String errorMsg;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "created_by", nullable = false)
    private String createdBy;

    @Column(name = "updated_by", nullable = false)
    private String updatedBy;

    @PrePersist
    void onCreate() {
        OffsetDateTime now = OffsetDateTime.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
        if (createdBy == null || createdBy.isBlank()) createdBy = io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
        if (updatedBy == null || updatedBy.isBlank()) updatedBy = io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = OffsetDateTime.now();
        if (updatedBy == null || updatedBy.isBlank()) updatedBy = io.translab.tantor.server.security.SecurityUtils.getCurrentUsername();
    }
}
