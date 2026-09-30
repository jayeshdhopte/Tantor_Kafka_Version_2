package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "kf_tasks")
@Getter
@Setter
public class Task {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "host_id", nullable = false)
    private String hostId;

    @Column(name = "agent_id")
    private UUID agentId;

    @Column(name = "cluster_id")
    private UUID clusterId;

    @Column(nullable = false)
    private String command;

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String parameters; // JSON string

    @Column(name = "artifact_url")
    private String artifactUrl;

    private String checksum;

    @Column(nullable = false)
    private String status; // PENDING, IN_PROGRESS, SUCCESS, FAILED

    @Column(name = "claim_token")
    private String claimToken;

    @Column(name = "claimed_at")
    private OffsetDateTime claimedAt;

    @Column(name = "lease_expires_at")
    private OffsetDateTime leaseExpiresAt;

    @Column(name = "attempt_count", nullable = false)
    private Integer attemptCount = 0;

    @Column(name = "log_output")
    private String logOutput;

    @Column(name = "error_msg")
    private String errorMsg;

    @Column(name = "current_step")
    private String currentStep;

    @Transient
    public String getFailedReason() {
        return getErrorMsg();
    }

    @Column(name = "step_logs", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String stepLogs; // JSON string representing Map<String, String>


    @Column(name = "created_at", insertable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", insertable = false, updatable = false)
    private OffsetDateTime updatedAt;
}
