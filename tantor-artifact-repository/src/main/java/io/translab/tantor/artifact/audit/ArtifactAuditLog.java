package io.translab.tantor.artifact.audit;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "kf_artifact_audit_log")
public class ArtifactAuditLog {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    @Column(name = "user_name", nullable = false) private String userName;
    @Column(name = "event_category", nullable = false) private String category;
    @Column(nullable = false) private String action;
    @Column(name = "resource_type", nullable = false) private String resourceType;
    @Column(name = "artifact_id") private String artifactId;
    @Column(nullable = false) private String status;
    @JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb") private String details;
    @Column(name = "created_at", nullable = false, updatable = false) private OffsetDateTime createdAt = OffsetDateTime.now();
    
    @Column(name = "host_ip", length = 100) private String hostIp;
    @Column(name = "created_by", length = 128) private String createdBy;
    @Column(name = "host_name", length = 255) private String hostName;

    @Column(name = "checksum", length = 64) private String checksum;

    public UUID getId() { return id; }
    public String getUserName() { return userName; } public void setUserName(String v) { userName = v; }
    public String getCategory() { return category; } public void setCategory(String v) { category = v; }
    public String getAction() { return action; } public void setAction(String v) { action = v; }
    public String getResourceType() { return resourceType; } public void setResourceType(String v) { resourceType = v; }
    public String getArtifactId() { return artifactId; } public void setArtifactId(String v) { artifactId = v; }
    public String getStatus() { return status; } public void setStatus(String v) { status = v; }
    public String getDetails() { return details; } public void setDetails(String v) { details = v; }
    public OffsetDateTime getCreatedAt() { return createdAt; } public void setCreatedAt(OffsetDateTime v) { createdAt = v; }
    
    public String getHostIp() { return hostIp; } public void setHostIp(String v) { hostIp = v; }
    public String getCreatedBy() { return createdBy; } public void setCreatedBy(String v) { createdBy = v; }
    public String getHostName() { return hostName; } public void setHostName(String v) { hostName = v; }

    public String getChecksum() { return checksum; } public void setChecksum(String v) { checksum = v; }
}
