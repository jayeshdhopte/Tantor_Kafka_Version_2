package io.translab.tantor.artifact.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Persistent metadata for a single stored artifact. The binary itself lives on
 * disk at {@link #relativePath}; this row is the index entry.
 */
@Entity
@Table(name = "kf_artifact")
public class Artifact {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(name = "service_type", nullable = false, length = 40)
    private ServiceType serviceType;

    @Column(name = "version_no", nullable = false, length = 80)
    private String version;

    @Column(name = "binary_file_name", nullable = false, length = 512)
    private String fileName;

    @Column(name = "path_of_tar", nullable = false, length = 1024)
    private String relativePath;

    @Column(name = "full_file_path", length = 2048)
    private String fullFilePath;

    @Column(name = "root_artifact_id", nullable = false)
    private UUID rootArtifactId;

    @Column(nullable = false, length = 40)
    private String action = "UPLOAD";

    @Column(name = "file_size_bytes", nullable = false)
    private long fileSizeBytes;

    @Column(name = "content_type", nullable = false, length = 128)
    private String contentType = "application/gzip";

    @Column(name = "checksum", nullable = false, length = 64)
    private String checksumSha256;

    @Column(name = "checksum_md5", length = 32)
    private String checksumMd5;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private ArtifactStatus status = ArtifactStatus.UPLOADING;

    @Column(name = "created_by", nullable = false, length = 128)
    private String createdBy = "system";

    @Column(name = "updated_by", nullable = false, length = 128)
    private String updatedBy = "system";

    @Column(name = "downloaded_by", length = 128)
    private String downloadedBy;

    @Column(name = "downloaded_at")
    private OffsetDateTime downloadedAt;

    @Column(name = "verified_checksum")
    private Boolean verifiedChecksum;

    @Column(name = "created_time", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "update_time", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "host_ip", length = 100)
    private String hostIp;

    @Column(name = "hostname", length = 255)
    private String hostname;

    /** Who uploaded/triggered the artifact action. Stored in kf_artifact.user_name. */
    @Column(name = "user_name", length = 255)
    private String userName;

    @PrePersist
    void onCreate() {
        if (id == null) {
            id = UUID.randomUUID();
        }
        if (rootArtifactId == null) rootArtifactId = id;
        OffsetDateTime now = OffsetDateTime.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }

    // --- getters / setters ------------------------------------------------

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public ServiceType getServiceType() { return serviceType; }
    public void setServiceType(ServiceType serviceType) { this.serviceType = serviceType; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }

    public String getRelativePath() { return relativePath; }
    public void setRelativePath(String relativePath) { this.relativePath = relativePath; }
    public String getFullFilePath() { return fullFilePath; }
    public void setFullFilePath(String fullFilePath) { this.fullFilePath = fullFilePath; }
    public UUID getRootArtifactId() { return rootArtifactId; }
    public void setRootArtifactId(UUID rootArtifactId) { this.rootArtifactId = rootArtifactId; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public long getFileSizeBytes() { return fileSizeBytes; }
    public void setFileSizeBytes(long fileSizeBytes) { this.fileSizeBytes = fileSizeBytes; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public String getChecksumSha256() { return checksumSha256; }
    public void setChecksumSha256(String checksumSha256) { this.checksumSha256 = checksumSha256; }

    public String getChecksumMd5() { return checksumMd5; }
    public void setChecksumMd5(String checksumMd5) { this.checksumMd5 = checksumMd5; }

    public ArtifactStatus getStatus() { return status; }
    public void setStatus(ArtifactStatus status) { this.status = status; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public String getUpdatedBy() { return updatedBy; }
    public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }
    public String getDownloadedBy() { return downloadedBy; }
    public void setDownloadedBy(String downloadedBy) { this.downloadedBy = downloadedBy; }
    public OffsetDateTime getDownloadedAt() { return downloadedAt; }
    public void setDownloadedAt(OffsetDateTime downloadedAt) { this.downloadedAt = downloadedAt; }
    public Boolean getVerifiedChecksum() { return verifiedChecksum; }
    public void setVerifiedChecksum(Boolean verifiedChecksum) { this.verifiedChecksum = verifiedChecksum; }

    public OffsetDateTime getCreatedAt() { return createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }

    public String getHostIp() { return hostIp; }
    public void setHostIp(String hostIp) { this.hostIp = hostIp; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public String getUserName() { return userName; }
    public void setUserName(String userName) { this.userName = userName; }

}
