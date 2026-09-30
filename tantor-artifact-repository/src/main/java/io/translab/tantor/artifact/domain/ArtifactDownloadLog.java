package io.translab.tantor.artifact.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Append-only record of every artifact download. BFSI customers require an
 * auditable trail of which binaries were pulled by which agent and when.
 */
@Entity
@Table(name = "kf_artifact_download_log")
public class ArtifactDownloadLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "artifact_id", nullable = false)
    private UUID artifactId;

    @Column(name = "downloaded_by", nullable = false, length = 128)
    private String downloadedBy = "agent";

    @Column(name = "downloaded_at", nullable = false)
    private OffsetDateTime downloadedAt;

    @Column(name = "remote_addr", length = 64)
    private String remoteAddr;

    @Column(name = "verified_checksum", nullable = false)
    private boolean verifiedChecksum;

    @PrePersist
    void onCreate() {
        if (downloadedAt == null) {
            downloadedAt = OffsetDateTime.now();
        }
    }

    public Long getId() { return id; }

    public UUID getArtifactId() { return artifactId; }
    public void setArtifactId(UUID artifactId) { this.artifactId = artifactId; }

    public String getDownloadedBy() { return downloadedBy; }
    public void setDownloadedBy(String downloadedBy) { this.downloadedBy = downloadedBy; }

    public OffsetDateTime getDownloadedAt() { return downloadedAt; }
    public void setDownloadedAt(OffsetDateTime downloadedAt) { this.downloadedAt = downloadedAt; }

    public String getRemoteAddr() { return remoteAddr; }
    public void setRemoteAddr(String remoteAddr) { this.remoteAddr = remoteAddr; }

    public boolean isVerifiedChecksum() { return verifiedChecksum; }
    public void setVerifiedChecksum(boolean verifiedChecksum) { this.verifiedChecksum = verifiedChecksum; }
}
