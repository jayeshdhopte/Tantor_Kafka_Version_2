package io.translab.tantor.artifact.dto;

import io.translab.tantor.artifact.domain.Artifact;
import io.translab.tantor.artifact.domain.ArtifactStatus;
import io.translab.tantor.artifact.domain.ServiceType;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Outward-facing view of an artifact. Excludes the on-disk path for safety. */
public record ArtifactResponse(
        UUID id,
        ServiceType serviceType,
        String version,
        String fileName,
        String relativePath,
        String fullFilePath,
        UUID rootArtifactId,
        String action,
        long fileSizeBytes,
        String contentType,
        String sha256,
        String md5,
        ArtifactStatus status,
        String createdBy,
        String updatedBy,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        String downloadUrl
) {
    public static ArtifactResponse from(Artifact a) {
        return new ArtifactResponse(
                a.getId(),
                a.getServiceType(),
                a.getVersion(),
                a.getFileName(),
                a.getRelativePath(),
                a.getFullFilePath(),
                a.getRootArtifactId(),
                a.getAction(),
                a.getFileSizeBytes(),
                a.getContentType(),
                a.getChecksumSha256(),
                a.getChecksumMd5(),
                a.getStatus(),
                a.getCreatedBy(),
                a.getUpdatedBy(),
                a.getCreatedAt(),
                a.getUpdatedAt(),
                "/api/v1/artifacts/" + a.getId() + "/download"
        );
    }
}
