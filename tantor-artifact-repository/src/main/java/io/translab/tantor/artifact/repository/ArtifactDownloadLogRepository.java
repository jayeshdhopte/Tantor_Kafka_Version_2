package io.translab.tantor.artifact.repository;

import io.translab.tantor.artifact.domain.ArtifactDownloadLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ArtifactDownloadLogRepository extends JpaRepository<ArtifactDownloadLog, Long> {
    long countByArtifactId(UUID artifactId);
}
