package io.translab.tantor.artifact.audit;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ArtifactAuditRepository extends JpaRepository<ArtifactAuditLog, UUID> {
    Optional<ArtifactAuditLog> findFirstByOrderByCreatedAtDescIdDesc();
    List<ArtifactAuditLog> findAllByOrderByCreatedAtAscIdAsc();
    Page<ArtifactAuditLog> findAllByOrderByCreatedAtDesc(Pageable pageable);
    List<ArtifactAuditLog> findByArtifactIdOrderByCreatedAtDesc(String artifactId);

    @Query(value = "select pg_advisory_xact_lock(772904222)", nativeQuery = true)
    void lockLedger();
}
