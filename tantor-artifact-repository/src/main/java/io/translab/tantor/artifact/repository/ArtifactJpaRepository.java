package io.translab.tantor.artifact.repository;

import io.translab.tantor.artifact.domain.Artifact;
import io.translab.tantor.artifact.domain.ArtifactStatus;
import io.translab.tantor.artifact.domain.ServiceType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ArtifactJpaRepository extends JpaRepository<Artifact, UUID> {

    @Query("""
            select count(a) from Artifact a
            where a.action = 'UPLOAD'
              and a.serviceType = :serviceType
              and a.version = :version
              and a.status = 'AVAILABLE'
              and not exists (
                  select d.id from Artifact d
                  where d.rootArtifactId = a.rootArtifactId and d.action = 'DELETE'
              )
            """)
    long countActiveByServiceTypeAndVersion(@Param("serviceType") ServiceType serviceType,
                                             @Param("version") String version);

    @Query("""
            select count(a) from Artifact a
            where a.action = 'UPLOAD'
              and a.checksumSha256 = :checksumSha256
              and a.status = 'AVAILABLE'
              and not exists (
                  select d.id from Artifact d
                  where d.rootArtifactId = a.rootArtifactId and d.action = 'DELETE'
              )
            """)
    long countActiveByChecksumSha256(@Param("checksumSha256") String checksumSha256);

    @Query(value = "select 1 from pg_advisory_xact_lock(hashtext(:lockKey))", nativeQuery = true)
    Integer acquireUploadLock(@Param("lockKey") String lockKey);
    List<Artifact> findByRootArtifactIdOrderByCreatedAtAsc(UUID rootArtifactId);

    List<Artifact> findByStatus(ArtifactStatus status);
    List<Artifact> findByStatusAndAction(ArtifactStatus status, String action);

    /**
     * Flexible listing with optional service-type and status filters. A null
     * filter matches all values for that dimension.
     */
    @Query("""
            select a from Artifact a
            where a.action = 'UPLOAD'
              and (:serviceType is null or a.serviceType = :serviceType)
              and (:status      is null or a.status      = :status)
              and not exists (select d.id from Artifact d where d.rootArtifactId = a.id and d.action = 'DELETE')
            """)
    Page<Artifact> search(@Param("serviceType") ServiceType serviceType,
                          @Param("status") ArtifactStatus status,
                          Pageable pageable);
}
