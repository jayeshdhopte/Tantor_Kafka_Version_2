package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.HostParcel;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface HostParcelRepository extends JpaRepository<HostParcel, UUID> {
    List<HostParcel> findByArtifactId(UUID artifactId);
    Optional<HostParcel> findFirstByHostIdAndArtifactIdOrderByCreatedAtDescIdDesc(String hostId, UUID artifactId);
    Optional<HostParcel> findFirstByLastTaskIdOrderByCreatedAtDescIdDesc(UUID lastTaskId);

    @org.springframework.data.jpa.repository.Query(value = """
        select hp.* from kf_host_parcels hp
        join (
          select distinct on (host_id, artifact_id) id
          from kf_host_parcels
          order by host_id, artifact_id, created_at desc, id desc
        ) latest on latest.id = hp.id
        order by hp.created_at desc
        """, nativeQuery = true)
    List<HostParcel> findLatestStates();

    @org.springframework.data.jpa.repository.Query(value = """
        select hp.* from kf_host_parcels hp
        join (
          select distinct on (host_id, artifact_id) id
          from kf_host_parcels
          where host_id = :hostId and service_type = :serviceType
          order by host_id, artifact_id, created_at desc, id desc
        ) latest on latest.id = hp.id
        where hp.active = true
        """, nativeQuery = true)
    List<HostParcel> findLatestActive(@org.springframework.data.repository.query.Param("hostId") String hostId,
                                      @org.springframework.data.repository.query.Param("serviceType") String serviceType);

    @org.springframework.data.jpa.repository.Modifying
    @org.springframework.data.jpa.repository.Query(value = """
        INSERT INTO kf_artifact_audit_log (id, user_name, event_category, action, resource_type, artifact_id, status, details, created_at)
        VALUES (gen_random_uuid(), :actor, 'DISTRIBUTION', :action, 'HOST_PARCEL', CAST(:artifactId AS UUID), :status, CAST(:details AS jsonb), now())
        """, nativeQuery = true)
    void recordArtifactAudit(@org.springframework.data.repository.query.Param("actor") String actor,
                             @org.springframework.data.repository.query.Param("action") String action,
                             @org.springframework.data.repository.query.Param("artifactId") String artifactId,
                             @org.springframework.data.repository.query.Param("status") String status,
                             @org.springframework.data.repository.query.Param("details") String details);
}
