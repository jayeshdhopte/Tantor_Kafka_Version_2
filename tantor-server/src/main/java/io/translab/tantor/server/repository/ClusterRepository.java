package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.Cluster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.EntityGraph;
import java.util.UUID;

public interface ClusterRepository extends JpaRepository<Cluster, UUID> {
    java.util.Optional<Cluster> findByCanonicalClusterUuidAndStatusNot(UUID canonicalClusterUuid, String status);
    java.util.List<Cluster> findByStatusNot(String status);
    java.util.List<Cluster> findByNameAndStatus(String name, String status);
    java.util.Optional<Cluster> findByNameAndStatusNot(String name, String status);
    java.util.List<Cluster> findByModeAndStatusNot(String mode, String status);
    java.util.Optional<Cluster> findByModeAndNameAndStatusNot(String mode, String name, String status);
    java.util.Optional<Cluster> findByModeAndBootstrapServersAndStatusNot(String mode, String bootstrapServers, String status);
    @org.springframework.data.jpa.repository.Query(value = """
            SELECT EXISTS (
                SELECT 1
                FROM kf_clusters
                WHERE LOWER(BTRIM(cluster_name)) = LOWER(BTRIM(:name))
                  AND status IS DISTINCT FROM 'DELETED'
            )
            """, nativeQuery = true)
    boolean existsActiveByNormalizedName(@org.springframework.data.repository.query.Param("name") String name);
    @EntityGraph(attributePaths = "services")
    java.util.Optional<Cluster> findWithServicesById(UUID id);

    @org.springframework.data.jpa.repository.Lock(jakarta.persistence.LockModeType.PESSIMISTIC_WRITE)
    @org.springframework.data.jpa.repository.Query("select c from Cluster c where c.id = :id")
    java.util.Optional<Cluster> findForServiceUpdate(@org.springframework.data.repository.query.Param("id") UUID id);

    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true, flushAutomatically = true)
    @org.springframework.data.jpa.repository.Query(value = """
            WITH cleared_hosts AS (
                UPDATE kf_hosts SET cluster_id = NULL WHERE cluster_id = :clusterId RETURNING id
            ), deleted_connections AS (
                DELETE FROM kf_data_service_connections WHERE cluster_id = :clusterId RETURNING id
            ), deleted_nodes AS (
                DELETE FROM kf_nodes WHERE cluster_id = :clusterId RETURNING id
            )
            DELETE FROM kf_clusters WHERE id = :clusterId
            """, nativeQuery = true)
    int purgeById(@org.springframework.data.repository.query.Param("clusterId") UUID clusterId);
}
