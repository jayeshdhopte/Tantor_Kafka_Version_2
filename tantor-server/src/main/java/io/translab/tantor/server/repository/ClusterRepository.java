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

    /** Clear live associations; referenced service/config/restart-history parents remain DELETED.
     * Returns the number of physically deleted parents (zero when history is retained). */
    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true, flushAutomatically = true)
    @org.springframework.data.jpa.repository.Query(value = """
            WITH history_reference AS MATERIALIZED (
                SELECT EXISTS (SELECT 1 FROM kf_add_service WHERE cluster_id = :clusterId)
                    OR EXISTS (SELECT 1 FROM kf_config_change WHERE cluster_id = :clusterId)
                    OR EXISTS (SELECT 1 FROM kf_rolling_restart_table WHERE cluster_id = :clusterId) AS keep_parent
            ), cleared_hosts AS (
                UPDATE kf_hosts SET cluster_id = NULL WHERE cluster_id = :clusterId RETURNING id
            ), deleted_connections AS (
                DELETE FROM kf_data_service_connections WHERE cluster_id = :clusterId RETURNING id
            ), deleted_nodes AS (
                DELETE FROM kf_nodes WHERE cluster_id = :clusterId RETURNING id
            ), deleted_services AS (
                DELETE FROM kf_cluster_services WHERE cluster_id = :clusterId
                  AND (SELECT keep_parent FROM history_reference)
                RETURNING id
            ), retained_history AS (
                UPDATE kf_clusters SET status = 'DELETED',
                    deleted_at = COALESCE(deleted_at, now()), updated_at = now()
                WHERE id = :clusterId
                  AND (SELECT keep_parent FROM history_reference)
                  AND (status IS DISTINCT FROM 'DELETED' OR deleted_at IS NULL)
                RETURNING id
            )
            DELETE FROM kf_clusters WHERE id = :clusterId
              AND NOT (SELECT keep_parent FROM history_reference)
            """, nativeQuery = true)
    int purgeById(@org.springframework.data.repository.query.Param("clusterId") UUID clusterId);
}
