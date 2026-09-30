package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.ExternalCluster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ExternalClusterRepository extends JpaRepository<ExternalCluster, UUID> {
    @org.springframework.data.jpa.repository.Lock(jakarta.persistence.LockModeType.PESSIMISTIC_WRITE)
    @org.springframework.data.jpa.repository.Query("select c from ExternalCluster c where c.id = :id")
    Optional<ExternalCluster> findForServiceUpdate(@org.springframework.data.repository.query.Param("id") UUID id);
    Optional<ExternalCluster> findByName(String name);
    Optional<ExternalCluster> findByKafkaClusterId(String kafkaClusterId);
    Optional<ExternalCluster> findByBootstrapServers(String bootstrapServers);
    List<ExternalCluster> findByStatus(String status);
    List<ExternalCluster> findByStatusNot(String status);
    Optional<ExternalCluster> findByNameAndStatusNot(String name, String status);
    Optional<ExternalCluster> findByBootstrapServersAndStatusNot(String bootstrapServers, String status);
    @org.springframework.data.jpa.repository.Query(value = """
            SELECT EXISTS (
                SELECT 1
                FROM kf_external_clusters
                WHERE LOWER(BTRIM(cluster_name)) = LOWER(BTRIM(:name))
                  AND status IS DISTINCT FROM 'DELETED'
            )
            """, nativeQuery = true)
    boolean existsActiveByNormalizedName(@org.springframework.data.repository.query.Param("name") String name);
}
