package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.ExternalClusterNode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ExternalClusterNodeRepository extends JpaRepository<ExternalClusterNode, UUID> {

    List<ExternalClusterNode> findByCanonicalClusterUuid(UUID canonicalClusterUuid);

    List<ExternalClusterNode> findByClusterId(UUID clusterId);

    Optional<ExternalClusterNode> findByClusterIdAndNodeId(UUID clusterId, Integer nodeId);

    @Modifying
    void deleteByClusterId(UUID clusterId);

    @Modifying
    @Query(value = """
        INSERT INTO kf_external_cluster_nodes (
            id, cluster_id, host, node_id, is_broker, is_controller, port,
            cpu_usage_pct, memory_used_mb, memory_total_mb, disk_used_gb, disk_total_gb, last_seen
        ) VALUES (
            gen_random_uuid(), :clusterId, :host, :nodeId, :isBroker, :isController, :port,
            NULL, NULL, NULL, NULL, NULL, NULL
        )
        ON CONFLICT (cluster_id, node_id) DO UPDATE SET
            host = EXCLUDED.host,
            is_broker = EXCLUDED.is_broker,
            is_controller = EXCLUDED.is_controller,
            port = EXCLUDED.port
        """, nativeQuery = true)
    void upsertTopology(
        @Param("clusterId") UUID clusterId,
        @Param("host") String host,
        @Param("nodeId") Integer nodeId,
        @Param("isBroker") Boolean isBroker,
        @Param("isController") Boolean isController,
        @Param("port") Integer port
    );

    @Modifying
    @Query(value = """
        UPDATE kf_external_cluster_nodes SET
            cpu_usage_pct = :cpu,
            memory_used_mb = :memUsed,
            memory_total_mb = :memTotal,
            disk_used_gb = :diskUsed,
            disk_total_gb = :diskTotal,
            disk_used_bytes = :diskUsedBytes,
            disk_total_bytes = :diskTotalBytes,
            last_seen = :lastSeen
        WHERE cluster_id = :clusterId AND host = :host
        """, nativeQuery = true)
    void upsertTelemetry(
        @Param("clusterId") UUID clusterId,
        @Param("host") String host,
        @Param("cpu") Double cpu,
        @Param("memUsed") Long memUsed,
        @Param("memTotal") Long memTotal,
        @Param("diskUsed") Long diskUsed,
        @Param("diskTotal") Long diskTotal,
        @Param("diskUsedBytes") Long diskUsedBytes,
        @Param("diskTotalBytes") Long diskTotalBytes,
        @Param("lastSeen") OffsetDateTime lastSeen
    );

    @Modifying
    @Query(value = """
        UPDATE kf_external_cluster_nodes SET
            messages_in_per_sec = :messagesInPerSec,
            bytes_in_per_sec = :bytesInPerSec
        WHERE cluster_id = :clusterId AND host = :host AND is_broker = TRUE
        """, nativeQuery = true)
    void updateBrokerIngestionRates(
        @Param("clusterId") UUID clusterId,
        @Param("host") String host,
        @Param("messagesInPerSec") Double messagesInPerSec,
        @Param("bytesInPerSec") Double bytesInPerSec
    );
}
