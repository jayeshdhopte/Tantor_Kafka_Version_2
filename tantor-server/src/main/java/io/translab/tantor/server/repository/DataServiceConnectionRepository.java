package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.DataServiceConnection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface DataServiceConnectionRepository extends JpaRepository<DataServiceConnection, UUID> {

    /**
     * Lookup a specific named active connection for a cluster+service.
     */
    Optional<DataServiceConnection> findByClusterIdAndServiceTypeAndConnectionNameAndIsActiveTrue(
            UUID clusterId, String serviceType, String connectionName);

    /**
     * List all active connections for a cluster+service (for the instance switcher dropdown).
     */
    List<DataServiceConnection> findByClusterIdAndServiceTypeAndIsActiveTrueOrderByConnectionNameAsc(
            UUID clusterId, String serviceType);

    /**
     * Find the default active connection for a cluster+service (when no connectionId is given).
     */
    Optional<DataServiceConnection> findByClusterIdAndServiceTypeAndIsDefaultTrueAndIsActiveTrue(
            UUID clusterId, String serviceType);

    /**
     * Find a specific active connection by its UUID, VALIDATED against clusterId and serviceType.
     * Prevents cross-cluster and cross-service contamination (e.g. using a Schema Registry
     * connection on Kafka Connect APIs, or using a connection from a different cluster).
     */
    Optional<DataServiceConnection> findByIdAndClusterIdAndServiceTypeAndIsActiveTrue(
            UUID id, UUID clusterId, String serviceType);

    /**
     * Count active connections for a cluster+service.
     * Used to auto-assign is_default on the first saved connection.
     */
    long countByClusterIdAndServiceTypeAndIsActiveTrue(UUID clusterId, String serviceType);
}
