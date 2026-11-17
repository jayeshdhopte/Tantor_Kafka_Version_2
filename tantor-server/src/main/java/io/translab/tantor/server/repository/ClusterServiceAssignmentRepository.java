package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.ClusterServiceAssignment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ClusterServiceAssignmentRepository extends JpaRepository<ClusterServiceAssignment, UUID> {
    @Query("select service from ClusterServiceAssignment service join fetch service.cluster cluster where cluster.status is null or cluster.status <> 'DELETED'")
    List<ClusterServiceAssignment> findReservationsForLiveClusters();

    @Query("select service from ClusterServiceAssignment service join fetch service.cluster cluster where service.hostId = :hostId and (cluster.status is null or cluster.status <> 'DELETED')")
    List<ClusterServiceAssignment> findLiveAssignmentsForHost(@Param("hostId") String hostId);

    @Query("select service from ClusterServiceAssignment service where service.cluster.id = :clusterId")
    List<ClusterServiceAssignment> findByClusterId(@Param("clusterId") UUID clusterId);

    @Query("select service from ClusterServiceAssignment service where service.cluster.id = :clusterId and service.hostId = :hostId")
    Optional<ClusterServiceAssignment> findByClusterIdAndHostId(@Param("clusterId") UUID clusterId, @Param("hostId") String hostId);

    @Query("select service from ClusterServiceAssignment service where service.cluster.id = :clusterId and service.role = :role")
    Optional<ClusterServiceAssignment> findByClusterIdAndRole(@Param("clusterId") UUID clusterId, @Param("role") String role);

    @Query("select service from ClusterServiceAssignment service where service.cluster.id = :clusterId and service.role = :role and service.hostId = :hostId")
    Optional<ClusterServiceAssignment> findByClusterIdAndRoleAndHostId(@Param("clusterId") UUID clusterId, @Param("role") String role, @Param("hostId") String hostId);
}
