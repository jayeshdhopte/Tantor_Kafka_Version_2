package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.KafkaExpansionOperation;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.LockModeType;
import java.util.*;

public interface KafkaExpansionOperationRepository extends JpaRepository<KafkaExpansionOperation,UUID> {
    List<KafkaExpansionOperation> findByClusterIdAndHostIdAndState(UUID clusterId,String hostId,io.translab.tantor.server.service.KafkaExpansionState state);
    Optional<KafkaExpansionOperation> findByJobId(UUID jobId);
    Optional<KafkaExpansionOperation> findByClusterIdAndRequestKey(UUID clusterId,String requestKey);
    @Lock(LockModeType.PESSIMISTIC_WRITE) @Query("select o from KafkaExpansionOperation o where o.id=:id")
    Optional<KafkaExpansionOperation> findForUpdate(@Param("id") UUID id);
    @Query("select o from KafkaExpansionOperation o where o.clusterId=:cluster and o.state not in (io.translab.tantor.server.service.KafkaExpansionState.SUCCEEDED,io.translab.tantor.server.service.KafkaExpansionState.CANCELLED_SAFE)")
    List<KafkaExpansionOperation> findReservedForCluster(@Param("cluster") UUID cluster);
    @Query("select o from KafkaExpansionOperation o where o.state not in (io.translab.tantor.server.service.KafkaExpansionState.SUCCEEDED,io.translab.tantor.server.service.KafkaExpansionState.CANCELLED_SAFE)")
    List<KafkaExpansionOperation> findAllReserved();
}
