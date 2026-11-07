package io.translab.tantor.server.repository;
import io.translab.tantor.server.domain.BrokerAdditionOperation;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.LockModeType;
import java.util.*;
public interface BrokerAdditionOperationRepository extends JpaRepository<BrokerAdditionOperation,UUID> {
    List<BrokerAdditionOperation> findByClusterIdAndHostIdAndState(UUID clusterId,String hostId,io.translab.tantor.server.service.BrokerAdditionState state);
    Optional<BrokerAdditionOperation> findByClusterIdAndRequestKey(UUID clusterId,String key);
    Optional<BrokerAdditionOperation> findByJobId(UUID jobId);
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select o from BrokerAdditionOperation o where o.id=:id")
    Optional<BrokerAdditionOperation> findForUpdate(@Param("id") UUID id);
    @Query("select o from BrokerAdditionOperation o where o.clusterId=:cluster and o.state not in (io.translab.tantor.server.service.BrokerAdditionState.SUCCEEDED,io.translab.tantor.server.service.BrokerAdditionState.CANCELLED_SAFE)")
    List<BrokerAdditionOperation> findReservedForCluster(@Param("cluster") UUID cluster);
    @Query("select o from BrokerAdditionOperation o where o.state not in (io.translab.tantor.server.service.BrokerAdditionState.SUCCEEDED,io.translab.tantor.server.service.BrokerAdditionState.CANCELLED_SAFE)")
    List<BrokerAdditionOperation> findAllReserved();
}
