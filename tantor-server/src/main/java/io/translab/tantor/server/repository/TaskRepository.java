package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;
import java.time.OffsetDateTime;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {
    List<Task> findByHostIdAndStatusOrderByCreatedAtAsc(String hostId, String status);
    List<Task> findByHostIdAndCommandOrderByCreatedAtDesc(String hostId, String command);
    List<Task> findByHostIdInOrderByCreatedAtDesc(List<String> hostIds);
    List<Task> findByClusterIdOrderByCreatedAtDesc(UUID clusterId);
    List<Task> findByClusterIdAndHostIdAndCommandOrderByCreatedAtDesc(UUID clusterId, String hostId, String command);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("update Task t set t.status = 'IN_PROGRESS', t.claimToken = :claimToken, t.claimedAt = :claimedAt, "
            + "t.leaseExpiresAt = :leaseExpiresAt, t.attemptCount = coalesce(t.attemptCount, 0) + 1 "
            + "where t.id = :taskId and t.hostId = :hostId and t.status = 'PENDING'")
    int claimPendingTask(@Param("taskId") UUID taskId, @Param("hostId") String hostId,
                         @Param("claimToken") String claimToken, @Param("claimedAt") OffsetDateTime claimedAt,
                         @Param("leaseExpiresAt") OffsetDateTime leaseExpiresAt);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("update Task t set t.status = 'FAILED', t.errorMsg = 'Task claim lease expired after maximum attempts', "
            + "t.claimToken = null, t.claimedAt = null, t.leaseExpiresAt = null "
            + "where t.hostId = :hostId and t.status = 'IN_PROGRESS' and t.leaseExpiresAt < :now "
            + "and coalesce(t.attemptCount, 0) >= :maxAttempts")
    int failExpiredClaims(@Param("hostId") String hostId, @Param("now") OffsetDateTime now,
                          @Param("maxAttempts") int maxAttempts);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("update Task t set t.status = 'PENDING', t.claimToken = null, t.claimedAt = null, t.leaseExpiresAt = null "
            + "where t.hostId = :hostId and t.status = 'IN_PROGRESS' and t.leaseExpiresAt < :now "
            + "and coalesce(t.attemptCount, 0) < :maxAttempts")
    int releaseExpiredClaims(@Param("hostId") String hostId, @Param("now") OffsetDateTime now,
                             @Param("maxAttempts") int maxAttempts);
}
