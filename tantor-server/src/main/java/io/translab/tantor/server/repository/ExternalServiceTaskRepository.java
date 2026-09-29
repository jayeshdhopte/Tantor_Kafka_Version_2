package io.translab.tantor.server.repository;
import io.translab.tantor.server.domain.ExternalServiceTask;
import org.springframework.data.jpa.repository.*;
import jakarta.persistence.LockModeType;
import java.util.*;
public interface ExternalServiceTaskRepository extends JpaRepository<ExternalServiceTask, UUID> {
 @Lock(LockModeType.PESSIMISTIC_WRITE)
 Optional<ExternalServiceTask> findFirstByAgentIdAndStatusInOrderByCreatedAtAsc(String agentId,Collection<String> statuses);
 @Lock(LockModeType.PESSIMISTIC_WRITE)
 @Query("select t from ExternalServiceTask t where t.id = :id")
 Optional<ExternalServiceTask> findForUpdate(@org.springframework.data.repository.query.Param("id") UUID id);
}
