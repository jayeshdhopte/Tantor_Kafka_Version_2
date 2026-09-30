package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.Alert;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface AlertRepository extends JpaRepository<Alert, UUID> {
    List<Alert> findByStatusOrderByCreatedAtDesc(String status);
    List<Alert> findTop100ByOrderByUpdatedAtDesc();
    Optional<Alert> findByAlertKey(String alertKey);
    List<Alert> findByClusterIdAndStatusAndTitleIn(
            UUID clusterId,
            String status,
            Collection<String> titles);
    List<Alert> findByStatusAndTitleIn(String status, Collection<String> titles);
    long countByStatus(String status);
}
