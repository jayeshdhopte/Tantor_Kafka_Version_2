package io.translab.tantor.server.repository;
import io.translab.tantor.server.domain.ExternalDataService;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;
public interface ExternalDataServiceRepository extends JpaRepository<ExternalDataService, UUID> {
 List<ExternalDataService> findByClusterId(UUID clusterId);
 List<ExternalDataService> findByAgentId(String agentId);
 boolean existsByClusterIdAndRole(UUID clusterId,String role);
}
