package io.translab.tantor.server.repository;
import io.translab.tantor.server.domain.ExternalDataService;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;
public interface ExternalDataServiceRepository extends JpaRepository<ExternalDataService, UUID> {
 List<ExternalDataService> findByClusterId(UUID clusterId);
 @org.springframework.data.jpa.repository.Query("select service from ExternalDataService service join ExternalCluster cluster on cluster.id=service.clusterId where service.agentId=:agentId and (cluster.status is null or cluster.status<>'DELETED')")
 List<ExternalDataService> findLiveReservationsByAgentId(@org.springframework.data.repository.query.Param("agentId") String agentId);
 List<ExternalDataService> findByAgentId(String agentId);
 boolean existsByClusterIdAndRole(UUID clusterId,String role);
}
