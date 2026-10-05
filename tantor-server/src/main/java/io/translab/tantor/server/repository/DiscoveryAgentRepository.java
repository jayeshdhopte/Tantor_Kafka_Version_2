package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.DiscoveryAgent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface DiscoveryAgentRepository extends JpaRepository<DiscoveryAgent, String> {
    List<DiscoveryAgent> findByClusterId(UUID clusterId);
    Optional<DiscoveryAgent> findByHostname(String hostname);

    @Query("select case when count(agent) > 0 then true else false end from DiscoveryAgent agent "
            + "where lower(trim(agent.hostname)) = :nodeName and agent.id <> :hostId")
    boolean existsNodeNameOwnedByAnotherHost(@Param("nodeName") String nodeName,
                                             @Param("hostId") String hostId);
}
