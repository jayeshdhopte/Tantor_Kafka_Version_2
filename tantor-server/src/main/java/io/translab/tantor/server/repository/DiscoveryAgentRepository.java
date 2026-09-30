package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.DiscoveryAgent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface DiscoveryAgentRepository extends JpaRepository<DiscoveryAgent, String> {
    List<DiscoveryAgent> findByClusterId(UUID clusterId);
    Optional<DiscoveryAgent> findByHostname(String hostname);
}
