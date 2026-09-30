package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.Host;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface HostRepository extends JpaRepository<Host, String> {
    Optional<Host> findFirstByHostnameAndAgentVersion(String hostname, String agentVersion);
}
