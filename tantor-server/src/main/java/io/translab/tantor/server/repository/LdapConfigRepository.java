package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.LdapConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository

public interface LdapConfigRepository extends JpaRepository<LdapConfig, UUID> {
}
