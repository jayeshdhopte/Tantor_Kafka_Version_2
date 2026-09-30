package io.translab.tantor.server.persistence;

import io.translab.tantor.server.domain.ConfigVersion;
import io.translab.tantor.server.domain.ConfigVersionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ConfigVersionRepository extends JpaRepository<ConfigVersion, UUID> {
    List<ConfigVersion> findByClusterIdOrderByCreatedAtDesc(UUID clusterId);
    List<ConfigVersion> findByClusterIdAndServiceIdOrderByConfigVersionDesc(UUID clusterId, UUID serviceId);
    Optional<ConfigVersion> findByClusterIdAndServiceIdAndConfigVersion(UUID clusterId, UUID serviceId, Integer configVersion);
    Optional<ConfigVersion> findFirstByClusterIdAndServiceIdAndStatusOrderByConfigVersionDesc(
            UUID clusterId, UUID serviceId, ConfigVersionStatus status);

    @Query("select coalesce(max(version.configVersion), 0) from ConfigVersion version " +
            "where version.clusterId = :clusterId and version.hostId = :hostId " +
            "and version.component = :component and version.configFileName = :fileName")
    Integer maxVersion(@Param("clusterId") UUID clusterId,
                       @Param("hostId") String hostId,
                       @Param("component") String component,
                       @Param("fileName") String fileName);
}
