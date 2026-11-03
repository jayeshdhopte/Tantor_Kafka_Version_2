package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.ExternalDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Additional storage only: deployment commands and existing API records remain authoritative. */
@Service
@RequiredArgsConstructor
public class AddServiceMappingService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final ApplicationEventPublisher events;
    private static final Set<String> ROLES = Set.of("schema_registry", "kafka_connect");

    @Transactional
    public void recordManaged(ClusterServiceAssignment service, String actor) {
        record(service.getId(), service.getCluster().getId(), service.getHostId(), service.getRole(),
                service.getStatus(), service.getConfigJson(), actor);
    }

    @Transactional
    public void recordExternal(ExternalDataService service, String actor) {
        record(service.getId(), service.getClusterId(), service.getAgentId(), service.getRole(),
                service.getStatus(), service.getConfigJson(), actor);
    }

    private void record(UUID id, UUID clusterId, String hostId, String role, String status, String config, String actor) {
        if (!ROLES.contains(role)) return;
        if (id == null) throw new IllegalStateException("Persist the service assignment before recording its mapping");
        Map<String, Object> payload;
        try {
            payload = mapper.readValue(config, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid persisted service configuration", e);
        }
        // Unsupported security fields deliberately remain NULL. Do not infer credentials from host inventory.
        int inserted = jdbc.update("""
                INSERT INTO kf_add_service
                    (add_service_id, cluster_id, created_time, status, host_id, broker_ip,
                     "user", created_by, updated_by, process_role)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (add_service_id) DO NOTHING
                """, id, clusterId, OffsetDateTime.now(ZoneOffset.UTC), status, hostId,
                text(payload.get("bootstrap_servers")), text(payload.get("service_user")), actor, actor, role);
        if (inserted == 1) events.publishEvent(new AddServiceAuditListener.Event(id, "SERVICE_ADDED", status, actor));
    }

    /** Update new mappings only. Old assignments are never implicitly backfilled. */
    @Transactional
    public void updateStatus(UUID serviceId, String status, String actor) {
        int updated = jdbc.update("""
                UPDATE kf_add_service SET status = ?, updated_by = COALESCE(?, updated_by)
                WHERE add_service_id = ? AND status IS DISTINCT FROM ?
                """, status, actor, serviceId, status);
        if (updated == 1) events.publishEvent(new AddServiceAuditListener.Event(serviceId, "SERVICE_STATUS_CHANGED", status, actor));
    }

    private static String text(Object value) {
        return value == null || value.toString().isBlank() ? null : value.toString();
    }
}
