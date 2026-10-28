package io.translab.tantor.server.service;

import io.translab.tantor.server.audit.AuditService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.LinkedHashMap;
import java.util.UUID;

/** Reuse the global audit table; rolled-back mapping writes produce no audit event. */
@Component
@RequiredArgsConstructor
@Slf4j
public class AddServiceAuditListener {
    private final JdbcTemplate jdbc;
    private final AuditService audit;

    public record Event(UUID serviceId, String action, String status, String actor) {}

    @TransactionalEventListener
    public void record(Event event) {
        try {
            var rows = jdbc.queryForList("SELECT cluster_id, host_id, process_role FROM kf_add_service WHERE add_service_id = ?", event.serviceId());
            if (rows.isEmpty()) return;
            var row = rows.get(0);
            var details = new LinkedHashMap<String, Object>();
            details.put("addServiceId", event.serviceId().toString());
            details.put("hostId", row.get("host_id"));
            details.put("processRole", row.get("process_role"));
            details.put("serviceStatus", event.status());
            audit.recordAs(event.actor(), "MANAGEMENT_SERVER", null, "SERVICE_DEPLOYMENT", event.action(),
                    "DATA_SERVICE", event.serviceId().toString(), (UUID) row.get("cluster_id"), event.status(),
                    null, null, null, details);
        } catch (RuntimeException error) {
            // The deployment has committed. An audit-storage failure must not turn it into an API failure.
            log.error("Could not audit add-service {} action {}", event.serviceId(), event.action(), error);
        }
    }
}
