package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/** Additional restart summaries; existing job/step/task records remain authoritative. */
@Service
@RequiredArgsConstructor
@Slf4j
public class RollingRestartMappingService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final ExternalClusterRepository externalClusters;
    private final ApplicationEventPublisher events;
    private final AuditService audit;

    public record AuditEvent(UUID jobId, UUID clusterId, String actor, String action,
                             String status, Map<String, Object> details) {}

    @Transactional
    public void record(Job job, Cluster cluster, List<JobStep> steps) {
        if (job.getType() != JobType.ROLLING_RESTART) return;
        if (job.getId() == null) throw new IllegalStateException("Persist the restart job before its summary");
        var external = externalClusters.findById(cluster.getId()).orElse(null);
        String name = external == null ? cluster.getName() : external.getName();
        String mode = external == null ? cluster.getMode() : external.getKafkaMode();
        String environment = external == null ? cluster.getEnvironment() : external.getEnvironment();
        String bootstrap = external == null ? cluster.getBootstrapServers() : external.getBootstrapServers();
        String type = external != null || "EXTERNAL".equalsIgnoreCase(cluster.getOriginType()) ? "Existing" : "New";
        var hosts = steps.stream().map(JobStep::getTargetId).filter(Objects::nonNull).distinct().toList();
        var roles = new ArrayList<Map<String, Object>>();
        for (var step : steps) {
            Map<String, Object> payload = read(step.getPayload());
            var node = new LinkedHashMap<String, Object>();
            node.put("stepId", step.getId());
            node.put("hostId", step.getTargetId());
            for (String key : List.of("role", "nodeId", "serviceName", "hostname", "ipAddress")) {
                if (payload.containsKey(key)) node.put(key, payload.get(key));
            }
            roles.add(node);
        }
        int inserted = jdbc.update("""
                INSERT INTO kf_rolling_restart_table
                    (unique_id, cluster_id, cluster_name, status, type, mode, env, bootstrap_server_ip,
                     "user", updated_by, created_at, updated_at, role_per_node, host_id, action, resource_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)
                ON CONFLICT (unique_id) DO NOTHING
                """, job.getId(), cluster.getId(), name, job.getStatus().name(), type, mode, environment, bootstrap,
                job.getRequestedBy(), job.getRequestedBy(), timestamp(job.getCreatedAt()), timestamp(job.getUpdatedAt()),
                json(roles), json(hosts), job.getType().name(), "CLUSTER");
        if (inserted == 1) publish(job.getId(), cluster.getId(), job.getRequestedBy(), "ROLLING_RESTART_REQUESTED",
                job.getStatus().name(), name, hosts, roles);
    }

    /** Missing historical summaries are intentionally not created by status updates. */
    @Transactional
    public void synchronizeStatus(Job job, String actor, String action) {
        if (job.getType() != JobType.ROLLING_RESTART) return;
        var rows = jdbc.queryForList("""
                UPDATE kf_rolling_restart_table
                SET status = ?, updated_by = COALESCE(?, updated_by), updated_at = ?
                WHERE unique_id = ? AND status IS DISTINCT FROM ?
                RETURNING cluster_id, cluster_name, updated_by, host_id, role_per_node
                """, job.getStatus().name(), actor, OffsetDateTime.now(ZoneOffset.UTC), job.getId(), job.getStatus().name());
        if (rows.isEmpty()) return;
        var row = rows.get(0);
        publish(job.getId(), (UUID) row.get("cluster_id"), (String) row.get("updated_by"), action,
                job.getStatus().name(), (String) row.get("cluster_name"),
                readArray(row.get("host_id")), readArray(row.get("role_per_node")));
    }

    private void publish(UUID id, UUID cluster, String actor, String action, String status,
                         String name, Object hosts, Object roles) {
        var details = new LinkedHashMap<String, Object>();
        details.put("jobId", id.toString()); details.put("clusterName", name);
        details.put("restartStatus", status); details.put("hostIds", hosts); details.put("rolesPerNode", roles);
        events.publishEvent(new AuditEvent(id, cluster, actor, action, status, Collections.unmodifiableMap(details)));
    }

    @TransactionalEventListener
    public void recordAudit(AuditEvent event) {
        try {
            String outcome = Set.of("FAILED", "PARTIAL_SUCCESS", "ROLLBACK_FAILED").contains(event.status()) ? "FAILED" : "SUCCESS";
            audit.recordAs(event.actor(), "MANAGEMENT_SERVER", null, "RESTART", event.action(), "CLUSTER",
                    event.clusterId().toString(), event.clusterId(), outcome, null, null, null, event.details());
        } catch (RuntimeException error) {
            log.error("Could not audit rolling restart {} action {}", event.jobId(), event.action(), error);
        }
    }

    private Map<String, Object> read(String value) {
        try { return mapper.readValue(value, new TypeReference<Map<String, Object>>() {}); }
        catch (Exception error) { throw new IllegalArgumentException("Invalid persisted restart step payload", error); }
    }
    private Object readArray(Object value) {
        if (value == null) return null;
        try { return mapper.readValue(value.toString(), List.class); }
        catch (Exception error) { throw new IllegalArgumentException("Invalid saved restart target array", error); }
    }
    private String json(Object value) {
        try { return mapper.writeValueAsString(value); }
        catch (Exception error) { throw new IllegalArgumentException("Cannot serialize restart summary", error); }
    }
    private static OffsetDateTime timestamp(Instant value) { return value == null ? null : value.atOffset(ZoneOffset.UTC); }
}
