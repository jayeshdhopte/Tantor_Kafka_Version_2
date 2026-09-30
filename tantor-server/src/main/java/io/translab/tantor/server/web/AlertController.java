package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.Alert;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.HostParcel;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.dto.ConsumerGroupSummaryDto;
import io.translab.tantor.server.repository.AlertRepository;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.ConsumerLagCacheService;
import io.translab.tantor.server.service.HostStatusService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/ui/alerts")
@RequiredArgsConstructor
public class AlertController {

    private static final List<String> RUNTIME_ALERT_KEY_PREFIXES = List.of(
            "host-offline-",
            "host-disk-full-",
            "host-disk-warning-",
            "host-memory-high-",
            "cluster-failed-",
            "cluster-deleting-",
            "cluster-host-offline-",
            "cluster-disk-full-",
            "cluster-port-closed-",
            "external-failed-",
            "parcel-failed-",
            "consumer-lag-",
            "task-failed-");

    private final AlertRepository alertRepository;
    private final ClusterRepository clusterRepository;
    private final HostRepository hostRepository;
    private final TaskRepository taskRepository;
    private final HostStatusService hostStatusService;
    private final HostParcelRepository hostParcelRepository;
    private final ConsumerLagCacheService consumerLagCacheService;

    @GetMapping
    @Transactional
    public ResponseEntity<List<Map<String, Object>>> getActiveAlerts() {
        List<Cluster> clusters = clusterRepository.findByStatusNot("DELETED");
        List<Host> allHosts = hostRepository.findAll();
        List<Host> hosts = allHosts.stream()
                .filter(hostStatusService::isInfrastructureHost)
                .toList();
        List<Task> tasks = taskRepository.findAll();

        Map<String, Cluster> clusterById = clusters.stream()
                .collect(Collectors.toMap(cluster -> cluster.getId().toString(), cluster -> cluster, (a, b) -> a));
        Map<String, Host> hostById = hosts.stream()
                .collect(Collectors.toMap(Host::getId, host -> host, (a, b) -> a));

        List<Map<String, Object>> alerts = new ArrayList<>();

        hosts.forEach(host -> {
            String effectiveStatus = hostStatusService.effectiveStatus(host);
            if ("OFFLINE".equalsIgnoreCase(effectiveStatus)) {
                alerts.add(runtimeAlert(
                        "host-offline-" + host.getId(),
                        "CRITICAL",
                        "Host agent offline",
                        "No recent heartbeat from " + hostLabel(host) + ". Deployments and service control may not work until the agent reconnects.",
                        null,
                        null,
                        host.getId(),
                        hostIp(host),
                        host.getLastHeartbeat(),
                        null,
                        "host"
                ));
            }

            long diskPct = diskUsedPercent(host);
            if (diskPct >= 90) {
                alerts.add(runtimeAlert(
                        "host-disk-full-" + host.getId(),
                        "CRITICAL",
                        "Host storage full",
                        hostLabel(host) + " is at " + diskPct + "% disk usage. Kafka may fail to start with 'No space left on device'.",
                        null,
                        null,
                        host.getId(),
                        hostIp(host),
                        host.getLastHeartbeat(),
                        null,
                        "storage"
                ));
            } else if (diskPct >= 80) {
                alerts.add(runtimeAlert(
                        "host-disk-warning-" + host.getId(),
                        "WARNING",
                        "Host storage pressure",
                        hostLabel(host) + " is at " + diskPct + "% disk usage. Clean old artifacts/logs before Kafka reaches a hard failure.",
                        null,
                        null,
                        host.getId(),
                        hostIp(host),
                        host.getLastHeartbeat(),
                        null,
                        "storage"
                ));
            }

            long memoryPct = memoryUsedPercent(host);
            if (memoryPct >= 90) {
                alerts.add(runtimeAlert(
                        "host-memory-high-" + host.getId(),
                        memoryPct >= 95 ? "CRITICAL" : "WARNING",
                        "Host memory pressure",
                        hostLabel(host) + " is using " + memoryPct + "% of memory. Inspect Kafka heap and other processes.",
                        host.getClusterId(),
                        null,
                        host.getId(),
                        hostIp(host),
                        host.getLastHeartbeat(),
                        null,
                        "memory"
                ));
            }
        });

        clusters.forEach(cluster -> {
            List<Host> assignedHosts = assignedHosts(cluster, hostById);
            if ("FAILED".equalsIgnoreCase(cluster.getStatus())) {
                Task latest = latestTask(tasks, cluster.getId());
                alerts.add(runtimeAlert(
                        "cluster-failed-" + cluster.getId(),
                        "CRITICAL",
                        "Cluster failed",
                        cluster.getName() + " is marked failed. " + taskReason(latest, "Review the latest deployment or upgrade task."),
                        cluster.getId(),
                        cluster.getName(),
                        latest == null ? null : latest.getHostId(),
                        latest == null ? null : hostIp(hostById.get(latest.getHostId())),
                        latest == null ? OffsetDateTime.now() : latest.getUpdatedAt(),
                        logExcerpt(latest),
                        "cluster"
                ));
            } else if ("DELETING".equalsIgnoreCase(cluster.getStatus())) {
                alerts.add(runtimeAlert(
                        "cluster-deleting-" + cluster.getId(),
                        "WARNING",
                        "Cluster cleanup in progress",
                        cluster.getName() + " is still deleting. If it remains here, check the cleanup task logs.",
                        cluster.getId(),
                        cluster.getName(),
                        null,
                        null,
                        OffsetDateTime.now(),
                        null,
                        "cluster"
                ));
            }

            assignedHosts.stream()
                    .filter(host -> "OFFLINE".equalsIgnoreCase(hostStatusService.effectiveStatus(host)))
                    .findFirst()
                    .ifPresent(host -> alerts.add(runtimeAlert(
                            "cluster-host-offline-" + cluster.getId() + "-" + host.getId(),
                            "CRITICAL",
                            "Cluster host offline",
                            cluster.getName() + " is assigned to " + hostLabel(host) + ", but the host agent is offline.",
                            cluster.getId(),
                            cluster.getName(),
                            host.getId(),
                            hostIp(host),
                            host.getLastHeartbeat(),
                            null,
                            "cluster"
                    )));

            assignedHosts.stream()
                    .filter(host -> diskUsedPercent(host) >= 95)
                    .findFirst()
                    .ifPresent(host -> alerts.add(runtimeAlert(
                            "cluster-disk-full-" + cluster.getId() + "-" + host.getId(),
                            "CRITICAL",
                            "Cluster host storage full",
                            cluster.getName() + " is on " + hostLabel(host) + " where disk usage is " + diskUsedPercent(host) + "%. Kafka can fail with 'No space left on device'.",
                            cluster.getId(),
                            cluster.getName(),
                            host.getId(),
                            hostIp(host),
                            host.getLastHeartbeat(),
                            null,
                            "storage"
                    )));

            if ("EXTERNAL".equalsIgnoreCase(cluster.getMode()) && "FAILED".equalsIgnoreCase(cluster.getStatus())) {
                alerts.add(runtimeAlert(
                        "external-failed-" + cluster.getId(),
                        "CRITICAL",
                        "External cluster unreachable",
                        cluster.getName() + " is connected as external but bootstrap/discovery health is failed. Bootstrap: " + nullToDash(cluster.getBootstrapServers()),
                        cluster.getId(),
                        cluster.getName(),
                        null,
                        cluster.getBootstrapServers(),
                        OffsetDateTime.now(),
                        null,
                        "external"
                ));
            }
        });

        hostParcelRepository.findAll().stream()
                .filter(parcel -> "FAILED".equalsIgnoreCase(parcel.getStatus()))
                .limit(12)
                .forEach(parcel -> alerts.add(runtimeAlert(
                        "parcel-failed-" + parcel.getId(), "CRITICAL",
                        "Package validation or distribution failed", parcelReason(parcel),
                        null, null, parcel.getHostId(), hostIp(hostById.get(parcel.getHostId())),
                        parcel.getUpdatedAt(), parcel.getErrorMsg(), "package"
                )));

        clusters.forEach(cluster -> consumerLagCacheService.getSummaries(cluster.getId()).stream()
                .filter(group -> group.getTotalLag() >= 1000 || "WARNING".equalsIgnoreCase(group.getHealth()))
                .limit(10)
                .forEach(group -> alerts.add(runtimeAlert(
                        "consumer-lag-" + cluster.getId() + "-" + group.getGroupId(),
                        group.getTotalLag() >= 10000 ? "CRITICAL" : "WARNING",
                        "Consumer group needs attention",
                        consumerReason(group),
                        cluster.getId(),
                        cluster.getName(),
                        null,
                        null,
                        OffsetDateTime.now(),
                        null,
                        "consumer"
                ))));
        tasks.stream()
                // A port check is an operator-requested prerequisite result. It
                // belongs in the audit trail, not in the live-health alert feed.
                .filter(task -> "FAILED".equalsIgnoreCase(task.getStatus()))
                .filter(task -> !"CHECK_PORTS".equalsIgnoreCase(task.getCommand()))
                .sorted(Comparator.comparing(Task::getUpdatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .limit(12)
                .forEach(task -> {
                    Cluster cluster = task.getClusterId() == null ? null : clusterById.get(task.getClusterId().toString());
                    alerts.add(runtimeAlert(
                            "task-failed-" + task.getId(),
                            "CRITICAL",
                            prettyCommand(task.getCommand()) + " failed",
                            taskReason(task, "A task failed and needs review."),
                            task.getClusterId(),
                            cluster == null ? null : cluster.getName(),
                            task.getHostId(),
                            hostIp(hostById.get(task.getHostId())),
                            task.getUpdatedAt(),
                            logExcerpt(task),
                            "task"
                    ));
                });

        syncRuntimeAlerts(alerts, clusterById);

        Map<String, Cluster> historyClusterById = new LinkedHashMap<>(clusterById);
        clusterRepository.findAll().forEach(cluster ->
                historyClusterById.putIfAbsent(cluster.getId().toString(), cluster));
        Map<String, Host> historyHostById = allHosts.stream()
                .collect(Collectors.toMap(Host::getId, host -> host, (a, b) -> a));

        // Query after synchronization so an alert resolved during this request is
        // returned as RESOLVED history instead of disappearing from the UI.
        List<Map<String, Object>> alertHistory = new ArrayList<>();
        alertRepository.findTop100ByOrderByUpdatedAtDesc().stream()
                // Preserve port-check history in Audits, while keeping it out of
                // alerts even for rows created before this policy existed.
                .filter(alert -> !isPortCheckAlert(alert))
                // Older builds persisted one cluster-level "2 of 3 agents"
                // alert. Agent connectivity is now represented by one alert per
                // affected agent, so the aggregate row must not reappear in
                // either Current or Resolved alert views.
                .filter(alert -> !isLegacyAggregateAgentAlert(alert))
                .forEach(alert -> alertHistory.add(storedAlert(alert, historyClusterById, historyHostById)));

        List<Map<String, Object>> deduped = alertHistory.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        alert -> String.valueOf(alert.get("id")),
                        alert -> alert,
                        (first, ignored) -> first,
                        LinkedHashMap::new
                ))
                .values()
                .stream()
                .sorted((a, b) -> compareCreatedAt(b.get("updatedAt"), a.get("updatedAt")))
                .limit(50)
                .toList();
        return ResponseEntity.ok(deduped);
    }

    private Map<String, Object> storedAlert(
            Alert alert,
            Map<String, Cluster> clusterById,
            Map<String, Host> hostById) {
        Cluster cluster = alert.getClusterId() == null ? null : clusterById.get(alert.getClusterId().toString());
        Host host = alert.getHostId() == null ? null : hostById.get(alert.getHostId());
        String liveHostIp = hostIp(host);
        boolean snapshotChanged = false;
        if (!hasText(alert.getClusterNameSnapshot()) && cluster != null && hasText(cluster.getName())) {
            alert.setClusterNameSnapshot(cluster.getName());
            snapshotChanged = true;
        }
        if (!hasText(alert.getKafkaClusterIdSnapshot()) && cluster != null && hasText(cluster.getKafkaClusterId())) {
            alert.setKafkaClusterIdSnapshot(cluster.getKafkaClusterId());
            snapshotChanged = true;
        }
        if (!hasText(alert.getHostIpSnapshot())) {
            String snapshotIp = firstNonBlank(liveHostIp, alert.getAffectedIps());
            if (hasText(snapshotIp)) {
                alert.setHostIpSnapshot(snapshotIp);
                snapshotChanged = true;
            }
        }
        if (snapshotChanged) {
            alertRepository.save(alert);
        }
        Map<String, Object> response = runtimeAlert(
                alert.getAlertKey() == null ? alert.getId().toString() : alert.getAlertKey(),
                alert.getSeverity(),
                alert.getTitle(),
                alert.getDescription(),
                alert.getClusterId(),
                firstNonBlank(alert.getClusterNameSnapshot(), cluster == null ? null : cluster.getName()),
                alert.getHostId(),
                firstNonBlank(alert.getHostIpSnapshot(), liveHostIp, alert.getAffectedIps()),
                alert.getCreatedAt() == null ? null : alert.getCreatedAt().atOffset(OffsetDateTime.now().getOffset()),
                alert.getErrorLog(),
                "stored"
        );
        response.put("kafkaClusterId", firstNonBlank(
                alert.getKafkaClusterIdSnapshot(),
                cluster == null ? null : cluster.getKafkaClusterId()));
        response.put("status", alert.getStatus() == null ? "ACTIVE" : alert.getStatus());
        response.put("resolvedAt", alert.getResolvedAt());
        response.put("updatedAt", alert.getUpdatedAt());
        return response;
    }

    private boolean isPortCheckAlert(Alert alert) {
        String key = alert.getAlertKey() == null ? "" : alert.getAlertKey().toLowerCase();
        String title = alert.getTitle() == null ? "" : alert.getTitle().toLowerCase();
        String description = alert.getDescription() == null ? "" : alert.getDescription().toLowerCase();
        return key.startsWith("cluster-port-closed-")
                || title.contains("check ports")
                || title.contains("broker port closed")
                || description.contains("port check failed");
    }

    private boolean isLegacyAggregateAgentAlert(Alert alert) {
        String key = alert.getAlertKey() == null ? "" : alert.getAlertKey().toLowerCase();
        String title = alert.getTitle() == null ? "" : alert.getTitle().toLowerCase();
        return key.startsWith("external-agent-partial-")
                || title.equals("external agents partially connected");
    }

    private void syncRuntimeAlerts(
            List<Map<String, Object>> runtimeAlerts,
            Map<String, Cluster> clusterById) {
        java.util.Set<String> observedKeys = new java.util.HashSet<>();
        for (Map<String, Object> runtime : runtimeAlerts) {
            String key = String.valueOf(runtime.get("id"));
            if (key.isBlank() || "null".equals(key)) continue;
            observedKeys.add(key);
            Alert alert = alertRepository.findByAlertKey(key).orElseGet(Alert::new);
            boolean newlyActive = alert.getId() == null || !"ACTIVE".equalsIgnoreCase(alert.getStatus());
            if (newlyActive) {
                alert.setCreatedAt(java.time.Instant.now());
            }
            alert.setAlertKey(key);
            alert.setSeverity(String.valueOf(runtime.getOrDefault("severity", "WARNING")));
            alert.setTitle(String.valueOf(runtime.getOrDefault("title", "Runtime alert")));
            alert.setDescription(String.valueOf(runtime.getOrDefault("description", "")));
            Object clusterId = runtime.get("clusterId");
            UUID parsedClusterId = clusterId instanceof UUID uuid ? uuid
                    : clusterId == null ? null : parseUuid(String.valueOf(clusterId));
            alert.setClusterId(parsedClusterId);
            alert.setHostId(runtime.get("hostId") == null ? null : String.valueOf(runtime.get("hostId")));
            String clusterName = textValue(runtime.get("clusterName"));
            if (hasText(clusterName)) {
                alert.setClusterNameSnapshot(clusterName);
            }
            Cluster cluster = parsedClusterId == null ? null : clusterById.get(parsedClusterId.toString());
            if (cluster != null && hasText(cluster.getKafkaClusterId())) {
                alert.setKafkaClusterIdSnapshot(cluster.getKafkaClusterId());
            }
            String hostIp = textValue(runtime.get("hostIp"));
            if (hasText(hostIp)) {
                alert.setHostIpSnapshot(hostIp);
                alert.setAffectedIps(hostIp);
            }
            alert.setSource(String.valueOf(runtime.getOrDefault("source", "runtime")));
            alert.setErrorLog(runtime.get("errorLog") == null ? null : String.valueOf(runtime.get("errorLog")));
            alert.setStatus("ACTIVE");
            alert.setResolvedAt(null);
            alertRepository.save(alert);
        }

        alertRepository.findByStatusOrderByCreatedAtDesc("ACTIVE").stream()
                .filter(alert -> alert.getAlertKey() != null)
                .filter(alert -> isRuntimeManagedAlertKey(alert.getAlertKey()))
                .filter(alert -> !observedKeys.contains(alert.getAlertKey()))
                .forEach(alert -> {
                    alert.setStatus("RESOLVED");
                    alert.setResolvedAt(java.time.Instant.now());
                    alertRepository.save(alert);
                });
    }

    private boolean isRuntimeManagedAlertKey(String alertKey) {
        return alertKey != null
                && RUNTIME_ALERT_KEY_PREFIXES.stream().anyMatch(alertKey::startsWith);
    }

    private UUID parseUuid(String value) {
        try {
            return UUID.fromString(value);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String textValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (hasText(value)) {
                return value;
            }
        }
        return null;
    }

    private Map<String, Object> runtimeAlert(
            String id,
            String severity,
            String title,
            String description,
            UUID clusterId,
            String clusterName,
            String hostId,
            String hostIp,
            OffsetDateTime createdAt,
            String errorLog,
            String source
    ) {
        Map<String, Object> alert = new LinkedHashMap<>();
        alert.put("id", id);
        alert.put("severity", severity == null ? "WARNING" : severity);
        alert.put("title", title);
        alert.put("description", description);
        alert.put("clusterId", clusterId);
        alert.put("clusterName", clusterName);
        alert.put("hostId", hostId);
        alert.put("hostIp", hostIp);
        alert.put("status", "ACTIVE");
        alert.put("createdAt", createdAt == null ? OffsetDateTime.now() : createdAt);
        alert.put("errorLog", errorLog);
        alert.put("source", source);
        return alert;
    }

    private List<Host> assignedHosts(Cluster cluster, Map<String, Host> hostById) {
        if (cluster.getServices() == null) {
            return List.of();
        }
        return cluster.getServices().stream()
                .map(service -> hostById.get(service.getHostId()))
                .filter(Objects::nonNull)
                .toList();
    }

    private Task latestTask(List<Task> tasks, UUID clusterId) {
        if (clusterId == null) {
            return null;
        }
        return tasks.stream()
                .filter(task -> clusterId.equals(task.getClusterId()))
                .sorted(Comparator.comparing(Task::getUpdatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .findFirst()
                .orElse(null);
    }

    private String taskReason(Task task, String fallback) {
        if (task == null) {
            return fallback;
        }
        String error = task.getErrorMsg();
        if (error == null || error.isBlank()) {
            error = task.getLogOutput();
        }
        return error == null || error.isBlank() ? fallback : shortText(error, 220);
    }

    private String logExcerpt(Task task) {
        if (task == null) {
            return null;
        }
        String log = task.getErrorMsg();
        if (log == null || log.isBlank()) {
            log = task.getLogOutput();
        }
        return log == null || log.isBlank() ? null : shortText(log, 1200);
    }

    private String shortText(String value, int maxLength) {
        String compact = value == null ? "" : value.replaceAll("\\s+", " ").trim();
        return compact.length() <= maxLength ? compact : compact.substring(0, maxLength - 1) + "...";
    }

    private long diskUsedPercent(Host host) {
        if (host == null || host.getDiskTotalGb() == null || host.getDiskTotalGb() <= 0) {
            return 0;
        }
        long used = host.getDiskUsedGb() == null ? 0L : host.getDiskUsedGb();
        return Math.min(100, Math.round((used * 100.0) / host.getDiskTotalGb()));
    }

    private long memoryUsedPercent(Host host) {
        if (host == null || host.getMemTotalMb() == null || host.getMemTotalMb() <= 0) return 0;
        long used = host.getMemUsedMb() == null ? 0L : host.getMemUsedMb();
        return Math.min(100, Math.round((used * 100.0) / host.getMemTotalMb()));
    }

    private String parcelReason(HostParcel parcel) {
        return parcel.getServiceType() + " " + parcel.getVersion() + " failed on host " + parcel.getHostId() + ". " + shortText(parcel.getErrorMsg(), 220);
    }

    private String consumerReason(ConsumerGroupSummaryDto group) {
        return group.getGroupId() + " has total lag " + group.getTotalLag() + " and health " + group.getHealth() + ".";
    }

    private String hostLabel(Host host) {
        if (host == null) {
            return "unknown host";
        }
        return (host.getHostname() == null || host.getHostname().isBlank() ? host.getId() : host.getHostname())
                + " (" + host.getId() + ")";
    }

    private String hostIp(Host host) {
        if (host == null || host.getIpAddresses() == null || host.getIpAddresses().isBlank()) {
            return null;
        }
        String cleaned = host.getIpAddresses().replace("[", "").replace("]", "").replace("\"", "");
        for (String value : cleaned.split(",")) {
            String ip = value.trim();
            if (!ip.isBlank() && !"localhost".equalsIgnoreCase(ip) && !"127.0.0.1".equals(ip)) {
                return ip;
            }
        }
        return cleaned.split(",")[0].trim();
    }

    private String prettyCommand(String command) {
        if (command == null || command.isBlank()) {
            return "Task";
        }
        String[] words = command.toLowerCase().replace('_', ' ').split(" ");
        List<String> titled = new ArrayList<>();
        for (String word : words) {
            if (!word.isBlank()) {
                titled.add(word.substring(0, 1).toUpperCase() + word.substring(1));
            }
        }
        return String.join(" ", titled);
    }

    private int compareCreatedAt(Object left, Object right) {
        OffsetDateTime a = toOffsetDateTime(left);
        OffsetDateTime b = toOffsetDateTime(right);
        return Comparator.nullsLast(Comparator.<OffsetDateTime>naturalOrder()).compare(a, b);
    }

    private OffsetDateTime toOffsetDateTime(Object value) {
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime;
        }
        if (value == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(String.valueOf(value));
        } catch (Exception ignored) {
            return null;
        }
    }

    private String nullToDash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
