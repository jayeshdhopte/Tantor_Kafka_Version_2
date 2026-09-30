package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.ActivityLog;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.HostParcel;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.ActivityLogRepository;
import io.translab.tantor.server.repository.AlertRepository;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.HostStatusService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/ui/dashboard")
@RequiredArgsConstructor
public class DashboardController {

    private final ClusterRepository clusterRepository;
    private final HostRepository hostRepository;
    private final AlertRepository alertRepository;
    private final ActivityLogRepository activityLogRepository;
    private final TaskRepository taskRepository;
    private final HostParcelRepository hostParcelRepository;
    private final HostStatusService hostStatusService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> getDashboard() {
        List<Cluster> clusters = clusterRepository.findByStatusNot("DELETED");
        List<Host> hosts = hostRepository.findAll();
        List<Host> infrastructureHosts = hosts.stream().filter(hostStatusService::isInfrastructureHost).toList();
        List<Task> tasks = taskRepository.findAll();
        List<HostParcel> parcels = hostParcelRepository.findAll();
        List<ActivityLog> activities = activityLogRepository.findTop50ByOrderByCreatedAtDesc();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("generatedAt", OffsetDateTime.now());
        response.put("summary", summary(clusters, infrastructureHosts, tasks, parcels, activities));
        response.put("hostStatus", hostStatusChart(infrastructureHosts));
        response.put("clusterStatus", clusterStatusChart(clusters));
        response.put("clusterHealth", clusterHealth(clusters, infrastructureHosts, tasks));
        response.put("hostDiskUsage", hostDiskUsage(infrastructureHosts));
        response.put("taskStatus", taskStatusChart(tasks));
        response.put("taskTimeline", taskTimeline(tasks));
        response.put("runningServices", runningServices(clusters, infrastructureHosts, parcels));
        response.put("failedServices", failedServices(clusters, infrastructureHosts, tasks, parcels));
        response.put("recentActivities", recentActivities(activities));
        response.put("recentTasks", recentTasks(tasks, clusters));
        return ResponseEntity.ok(response);
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getOverviewStats() {
        Map<String, Object> stats = new HashMap<>();
        List<Cluster> clusters = clusterRepository.findByStatusNot("DELETED");
        List<Host> hosts = hostRepository.findAll().stream().filter(hostStatusService::isInfrastructureHost).toList();

        stats.put("totalClusters", clusters.size());
        stats.put("totalHosts", hosts.size());
        stats.put("activeAlerts", alertRepository.countByStatus("ACTIVE"));
        stats.put("healthyClusters", clusters.stream().filter(cluster -> "SUCCESS".equalsIgnoreCase(cluster.getStatus())).count());
        return ResponseEntity.ok(stats);
    }

    @GetMapping("/activity")
    public ResponseEntity<List<ActivityLog>> getRecentActivity() {
        return ResponseEntity.ok(activityLogRepository.findTop50ByOrderByCreatedAtDesc());
    }

    private Map<String, Object> summary(
            List<Cluster> clusters,
            List<Host> hosts,
            List<Task> tasks,
            List<HostParcel> parcels,
            List<ActivityLog> activities
    ) {
        Map<String, Long> hostCounts = hosts.stream()
                .collect(Collectors.groupingBy(host -> normalizeStatus(hostStatusService.effectiveStatus(host)), Collectors.counting()));
        long activeHosts = hosts.stream().filter(this::hasConnectedAgent).count();
        Map<String, Long> clusterCounts = clusters.stream()
                .collect(Collectors.groupingBy(cluster -> normalizeStatus(cluster.getStatus()), Collectors.counting()));
        Map<String, Long> taskCounts = tasks.stream()
                .collect(Collectors.groupingBy(task -> normalizeStatus(task.getStatus()), Collectors.counting()));

        long externalClusters = clusters.stream().filter(cluster -> "EXTERNAL".equalsIgnoreCase(cluster.getMode())).count();
        long internalClusters = clusters.size() - externalClusters;
        long activeParcels = parcels.stream().filter(HostParcel::isActive).count();
        long failedParcels = parcels.stream().filter(parcel -> "FAILED".equalsIgnoreCase(parcel.getStatus())).count();

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalHosts", hosts.size());
        summary.put("activeHosts", activeHosts);
        summary.put("offlineHosts", hostCounts.getOrDefault("OFFLINE", 0L));
        summary.put("pendingHosts", hostCounts.getOrDefault("PENDING", 0L));
        summary.put("totalClusters", clusters.size());
        summary.put("activeClusters", clusterCounts.getOrDefault("SUCCESS", 0L));
        summary.put("failedClusters", clusterCounts.getOrDefault("FAILED", 0L));
        summary.put("externalClusters", externalClusters);
        summary.put("internalClusters", internalClusters);
        summary.put("activeAlerts", alertRepository.countByStatus("ACTIVE"));
        summary.put("runningTasks", taskCounts.getOrDefault("PENDING", 0L) + taskCounts.getOrDefault("IN_PROGRESS", 0L));
        summary.put("failedTasks", taskCounts.getOrDefault("FAILED", 0L));
        summary.put("activeParcels", activeParcels);
        summary.put("failedParcels", failedParcels);
        summary.put("runningServices", runningServiceCount(clusters, hosts, parcels));
        summary.put("failedServices", failedServiceCount(clusters, hosts, tasks, parcels));
        summary.put("firstClusterCreatedAt", clusters.stream().map(Cluster::getCreatedAt).filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(null));
        summary.put("latestClusterCreatedAt", clusters.stream().map(Cluster::getCreatedAt).filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null));
        summary.put("lastActivityAt", activities.stream().map(ActivityLog::getCreatedAt).filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null));
        return summary;
    }

    private List<Map<String, Object>> hostStatusChart(List<Host> hosts) {
        Map<String, Long> counts = hosts.stream()
                .collect(Collectors.groupingBy(host -> normalizeStatus(hostStatusService.effectiveStatus(host)), LinkedHashMap::new, Collectors.counting()));
        return orderedStatusRows(counts, List.of("ONLINE", "OFFLINE", "PENDING"));
    }

    private List<Map<String, Object>> clusterStatusChart(List<Cluster> clusters) {
        Map<String, Long> counts = clusters.stream()
                .collect(Collectors.groupingBy(cluster -> normalizeStatus(cluster.getStatus()), LinkedHashMap::new, Collectors.counting()));
        return orderedStatusRows(counts, List.of("SUCCESS", "FAILED", "RUNNING", "DELETING", "PENDING"));
    }

    private List<Map<String, Object>> clusterHealth(List<Cluster> clusters, List<Host> hosts, List<Task> tasks) {
        Map<String, Host> hostById = hosts.stream()
                .collect(Collectors.toMap(Host::getId, host -> host, (a, b) -> a));
        Map<String, Task> latestTaskByCluster = tasks.stream()
                .filter(task -> task.getClusterId() != null)
                .sorted(Comparator.comparing(Task::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .collect(Collectors.toMap(task -> task.getClusterId().toString(), task -> task, (first, ignored) -> first, LinkedHashMap::new));

        return clusters.stream()
                .sorted(Comparator.comparing(Cluster::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .map(cluster -> clusterHealthRow(cluster, hostById, latestTaskByCluster.get(cluster.getId().toString())))
                .toList();
    }

    private Map<String, Object> clusterHealthRow(Cluster cluster, Map<String, Host> hostById, Task latestTask) {
        List<Host> assignedHosts = assignedInfrastructureHosts(cluster, hostById);
        String status = normalizeStatus(cluster.getStatus());
        String health = "HEALTHY";
        String reason = "Cluster record is active";

        if ("DELETING".equals(status)) {
            health = "DELETING";
            reason = "Cleanup is in progress";
        } else if ("FAILED".equals(status)) {
            health = "FAILED";
            reason = latestTask != null && latestTask.getErrorMsg() != null && !latestTask.getErrorMsg().isBlank()
                    ? shortText(latestTask.getErrorMsg(), 140)
                    : "Cluster lifecycle task failed";
        } else if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
            if ("SUCCESS".equals(status)) {
                health = "HEALTHY";
                reason = "External cluster is connected";
            } else {
                health = "FAILED";
                reason = "External bootstrap or discovery health is not successful";
            }
        } else if (assignedHosts.stream().anyMatch(host -> "OFFLINE".equalsIgnoreCase(hostStatusService.effectiveStatus(host)))) {
            health = "FAILED";
            reason = "One or more assigned host agents are offline";
        } else if (assignedHosts.stream().anyMatch(host -> diskUsedPercent(host) >= 95)) {
            health = "FAILED";
            reason = "Assigned host storage is full";
        } else if (assignedHosts.stream().anyMatch(host -> diskUsedPercent(host) >= 85)) {
            health = "WARNING";
            reason = "Assigned host storage is near capacity";
        } else if ("SUCCESS".equals(status) && !assignedHosts.isEmpty() && !clusterBrokerPortListening(cluster, assignedHosts)) {
            health = "FAILED";
            reason = "Broker port " + brokerPort(cluster) + " is not reachable from the management server";
        } else if ("SUCCESS".equals(status)) {
            health = "HEALTHY";
            reason = "Deployment succeeded and assigned agents are online";
        } else {
            health = status;
            reason = "Cluster status is " + title(status);
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", cluster.getId());
        row.put("name", cluster.getName());
        row.put("mode", cluster.getMode());
        row.put("kafkaVersion", cluster.getKafkaVersion());
        row.put("source", "EXTERNAL".equalsIgnoreCase(cluster.getMode()) ? "External" : "Internal");
        row.put("status", health);
        row.put("reason", reason);
        row.put("createdAt", cluster.getCreatedAt());
        row.put("bootstrapServers", cluster.getBootstrapServers());
        row.put("hostCount", assignedHosts.isEmpty() && cluster.getServices() != null ? cluster.getServices().size() : assignedHosts.size());
        row.put("latestTaskStatus", latestTask == null ? null : latestTask.getStatus());
        row.put("latestTaskCommand", latestTask == null ? null : latestTask.getCommand());
        return row;
    }

    private List<Host> assignedInfrastructureHosts(Cluster cluster, Map<String, Host> hostById) {
        if (cluster.getServices() == null) {
            return List.of();
        }
        return cluster.getServices().stream()
                .map(service -> hostById.get(service.getHostId()))
                .filter(Objects::nonNull)
                .filter(hostStatusService::isInfrastructureHost)
                .toList();
    }

    private List<Map<String, Object>> taskStatusChart(List<Task> tasks) {
        Map<String, Long> counts = tasks.stream()
                .collect(Collectors.groupingBy(task -> normalizeStatus(task.getStatus()), LinkedHashMap::new, Collectors.counting()));
        return orderedStatusRows(counts, List.of("SUCCESS", "FAILED", "PENDING", "IN_PROGRESS"));
    }

    private List<Map<String, Object>> orderedStatusRows(Map<String, Long> counts, List<String> order) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String status : order) {
            long count = counts.getOrDefault(status, 0L);
            if (count > 0 || rows.isEmpty()) {
                rows.add(statusRow(status, count));
            }
        }
        counts.entrySet().stream()
                .filter(entry -> !order.contains(entry.getKey()))
                .forEach(entry -> rows.add(statusRow(entry.getKey(), entry.getValue())));
        return rows;
    }

    private Map<String, Object> statusRow(String status, long count) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", title(status));
        row.put("status", status);
        row.put("value", count);
        return row;
    }

    private List<Map<String, Object>> hostDiskUsage(List<Host> hosts) {
        return hosts.stream()
                .filter(host -> host.getDiskTotalGb() != null && host.getDiskTotalGb() > 0)
                .sorted(Comparator.comparingLong(this::diskUsedPercent).reversed())
                .limit(8)
                .map(host -> {
                    long total = host.getDiskTotalGb();
                    long used = host.getDiskUsedGb() == null ? 0L : host.getDiskUsedGb();
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("name", host.getHostname() == null || host.getHostname().isBlank() ? host.getId() : host.getHostname());
                    row.put("hostId", host.getId());
                    row.put("status", hostStatusService.agentConnectivityStatus(host));
                    row.put("usedGb", used);
                    row.put("freeGb", Math.max(total - used, 0));
                    row.put("totalGb", total);
                    row.put("usedPct", diskUsedPercent(host));
                    row.put("lastHeartbeat", host.getLastHeartbeat());
                    return row;
                })
                .toList();
    }

    private long diskUsedPercent(Host host) {
        if (host.getDiskTotalGb() == null || host.getDiskTotalGb() <= 0) {
            return 0;
        }
        long used = host.getDiskUsedGb() == null ? 0L : host.getDiskUsedGb();
        return Math.min(100, Math.round((used * 100.0) / host.getDiskTotalGb()));
    }

    private boolean clusterBrokerPortListening(Cluster cluster, List<Host> assignedHosts) {
        int port = brokerPort(cluster);
        for (Host host : assignedHosts) {
            String ip = hostIp(host);
            if (ip == null || ip.isBlank()) {
                continue;
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(ip, port), 600);
                return true;
            } catch (Exception ignored) {
                // Try the next assigned host.
            }
        }
        return false;
    }

    private int brokerPort(Cluster cluster) {
        Integer bootstrapPort = firstBootstrapPort(cluster.getBootstrapServers());
        if (bootstrapPort != null) {
            return bootstrapPort;
        }
        Integer configPort = configInt(cluster.getConfigJson(), "listener_port");
        return configPort == null ? 9092 : configPort;
    }

    private Integer firstBootstrapPort(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            return null;
        }
        String endpoint = bootstrapServers.split(",")[0].trim();
        int colon = endpoint.lastIndexOf(':');
        if (colon < 0 || colon == endpoint.length() - 1) {
            return null;
        }
        try {
            return Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private Integer configInt(String configJson, String key) {
        if (configJson == null || configJson.isBlank()) {
            return null;
        }
        String quotedKey = "\"" + key + "\"";
        int keyIndex = configJson.indexOf(quotedKey);
        if (keyIndex < 0) {
            return null;
        }
        int colon = configJson.indexOf(':', keyIndex + quotedKey.length());
        if (colon < 0) {
            return null;
        }
        int start = colon + 1;
        while (start < configJson.length() && !Character.isDigit(configJson.charAt(start))) {
            start++;
        }
        int end = start;
        while (end < configJson.length() && Character.isDigit(configJson.charAt(end))) {
            end++;
        }
        if (start == end) {
            return null;
        }
        try {
            return Integer.parseInt(configJson.substring(start, end));
        } catch (NumberFormatException ignored) {
            return null;
        }
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

    private List<Map<String, Object>> taskTimeline(List<Task> tasks) {
        ZoneId zone = ZoneId.systemDefault();
        LocalDate start = LocalDate.now(zone).minusDays(6);
        Map<LocalDate, List<Task>> byDay = tasks.stream()
                .filter(task -> task.getCreatedAt() != null)
                .collect(Collectors.groupingBy(task -> task.getCreatedAt().toLocalDate()));

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            LocalDate day = start.plusDays(i);
            List<Task> dayTasks = byDay.getOrDefault(day, List.of());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("date", day.toString());
            row.put("label", day.getMonth().toString().substring(0, 3) + " " + day.getDayOfMonth());
            row.put("success", countTasks(dayTasks, "SUCCESS"));
            row.put("failed", countTasks(dayTasks, "FAILED"));
            row.put("running", countTasks(dayTasks, "PENDING") + countTasks(dayTasks, "IN_PROGRESS"));
            rows.add(row);
        }
        return rows;
    }

    private long countTasks(List<Task> tasks, String status) {
        return tasks.stream().filter(task -> status.equalsIgnoreCase(task.getStatus())).count();
    }

    private List<Map<String, Object>> runningServices(List<Cluster> clusters, List<Host> hosts, List<HostParcel> parcels) {
        List<Map<String, Object>> services = new ArrayList<>();
        long onlineHosts = hosts.stream().filter(this::hasConnectedAgent).count();
        long activeClusters = clusters.stream().filter(cluster -> "SUCCESS".equalsIgnoreCase(cluster.getStatus())).count();
        long activeExternal = clusters.stream()
                .filter(cluster -> "EXTERNAL".equalsIgnoreCase(cluster.getMode()))
                .filter(cluster -> "SUCCESS".equalsIgnoreCase(cluster.getStatus()))
                .count();
        long activeParcels = parcels.stream().filter(HostParcel::isActive).count();

        services.add(serviceRow("Tantor Backend API", "Management API is serving this dashboard", "SUCCESS", "platform"));
        services.add(serviceRow("Agent fleet", onlineHosts + " active host" + plural(onlineHosts), onlineHosts > 0 ? "SUCCESS" : "IDLE", "agent"));
        services.add(serviceRow("Kafka clusters", activeClusters + " active cluster" + plural(activeClusters), activeClusters > 0 ? "SUCCESS" : "IDLE", "kafka"));
        services.add(serviceRow("External cluster control", activeExternal + " connected external cluster" + plural(activeExternal), activeExternal > 0 ? "SUCCESS" : "IDLE", "external"));
        services.add(serviceRow("Active parcels", activeParcels + " active parcel" + plural(activeParcels), activeParcels > 0 ? "SUCCESS" : "IDLE", "parcel"));
        return services;
    }

    private List<Map<String, Object>> failedServices(List<Cluster> clusters, List<Host> hosts, List<Task> tasks, List<HostParcel> parcels) {
        List<Map<String, Object>> services = new ArrayList<>();
        long offlineHosts = hosts.stream().filter(host -> "OFFLINE".equalsIgnoreCase(hostStatusService.effectiveStatus(host))).count();
        long failedClusters = clusters.stream().filter(cluster -> "FAILED".equalsIgnoreCase(cluster.getStatus())).count();
        long deletingClusters = clusters.stream().filter(cluster -> "DELETING".equalsIgnoreCase(cluster.getStatus())).count();
        long failedTasks = tasks.stream().filter(task -> "FAILED".equalsIgnoreCase(task.getStatus())).count();
        long failedParcels = parcels.stream().filter(parcel -> "FAILED".equalsIgnoreCase(parcel.getStatus())).count();
        long fullDiskHosts = hosts.stream().filter(host -> diskUsedPercent(host) >= 95).count();
        long warningDiskHosts = hosts.stream().filter(host -> diskUsedPercent(host) >= 85 && diskUsedPercent(host) < 95).count();

        if (offlineHosts > 0) services.add(serviceRow("Offline agents", offlineHosts + " host" + plural(offlineHosts) + " missing heartbeat", "FAILED", "agent"));
        if (fullDiskHosts > 0) services.add(serviceRow("Storage full", fullDiskHosts + " host" + plural(fullDiskHosts) + " at or above 95% disk usage", "FAILED", "storage"));
        if (warningDiskHosts > 0) services.add(serviceRow("Storage pressure", warningDiskHosts + " host" + plural(warningDiskHosts) + " above 85% disk usage", "WARNING", "storage"));
        if (failedClusters > 0) services.add(serviceRow("Failed clusters", failedClusters + " cluster" + plural(failedClusters) + " failed", "FAILED", "kafka"));
        if (deletingClusters > 0) services.add(serviceRow("Cleanup in progress", deletingClusters + " cluster cleanup task" + plural(deletingClusters), "RUNNING", "cleanup"));
        if (failedTasks > 0) services.add(serviceRow("Failed tasks", failedTasks + " task" + plural(failedTasks) + " need review", "FAILED", "task"));
        if (failedParcels > 0) services.add(serviceRow("Failed parcel actions", failedParcels + " parcel action" + plural(failedParcels), "FAILED", "parcel"));
        return services;
    }

    private long runningServiceCount(List<Cluster> clusters, List<Host> hosts, List<HostParcel> parcels) {
        long onlineHosts = hosts.stream().filter(this::hasConnectedAgent).count();
        long activeClusters = clusters.stream().filter(cluster -> "SUCCESS".equalsIgnoreCase(cluster.getStatus())).count();
        long activeParcels = parcels.stream().filter(HostParcel::isActive).count();
        return 1 + onlineHosts + activeClusters + activeParcels;
    }

    private long failedServiceCount(List<Cluster> clusters, List<Host> hosts, List<Task> tasks, List<HostParcel> parcels) {
        long offlineHosts = hosts.stream().filter(host -> "OFFLINE".equalsIgnoreCase(hostStatusService.effectiveStatus(host))).count();
        long failedClusters = clusters.stream().filter(cluster -> "FAILED".equalsIgnoreCase(cluster.getStatus())).count();
        long failedTasks = tasks.stream().filter(task -> "FAILED".equalsIgnoreCase(task.getStatus())).count();
        long failedParcels = parcels.stream().filter(parcel -> "FAILED".equalsIgnoreCase(parcel.getStatus())).count();
        long diskIssues = hosts.stream().filter(host -> diskUsedPercent(host) >= 85).count();
        return offlineHosts + failedClusters + failedTasks + failedParcels + diskIssues;
    }

    private boolean hasConnectedAgent(Host host) {
        if (host == null || Boolean.TRUE.equals(host.getRemoved())
                || "PENDING".equalsIgnoreCase(host.getStatus())) {
            return false;
        }
        return "ONLINE".equalsIgnoreCase(hostStatusService.agentConnectivityStatus(host));
    }

    private Map<String, Object> serviceRow(String name, String description, String status, String type) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", name);
        row.put("description", description);
        row.put("status", status);
        row.put("type", type);
        return row;
    }

    private List<Map<String, Object>> recentActivities(List<ActivityLog> activities) {
        Map<String, ActivityLog> unique = new LinkedHashMap<>();
        for (ActivityLog activity : activities) {
            String key = normalizeStatus(activity.getLevel()) + "|" + activity.getMessage() + "|" + activity.getClusterId();
            unique.putIfAbsent(key, activity);
            if (unique.size() >= 8) {
                break;
            }
        }
        return unique.values().stream().map(activity -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", activity.getId());
            row.put("level", activity.getLevel());
            row.put("message", activity.getMessage());
            row.put("clusterId", activity.getClusterId());
            row.put("createdAt", activity.getCreatedAt());
            return row;
        }).toList();
    }

    private List<Map<String, Object>> recentTasks(List<Task> tasks, List<Cluster> clusters) {
        Map<String, String> clusterNames = clusters.stream()
                .collect(Collectors.toMap(cluster -> cluster.getId().toString(), Cluster::getName, (a, b) -> a));
        return tasks.stream()
                .sorted(Comparator.comparing(Task::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
                .limit(8)
                .map(task -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", task.getId());
                    row.put("command", task.getCommand());
                    row.put("status", task.getStatus());
                    row.put("hostId", task.getHostId());
                    row.put("clusterId", task.getClusterId());
                    row.put("clusterName", task.getClusterId() == null ? "" : clusterNames.getOrDefault(task.getClusterId().toString(), ""));
                    row.put("createdAt", task.getCreatedAt());
                    row.put("updatedAt", task.getUpdatedAt());
                    row.put("errorMsg", task.getErrorMsg());
                    return row;
                }).toList();
    }

    private String normalizeStatus(String status) {
        return status == null || status.isBlank() ? "UNKNOWN" : status.trim().toUpperCase(Locale.ROOT);
    }

    private String title(String value) {
        String normalized = value == null ? "Unknown" : value.toLowerCase(Locale.ROOT).replace('_', ' ');
        String[] words = normalized.split(" ");
        List<String> titled = new ArrayList<>();
        for (String word : words) {
            if (word.isBlank()) continue;
            titled.add(word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1));
        }
        return titled.isEmpty() ? "Unknown" : String.join(" ", titled);
    }

    private String plural(long count) {
        return count == 1 ? "" : "s";
    }

    private String shortText(String value, int maxLength) {
        if (value == null) {
            return "";
        }
        String compact = value.replaceAll("\\s+", " ").trim();
        return compact.length() <= maxLength ? compact : compact.substring(0, maxLength - 1) + "...";
    }
}
