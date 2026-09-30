package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class RollingRestartService {

    private final ClusterRepository clusterRepository;
    private final DeploymentService deploymentService;
    private final KafkaAdminService kafkaAdminService;
    private final ExternalClusterService externalClusterService;
    private final TaskRepository taskRepository;
    private final RollingRestartHealthService healthService;

    @Value("${tantor.rolling-restart.health-timeout-seconds:300}")
    private long healthTimeoutSeconds;
    @Value("${tantor.rolling-restart.health-poll-seconds:10}")
    private long healthPollSeconds;
    @Value("${tantor.rolling-restart.stable-samples:2}")
    private int stableSamples;

    private final Map<String, String> restartTasks = new ConcurrentHashMap<>();
    private final Set<UUID> activeClusters = ConcurrentHashMap.newKeySet();

    @Async
    public void executeRollingRestart(UUID clusterId, String taskId) {
        restartTasks.put(taskId, "Starting rolling restart for cluster " + clusterId);
        if (!activeClusters.add(clusterId)) {
            restartTasks.put(taskId, "FAILED: A rolling restart is already running for this cluster");
            return;
        }

        try {
            Cluster cluster = clusterRepository.findWithServicesById(clusterId).orElse(null);
            if (cluster == null) {
                restartTasks.put(taskId, "FAILED: Cluster not found");
                return;
            }

            if ("EXTERNAL".equalsIgnoreCase(cluster.getMode())) {
                restartTasks.put(taskId, "Dispatching restart command to external discovery agent");
                Map<String, Object> externalTask = externalClusterService.queueRestart(clusterId);
                waitForExternalTask(String.valueOf(externalTask.get("taskId")));
                waitForStableHealth(clusterId, 1, Set.of(), taskId, "External cluster: ");
                restartTasks.put(taskId, "COMPLETED successfully.");
                return;
            }

            List<ClusterServiceAssignment> services = cluster.getServices() == null
                    ? List.of()
                    : new ArrayList<>(cluster.getServices());
            List<ClusterServiceAssignment> brokers = services.stream().filter(this::isBrokerService).toList();
            List<ClusterServiceAssignment> metadataServices = services.stream()
                    .filter(service -> "zookeeper".equalsIgnoreCase(cluster.getMode())
                            ? "zookeeper".equals(service.getRole())
                            : isControllerService(service))
                    .toList();

            if (brokers.size() < 2) {
                restartTasks.put(taskId, "FAILED: A zero-downtime rolling restart requires at least two brokers");
                return;
            }
            if (metadataServices.size() < 3) {
                restartTasks.put(taskId, "FAILED: A zero-downtime rolling restart requires at least three controller or ZooKeeper quorum nodes");
                return;
            }

            Set<String> requiredHostIds = services.stream()
                    .map(ClusterServiceAssignment::getHostId)
                    .collect(java.util.stream.Collectors.toSet());
            RollingRestartHealthService.HealthSnapshot precheck =
                    healthService.inspect(clusterId, brokers.size(), requiredHostIds);
            if (!precheck.healthy()) {
                restartTasks.put(taskId, "PAUSED: Pre-restart health gate failed: " + precheck.failureReason());
                return;
            }
            restartTasks.put(taskId, healthSummary("Pre-restart health gate passed", precheck));
            Integer activeControllerId = "kraft".equalsIgnoreCase(cluster.getMode())
                    ? kafkaAdminService.getControllerId(clusterId)
                    : null;
            services.sort(Comparator
                    .comparingInt((ClusterServiceAssignment service) -> restartPhase(service, cluster.getMode()))
                    .thenComparingInt(service -> activeControllerId != null && activeControllerId.equals(service.getNodeId()) ? 1 : 0)
                    .thenComparing(service -> service.getNodeId() == null ? Integer.MAX_VALUE : service.getNodeId()));

            for (int i = 0; i < services.size(); i++) {
                ClusterServiceAssignment service = services.get(i);
                String prefix = String.format("Service %d/%d (%s, node %s on %s): ",
                        i + 1, services.size(), service.getRole(), service.getNodeId(), service.getHostId());
                restartTasks.put(taskId, prefix + "queued");
                UUID agentTaskId = deploymentService.restartService(
                        clusterId,
                        service.getHostId(),
                        systemdServiceName(service.getRole())
                );
                waitForAgentTask(agentTaskId, prefix);
                waitForStableHealth(clusterId, brokers.size(), requiredHostIds, taskId, prefix);
                restartTasks.put(taskId, prefix + "healthy; continuing");
            }
            restartTasks.put(taskId, "COMPLETED successfully.");
        } catch (Exception e) {
            log.error("Rolling restart failed", e);
            restartTasks.put(taskId, "PAUSED: " + safeMessage(e));
        } finally {
            activeClusters.remove(clusterId);
        }
    }

    private void waitForStableHealth(UUID clusterId, int expectedBrokerCount, Set<String> requiredHostIds,
            String taskId, String prefix) throws InterruptedException {
        long deadline = System.currentTimeMillis() + healthTimeoutSeconds * 1000;
        int consecutiveHealthySamples = 0;
        int requiredStableSamples = Math.max(1, stableSamples);
        RollingRestartHealthService.HealthSnapshot last = null;
        while (System.currentTimeMillis() < deadline) {
            last = healthService.inspect(clusterId, expectedBrokerCount, requiredHostIds);
            if (last.healthy()) {
                consecutiveHealthySamples++;
                restartTasks.put(taskId, prefix + "health stable " + consecutiveHealthySamples + "/" + requiredStableSamples);
                if (consecutiveHealthySamples >= requiredStableSamples) return;
            } else {
                consecutiveHealthySamples = 0;
                restartTasks.put(taskId, prefix + "waiting for health: " + last.failureReason());
            }
            Thread.sleep(Math.max(1, healthPollSeconds) * 1000);
        }
        throw new IllegalStateException(prefix + "health gate timed out: "
                + (last == null ? "no health sample available" : last.failureReason()));
    }

    private String healthSummary(String label, RollingRestartHealthService.HealthSnapshot health) {
        return label + ": brokers=" + health.visibleBrokers() + ", controller=" + health.controllerId()
                + ", offline=0, URP=0, minISR=0, consumerLag=" + health.consumerLag();
    }

    private String safeMessage(Exception exception) {
        return exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
    }

    private void waitForAgentTask(UUID agentTaskId, String prefix) throws InterruptedException {
        for (int i = 0; i < 120; i++) {
            io.translab.tantor.server.domain.Task task = taskRepository.findById(agentTaskId).orElse(null);
            if (task != null && "SUCCESS".equalsIgnoreCase(task.getStatus())) return;
            if (task != null && "FAILED".equalsIgnoreCase(task.getStatus())) {
                throw new RuntimeException(prefix + (task.getErrorMsg() == null ? "agent restart failed" : task.getErrorMsg()));
            }
            Thread.sleep(2000);
        }
        throw new RuntimeException(prefix + "timed out waiting for the agent restart task");
    }

    private void waitForExternalTask(String externalTaskId) throws InterruptedException {
        for (int i = 0; i < 60; i++) {
            Map<String, Object> statusMap = externalClusterService.getExternalTaskStatus(externalTaskId);
            String status = String.valueOf(statusMap.get("status"));
            if ("SUCCESS".equals(status)) return;
            if ("FAILED".equals(status)) throw new RuntimeException(String.valueOf(statusMap.get("message")));
            Thread.sleep(2000);
        }
        throw new RuntimeException("Timeout waiting for external agent task to finish");
    }

    private int restartPhase(ClusterServiceAssignment service, String mode) {
        if ("zookeeper".equalsIgnoreCase(mode)) return "zookeeper".equals(service.getRole()) ? 0 : 1;
        if ("controller".equals(service.getRole())) return 0;
        if ("broker_controller".equals(service.getRole())) return 1;
        return 2;
    }

    private boolean isBrokerService(ClusterServiceAssignment service) {
        return "broker".equals(service.getRole())
                || "broker_controller".equals(service.getRole())
                || "broker_zookeeper".equals(service.getRole());
    }

    private boolean isControllerService(ClusterServiceAssignment service) {
        return "controller".equals(service.getRole()) || "broker_controller".equals(service.getRole());
    }

    private String systemdServiceName(String role) {
        if ("controller".equals(role)) return "controller";
        if ("zookeeper".equals(role)) return "zookeeper";
        if ("broker_controller".equals(role) || "broker_zookeeper".equals(role)) return "kafka";
        return "broker";
    }

    public String getTaskStatus(String taskId) {
        return restartTasks.getOrDefault(taskId, "NOT_FOUND");
    }
}
