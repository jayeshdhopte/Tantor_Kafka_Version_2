package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.JobRepository;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/** Plans role-scoped Kafka changes; static changes use one verified restart per node. */
@Service
@RequiredArgsConstructor
public class TargetedConfigChangeService {
    private static final Set<String> PROTECTED_KEYS = Set.of(
            "node.id", "broker.id", "process.roles", "log.dirs", "metadata.log.dir",
            "listeners", "advertised.listeners", "controller.quorum.voters",
            "controller.quorum.bootstrap.servers", "inter.broker.listener.name",
            "cluster.id", "zookeeper.connect");
    private static final List<JobStatus> ACTIVE_STATUSES = List.of(
            JobStatus.PENDING, JobStatus.IN_PROGRESS, JobStatus.ROLLBACK_PENDING, JobStatus.ROLLING_BACK);

    private final ClusterRepository clusters;
    private final ExternalClusterRepository externalClusters;
    private final JobRepository jobs;
    private final JobService jobService;
    private final KafkaAdminService kafka;
    private final RollingRestartHealthService health;
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final DeploymentService deployment;
    private final AgentTaskAwaiter taskAwaiter;
    private final PlatformTransactionManager transactions;

    @Data
    public static class ChangeRequest {
        private String role;
        private String scope;
        private List<Integer> targetNodeIds;
        private String key;
        private String value;
        private Map<String, String> changes;
    }

    public Map<Integer, Map<String, String>> readStaticProperties(UUID clusterId, String role,
                                                                    List<Integer> targetNodeIds) {
        if (!"BROKER".equalsIgnoreCase(role) && !"CONTROLLER".equalsIgnoreCase(role)) {
            throw new IllegalArgumentException("Select brokers or controllers.");
        }
        if (targetNodeIds == null || targetNodeIds.isEmpty()) {
            throw new IllegalArgumentException("Select at least one node.");
        }
        Cluster cluster = clusters.findWithServicesById(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("Managed cluster not found."));
        Map<Integer, UUID> tasks = new LinkedHashMap<>();
        for (Integer nodeId : new LinkedHashSet<>(targetNodeIds)) {
            if (nodeId == null) throw new IllegalArgumentException("Invalid node ID.");
            ClusterServiceAssignment service = findService(cluster, role, nodeId);
            Map<String, Object> config = mergedConfig(cluster, service);
            config.put("mode", cluster.getMode());
            config.put("version", cluster.getKafkaVersion());
            tasks.put(nodeId, deployment.readKafkaProperties(clusterId, service.getHostId(), service.getRole(),
                    String.valueOf(nodeId), json(config), configPath(cluster, service, config)));
        }
        Map<Integer, Map<String, String>> values = new LinkedHashMap<>();
        for (Map.Entry<Integer, UUID> entry : tasks.entrySet()) {
            Task result = taskAwaiter.await(entry.getValue(), 90);
            Object raw = readMap(result.getLogOutput()).get("properties");
            if (!(raw instanceof Map<?, ?> properties)) {
                throw new IllegalStateException("Agent did not return Kafka properties for node " + entry.getKey());
            }
            Map<String, String> nodeValues = new TreeMap<>();
            properties.forEach((key, value) -> {
                if (key instanceof String name && value instanceof String text) nodeValues.put(name, text);
            });
            values.put(entry.getKey(), nodeValues);
        }
        return values;
    }

    public Map<String, Object> preview(UUID clusterId, ChangeRequest request) {
        if (request == null || request.getChanges() == null) return previewSingle(clusterId, request, false, null);
        if (request.getKey() != null || request.getValue() != null) throw new IllegalArgumentException("Use either changes or a single key/value.");
        if (request.getChanges().isEmpty() || request.getChanges().size() > 100) throw new IllegalArgumentException("Select between 1 and 100 properties.");
        Set<String> normalized = new HashSet<>();
        for (String key : request.getChanges().keySet()) {
            if (key == null || !normalized.add(key.trim())) throw new IllegalArgumentException("Duplicate or missing property name.");
        }
        Cluster internal = clusters.findWithServicesById(clusterId).orElse(null);
        Map<Integer, Map<String, Object>> live = kafka.getBrokerConfigs(clusterId);
        PreviewContext context = new PreviewContext(internal, live, new HashMap<>(), health.inspect(clusterId, live.size(), Set.of()));
        List<Map<String, Object>> properties = new ArrayList<>();
        Map<Integer, Map<String, Object>> nodes = new LinkedHashMap<>();
        Map<String, Object> result = null;
        String classification = null;
        for (var entry : request.getChanges().entrySet()) {
            ChangeRequest single = new ChangeRequest();
            single.setRole(request.getRole()); single.setScope(request.getScope()); single.setTargetNodeIds(request.getTargetNodeIds());
            single.setKey(entry.getKey()); single.setValue(entry.getValue());
            Map<String, Object> plan = previewSingle(clusterId, single, true, context);
            String policy = String.valueOf(plan.get("classification"));
            if (classification != null && !classification.equals(policy)) throw new IllegalArgumentException("Review dynamic and restart-required properties separately.");
            classification = policy;
            if (result == null) result = new LinkedHashMap<>(plan);
            result.put("healthy", Boolean.TRUE.equals(result.get("healthy")) && Boolean.TRUE.equals(plan.get("healthy")));
            if (!Boolean.TRUE.equals(plan.get("healthy"))) result.put("healthFailures", plan.get("healthFailures"));
            result.put("clusterLocked", Boolean.TRUE.equals(result.get("clusterLocked")) || Boolean.TRUE.equals(plan.get("clusterLocked")));
            properties.add(plan);
            for (Object raw : (List<?>) plan.get("targets")) {
                Map<?, ?> target = (Map<?, ?>) raw;
                int nodeId = ((Number) target.get("nodeId")).intValue();
                Map<String, Object> node = nodes.computeIfAbsent(nodeId, ignored -> {
                    Map<String, Object> next = new LinkedHashMap<>(); next.put("nodeId", nodeId);
                    next.put("changes", new LinkedHashMap<String, Object>()); next.put("expectedValues", new LinkedHashMap<String, Object>()); return next;
                });
                @SuppressWarnings("unchecked") Map<String, Object> changes = (Map<String, Object>) node.get("changes");
                @SuppressWarnings("unchecked") Map<String, Object> expected = (Map<String, Object>) node.get("expectedValues");
                String key = String.valueOf(plan.get("key"));
                if (changes.containsKey(key)) throw new IllegalArgumentException("Duplicate normalized property: " + key);
                changes.put(key, target.get("newValue")); expected.put(key, target.get("oldValue"));
            }
        }
        if (nodes.isEmpty()) throw new IllegalArgumentException("The selected nodes already have the requested values.");
        List<Map<String, Object>> targets = new ArrayList<>(nodes.values());
        if ("CONTROLLER".equalsIgnoreCase(request.getRole())) {
            Integer leader = kafka.getControllerId(clusterId);
            targets.sort(Comparator.comparingInt(node -> Objects.equals(node.get("nodeId"), leader) ? 1 : 0));
        }
        if ("DYNAMIC_ALLOWED".equals(classification)) {
            Map<String, String> normalizedChanges = new LinkedHashMap<>();
            request.getChanges().forEach((key, value) -> normalizedChanges.put(key.trim(), value));
            for (Map<String, Object> target : targets) kafka.validateBrokerConfigs(clusterId, ((Number) target.get("nodeId")).intValue(), normalizedChanges);
        }
        result.put("key", request.getChanges().size() + " properties"); result.put("properties", properties); result.put("targets", targets);
        return result;
    }

    private record PreviewContext(Cluster internal, Map<Integer, Map<String, Object>> live,
                                  Map<Integer, Map<String, String>> files, RollingRestartHealthService.HealthSnapshot health) {}

    private Map<String, Object> previewSingle(UUID clusterId, ChangeRequest request, boolean allowUnchanged, PreviewContext context) {
        Cluster internal = context == null ? clusters.findWithServicesById(clusterId).orElse(null) : context.internal();
        if (internal == null && !externalClusters.existsById(clusterId)) {
            throw new IllegalArgumentException("Cluster not found.");
        }
        if (request == null || (!"BROKER".equalsIgnoreCase(request.getRole())
                && !"CONTROLLER".equalsIgnoreCase(request.getRole()))) {
            throw new IllegalArgumentException("Select brokers or controllers.");
        }
        String role = request.getRole().toUpperCase(Locale.ROOT);
        String key = request.getKey() == null ? "" : request.getKey().trim();
        if (!key.matches("[A-Za-z][A-Za-z0-9._-]{0,127}") || PROTECTED_KEYS.contains(key)
                || key.toLowerCase(Locale.ROOT).matches(".*(password|secret|token|jaas|credential|keystore|truststore).*")) {
            throw new IllegalArgumentException("This property cannot be changed from the cluster configuration flow.");
        }
        String value = request.getValue();
        if (value == null || value.length() > 4096 || value.contains("\n") || value.contains("\r")) {
            throw new IllegalArgumentException("Provide a single-line property value (up to 4096 characters).");
        }
        Map<Integer, Map<String, Object>> live = context == null ? kafka.getBrokerConfigs(clusterId) : context.live();
        if (live.isEmpty()) throw new IllegalStateException("No live brokers are available.");
        Set<Integer> available = "BROKER".equals(role) ? live.keySet() : internal == null ? Set.of()
                : internal.getServices().stream().filter(service -> "controller".equalsIgnoreCase(service.getRole()))
                .map(ClusterServiceAssignment::getNodeId).filter(Objects::nonNull).collect(java.util.stream.Collectors.toSet());
        List<Integer> targets;
        if ("ALL".equalsIgnoreCase(request.getScope())) {
            targets = available.stream().sorted().toList();
        } else if ("SPECIFIC".equalsIgnoreCase(request.getScope())) {
            targets = request.getTargetNodeIds() == null ? List.of() : request.getTargetNodeIds().stream().distinct().sorted().toList();
            if (targets.isEmpty() || !available.containsAll(targets)) {
                throw new IllegalArgumentException("Select one or more known nodes in this cluster.");
            }
        } else {
            throw new IllegalArgumentException("Select all nodes or specific nodes.");
        }
        if (targets.isEmpty()) throw new IllegalArgumentException("No eligible nodes were found for this role.");
        boolean dynamic = "BROKER".equals(role);
        if ("BROKER".equals(role)) {
            Boolean firstReadOnly = null;
            for (Integer brokerId : targets) {
                Object raw = live.get(brokerId).get(key);
                if (!(raw instanceof Map<?, ?> details)) {
                    throw new IllegalArgumentException("Kafka does not report property " + key + " on broker " + brokerId + ".");
                }
                if (Boolean.TRUE.equals(details.get("isSensitive"))) throw new IllegalArgumentException("Sensitive properties are excluded from configuration changes.");
                boolean readOnly = Boolean.TRUE.equals(details.get("isReadOnly"));
                if (firstReadOnly != null && firstReadOnly != readOnly) {
                    throw new IllegalArgumentException("The property has different update policies on selected brokers.");
                }
                firstReadOnly = readOnly;
            }
            dynamic = !Boolean.TRUE.equals(firstReadOnly);
        }
        if ("CONTROLLER".equals(role)) KafkaPropertyCatalog.requireRecognizedControllerSetting(key, live);
        if (!dynamic) {
            if (internal == null) {
                throw new IllegalArgumentException("External static changes use the agent-backed versioned file editor.");
            }
            long roleCount = internal.getServices().stream().filter(service -> "BROKER".equals(role)
                    ? "broker".equalsIgnoreCase(service.getRole()) : "controller".equalsIgnoreCase(service.getRole())).count();
            if ("BROKER".equals(role) && roleCount < 2) {
                throw new IllegalStateException("Broker rolling change requires at least two dedicated brokers.");
            }
            if ("CONTROLLER".equals(role) && roleCount < 3) {
                throw new IllegalStateException("Controller rolling change requires at least three dedicated controllers.");
            }
            if ("CONTROLLER".equals(role)) {
                Integer active = kafka.getControllerId(clusterId);
                if (active == null || !available.contains(active)) {
                    throw new IllegalStateException("Active KRaft controller could not be determined.");
                }
                targets = new ArrayList<>(targets);
                targets.sort(Comparator.comparingInt((Integer nodeId) -> nodeId.equals(active) ? 1 : 0)
                        .thenComparingInt(Integer::intValue));
            }
        }
        List<Map<String, Object>> changes = new ArrayList<>();
        for (Integer nodeId : targets) {
            Map<String, Object> change = new LinkedHashMap<>();
            change.put("nodeId", nodeId);
            if (dynamic) {
                Map<?, ?> details = (Map<?, ?>) live.get(nodeId).get(key);
                change.put("oldValue", details.get("value"));
            } else {
                ClusterServiceAssignment service = findService(internal, role, nodeId);
                Map<String, Object> config = mergedConfig(internal, service);
                config.put("mode", internal.getMode());
                config.put("version", internal.getKafkaVersion());
                if (context == null) {
                    UUID readTask = deployment.readKafkaProperty(clusterId, service.getHostId(), service.getRole(),
                            String.valueOf(nodeId), json(config), configPath(internal, service, config), key);
                    Task readResult = taskAwaiter.await(readTask, 60);
                    Map<String, Object> observed = readMap(readResult.getLogOutput());
                    change.put("oldValue", Boolean.TRUE.equals(observed.get("found")) ? observed.get("value") : null);
                } else {
                    Map<String, String> properties = context.files().get(nodeId);
                    if (properties == null) {
                        properties = readStaticProperties(clusterId, role, List.of(nodeId)).get(nodeId);
                        context.files().put(nodeId, properties);
                    }
                    change.put("oldValue", properties.get(key));
                }
            }
            change.put("newValue", value);
            changes.add(change);
        }
        changes.removeIf(change -> Objects.equals(change.get("oldValue"), value));
        if (changes.isEmpty() && !allowUnchanged) {
            throw new IllegalArgumentException("The selected nodes already have the requested value.");
        }
        if (dynamic && context == null) for (Integer nodeId : targets) kafka.validateBrokerConfigs(clusterId, nodeId, Map.of(key, value));
        var snapshot = context == null ? health.inspect(clusterId, live.size(), Set.of()) : context.health();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("role", role);
        result.put("scope", request.getScope().toUpperCase(Locale.ROOT));
        result.put("classification", dynamic ? "DYNAMIC_ALLOWED" : "ROLLING_RESTART_REQUIRED");
        result.put("valueSource", dynamic ? "live Kafka broker configuration" : "active node configuration read by the agent");
        result.put("key", key);
        result.put("targets", changes);
        result.put("healthy", snapshot.healthy());
        result.put("healthFailures", snapshot.failures());
        result.put("clusterLocked", !jobs.findByResourceKeyAndStatusIn("cluster:" + clusterId, ACTIVE_STATUSES).isEmpty());
        return result;
    }

    public Map<String, Object> apply(UUID clusterId, ChangeRequest request) {
        Map<String, Object> plan = preview(clusterId, request);
        if (!Boolean.TRUE.equals(plan.get("healthy"))) {
            throw new IllegalStateException("Cluster health precheck failed: " + plan.get("healthFailures"));
        }
        Map<String, Object> result = new TransactionTemplate(transactions)
                .execute(ignored -> persistPlan(clusterId, request, plan));
        if (result == null) throw new IllegalStateException("Configuration job was not created.");
        return result;
    }

    private Map<String, Object> persistPlan(UUID clusterId, ChangeRequest request, Map<String, Object> plan) {
        // Serialize plan creation across management server instances.
        jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext(?))", Object.class, "config-change:" + clusterId);
        if (!jobs.findByResourceKeyAndStatusIn("cluster:" + clusterId, ACTIVE_STATUSES).isEmpty()) {
            throw new IllegalStateException("Another cluster operation is in progress. Wait for its job to finish.");
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> targets = (List<Map<String, Object>>) plan.get("targets");
        List<JobStep> steps = new ArrayList<>();
        boolean staticChange = "ROLLING_RESTART_REQUIRED".equals(plan.get("classification"));
        Cluster internal = clusters.findWithServicesById(clusterId).orElseThrow(() -> new IllegalArgumentException("Configuration changes require a managed node agent for mandatory backups."));
        int brokerCount = kafka.getBrokerConfigs(clusterId).size();
        int order = 1;
        for (Map<String, Object> target : targets) {
            JobStep step = new JobStep();
            step.setStepOrder(order++);
            step.setTargetId(String.valueOf(target.get("nodeId")));
            step.setName((staticChange ? "Back up, patch and restart " : "Apply ") + plan.get("key")
                    + " on " + String.valueOf(plan.get("role")).toLowerCase(Locale.ROOT) + " " + target.get("nodeId"));
            if (staticChange) {
                int nodeId = ((Number) target.get("nodeId")).intValue();
                ClusterServiceAssignment service = findService(internal, String.valueOf(plan.get("role")), nodeId);
                Map<String, Object> config = mergedConfig(internal, service);
                config.put("mode", internal.getMode());
                config.put("version", internal.getKafkaVersion());
                Map<String, Object> stepPayload = new LinkedHashMap<>();
                stepPayload.put("operation", "STATIC");
                stepPayload.put("nodeId", nodeId);
                stepPayload.put("hostId", service.getHostId());
                stepPayload.put("role", service.getRole());
                stepPayload.put("configPath", configPath(internal, service, config));
                stepPayload.put("configJson", json(config));
                stepPayload.put("key", plan.get("key"));
                stepPayload.put("value", request.getValue());
                stepPayload.put("expectedOldValue", target.get("oldValue"));
                stepPayload.put("expectedBrokerCount", brokerCount);
                step.setPayload(json(stepPayload));
            } else {
                int nodeId = ((Number) target.get("nodeId")).intValue();
                ClusterServiceAssignment service = findService(internal, "BROKER", nodeId);
                Map<String, Object> config = mergedConfig(internal, service);
                config.put("mode", internal.getMode()); config.put("version", internal.getKafkaVersion());
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("operation", "DYNAMIC"); payload.put("brokerId", nodeId);
                payload.put("key", plan.get("key")); payload.put("value", request.getValue());
                payload.put("expectedOldValue", target.get("oldValue") == null ? "" : target.get("oldValue"));
                payload.put("expectedBrokerCount", brokerCount);
                payload.put("hostId", service.getHostId()); payload.put("role", service.getRole());
                payload.put("configPath", configPath(internal, service, config)); payload.put("configJson", json(config));
                step.setPayload(json(payload));
            }
            if (target.get("changes") instanceof Map<?, ?>) {
                Map<String, Object> grouped = readMap(step.getPayload());
                grouped.remove("key"); grouped.remove("value"); grouped.remove("expectedOldValue");
                grouped.put("changes", target.get("changes")); grouped.put("expectedValues", target.get("expectedValues"));
                step.setPayload(json(grouped));
            }
            steps.add(step);
        }
        Job job = new Job();
        job.setType(JobType.TARGETED_CONFIG_CHANGE);
        job.setStatus(JobStatus.PENDING);
        job.setResourceKey("cluster:" + clusterId);
        job.setRollbackSupported(false); // Explicit recovery after partial application.
        job.setPayload(json(Map.of("clusterId", clusterId.toString(), "role", plan.get("role"), "scope", plan.get("scope"),
                "classification", plan.get("classification"), "key", plan.get("key"))));
        Job saved = jobService.createJob(job, steps);
        return Map.of("jobId", saved.getId().toString(), "status", saved.getStatus().name());
    }

    private String json(Object value) {
        try { return mapper.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Cannot save configuration plan", e); }
    }

    private ClusterServiceAssignment findService(Cluster cluster, String role, int nodeId) {
        if (cluster == null || cluster.getServices() == null) throw new IllegalArgumentException("No managed node topology is available.");
        return cluster.getServices().stream().filter(service -> service.getNodeId() != null
                && service.getNodeId() == nodeId && (role.equalsIgnoreCase(service.getRole()) || ("BROKER".equals(role) && "broker_controller".equalsIgnoreCase(service.getRole()))))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Dedicated " + role.toLowerCase(Locale.ROOT)
                        + " node " + nodeId + " is not managed by this cluster."));
    }

    private Map<String, Object> mergedConfig(Cluster cluster, ClusterServiceAssignment service) {
        Map<String, Object> config = readMap(cluster.getConfigJson());
        config.putAll(readMap(service.getConfigJson()));
        return config;
    }

    private Map<String, Object> readMap(String json) {
        if (json == null || json.isBlank()) return new LinkedHashMap<>();
        try { return mapper.readValue(json, new com.fasterxml.jackson.core.type.TypeReference<LinkedHashMap<String, Object>>() {}); }
        catch (Exception e) { throw new IllegalStateException("Stored node configuration is invalid", e); }
    }

    private String configPath(Cluster cluster, ClusterServiceAssignment service, Map<String, Object> config) {
        String base = String.valueOf(config.getOrDefault("kafka_install_base_dir",
                config.getOrDefault("kafka_install_dir", "/opt"))).replaceAll("/+$", "");
        if (base.isBlank()) base = "/opt";
        if (base.endsWith("/kafka")) { /* active symlink already selected */ }
        else if (base.substring(base.lastIndexOf('/') + 1).startsWith("kafka_")) base = base.substring(0, base.lastIndexOf('/')) + "/kafka";
        else base += "/kafka";
        boolean flat = cluster.getKafkaVersion() != null && cluster.getKafkaVersion().matches("^[vV]?[4-9].*");
        String directory = base + "/config" + (flat ? "" : "/kraft");
        return directory + ("CONTROLLER".equalsIgnoreCase(service.getRole()) ? "/controller.properties" : "broker_controller".equalsIgnoreCase(service.getRole()) ? "/server.properties" : "/broker.properties");
    }
}
