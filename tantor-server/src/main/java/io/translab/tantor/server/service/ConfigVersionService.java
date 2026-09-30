package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.persistence.ConfigVersionRepository;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class ConfigVersionService {

    private static final Set<String> GENERATED_KEYS = Set.of(
            "process.roles", "node.id", "broker.id", "listeners", "advertised.listeners",
            "controller.quorum.voters", "controller.quorum.bootstrap.servers", "zookeeper.connect",
            "dataDir", "clientPort", "servers"
    );
    private static final Set<String> POSITIVE_INTEGER_KEYS = Set.of(
            "num.partitions", "default.replication.factor", "offsets.topic.replication.factor",
            "transaction.state.log.replication.factor", "min.insync.replicas",
            "transaction.state.log.min.isr", "num.network.threads", "num.io.threads"
    );

    private final ConfigVersionRepository configVersionRepository;
    private final ClusterServiceAssignmentRepository serviceRepository;
    private final ClusterRepository clusterRepository;
    private final JobService jobService;
    private final ActivityAlertService activityAlertService;
    private final ObjectMapper objectMapper;
    private final AuditService auditService;

    public Map<String, Object> preview(Map<String, Object> oldConfig, Map<String, Object> newConfig, boolean restart) {
        List<Map<String, Object>> diff = diff(oldConfig, newConfig);
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        if (newConfig == null || newConfig.isEmpty()) {
            errors.add("Configuration must contain at least one property.");
        } else {
            for (Map.Entry<String, Object> entry : newConfig.entrySet()) {
                String key = entry.getKey();
                String value = stringValue(entry.getValue());
                if (key == null || !key.matches("[A-Za-z0-9._-]+")) {
                    errors.add("Invalid configuration key: " + key);
                    continue;
                }
                if (key.length() > 255 || value.length() > 16_384) {
                    errors.add("Configuration property is too long: " + key);
                }
                if (value.contains("\n") || value.contains("\r")) {
                    errors.add("Multi-line values are not allowed: " + key);
                }
                if (GENERATED_KEYS.contains(key) && oldConfig.containsKey(key)
                        && !Objects.equals(stringValue(oldConfig.get(key)), value)) {
                    errors.add(key + " is topology-managed and cannot be changed here.");
                }
                if (POSITIVE_INTEGER_KEYS.contains(key)) {
                    try {
                        if (Integer.parseInt(value) <= 0) errors.add(key + " must be greater than zero.");
                    } catch (NumberFormatException e) {
                        errors.add(key + " must be a positive integer.");
                    }
                }
            }
        }

        validateReplication(newConfig, errors);
        if (diff.isEmpty()) errors.add("No configuration changes were detected.");
        if (!restart) warnings.add("The file will change without a service restart; static properties may not become active immediately.");

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("valid", errors.isEmpty());
        result.put("errors", errors);
        result.put("warnings", warnings);
        result.put("diff", diff);
        return result;
    }

    @Transactional
    public ConfigVersion createVersion(
            Cluster cluster,
            ClusterServiceAssignment service,
            String configFileName,
            Map<String, Object> oldConfig,
            Map<String, Object> newConfig,
            boolean approvalRequested,
            boolean restart
    ) {
        Map<String, Object> validation = preview(oldConfig, newConfig, restart);
        if (!Boolean.TRUE.equals(validation.get("valid"))) {
            throw new IllegalArgumentException("Configuration validation failed: " + validation.get("errors"));
        }
        boolean approvalRequired = false;

        ConfigVersion version = new ConfigVersion();
        version.setClusterId(cluster.getId());
        version.setServiceId(service.getId());
        version.setHostId(service.getHostId());
        version.setComponent(service.getRole());
        version.setConfigFileName(requireText(configFileName, "Configuration file name is required."));
        version.setConfigVersion(configVersionRepository.maxVersion(
                cluster.getId(), service.getHostId(), service.getRole(), configFileName) + 1);
        version.setOldConfig(writeJson(oldConfig));
        version.setNewConfig(writeJson(newConfig));
        version.setValidationResult(writeJson(validation));
        version.setCreatedBy(currentUser());
        version.setApprovalRequired(approvalRequired);
        version.setStatus(approvalRequired ? ConfigVersionStatus.PENDING_APPROVAL : ConfigVersionStatus.VALIDATED);
        ConfigVersion saved = configVersionRepository.saveAndFlush(version);
        auditService.record("CONFIG_CHANGE", "CONFIG_VERSION_CREATED", "CONFIG_VERSION", saved.getId().toString(),
                cluster.getId(), "SUCCESS", oldConfig, newConfig, null,
                Map.of("hostId", service.getHostId(), "component", service.getRole(),
                        "file", configFileName, "version", saved.getConfigVersion()));
        activityAlertService.logActivity("INFO", "Saved config version v" + saved.getConfigVersion()
                + " for " + service.getHostId() + "; active config unchanged", cluster.getId());
        return saved;
    }

    public List<ConfigVersion> history(UUID clusterId, UUID serviceId) {
        return serviceId == null
                ? configVersionRepository.findByClusterIdOrderByCreatedAtDesc(clusterId)
                : configVersionRepository.findByClusterIdAndServiceIdOrderByConfigVersionDesc(clusterId, serviceId);
    }

    @Transactional
    public ConfigVersion approve(UUID clusterId, UUID versionId) {
        ConfigVersion version = ownedVersion(clusterId, versionId);
        if (version.getStatus() != ConfigVersionStatus.PENDING_APPROVAL) {
            throw new IllegalStateException("Only versions pending approval can be approved.");
        }
        version.setApprovedBy(currentUser());
        version.setApprovedAt(Instant.now());
        version.setStatus(ConfigVersionStatus.APPROVED);
        auditService.record("APPROVAL", "CONFIG_VERSION_APPROVED", "CONFIG_VERSION", version.getId().toString(),
                clusterId, "SUCCESS", Map.of("status", ConfigVersionStatus.PENDING_APPROVAL.name()),
                Map.of("status", ConfigVersionStatus.APPROVED.name()),
                Map.of("required", true, "approvedBy", currentUser(), "approvedAt", version.getApprovedAt()),
                Map.of("version", version.getConfigVersion(), "hostId", version.getHostId()));
        activityAlertService.logActivity("INFO", "Approved config version v" + version.getConfigVersion(), clusterId);
        return configVersionRepository.save(version);
    }

    @Transactional
    public Job apply(UUID clusterId, UUID versionId, boolean restart) {
        ConfigVersion version = ownedVersion(clusterId, versionId);
        ConfigVersionStatus previousStatus = version.getStatus();
        if (version.getStatus() != ConfigVersionStatus.VALIDATED
                && version.getStatus() != ConfigVersionStatus.APPROVED
                && version.getStatus() != ConfigVersionStatus.FAILED) {
            throw new IllegalStateException("Version must be validated or approved before it can be applied.");
        }
        if (Boolean.TRUE.equals(version.getApprovalRequired()) && version.getApprovedBy() == null) {
            throw new IllegalStateException("Approval is required before this version can be applied.");
        }

        Cluster cluster = clusterRepository.findById(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("Cluster not found."));
        ClusterServiceAssignment service = serviceRepository.findById(version.getServiceId())
                .orElseThrow(() -> new IllegalArgumentException("Service assignment no longer exists."));
        if (!clusterId.equals(service.getCluster().getId())) {
            throw new IllegalArgumentException("Service assignment does not belong to this cluster.");
        }
        assertVersionIsNotStale(version);

        Map<String, Object> baseConfig = readMap(cluster.getConfigJson());
        Map<String, Object> serviceConfig = readMap(service.getConfigJson());
        baseConfig.putAll(serviceConfig);
        baseConfig.put("mode", cluster.getMode());
        baseConfig.put("version", cluster.getKafkaVersion());

        Map<String, Object> newProperties = readMap(version.getNewConfig());
        Map<String, Object> activeServiceConfig = serviceConfig.isEmpty()
                ? new LinkedHashMap<>(baseConfig) : new LinkedHashMap<>(serviceConfig);
        activeServiceConfig.put("properties", newProperties);

        Map<String, Object> stepPayload = new LinkedHashMap<>();
        stepPayload.put("operation", "service");
        stepPayload.put("configVersionId", version.getId().toString());
        stepPayload.put("configVersion", version.getConfigVersion().toString());
        stepPayload.put("configPath", version.getConfigFileName());
        stepPayload.put("hostId", service.getHostId());
        stepPayload.put("role", service.getRole());
        stepPayload.put("nodeId", service.getNodeId() == null ? "1" : String.valueOf(service.getNodeId()));
        stepPayload.put("configJson", writeJson(baseConfig));
        stepPayload.put("propertiesTemplate", serializeProperties(newProperties));
        stepPayload.put("previousConfigJson", service.getConfigJson() == null ? "{}" : service.getConfigJson());
        stepPayload.put("previousPropertiesTemplate", serializeProperties(readMap(version.getOldConfig())));
        stepPayload.put("restart", restart);

        JobStep step = new JobStep();
        step.setStepOrder(1);
        step.setTargetId(service.getHostId());
        step.setName("Back up and apply " + version.getConfigFileName() + " v" + version.getConfigVersion());
        step.setPayload(writeJson(stepPayload));

        Job job = new Job();
        job.setType(JobType.CONFIG_CHANGE);
        job.setStatus(JobStatus.PENDING);
        job.setRollbackSupported(!Boolean.TRUE.equals(version.getApprovalRequired()));
        job.setResourceKey("cluster:" + clusterId);
        job.setPayload(writeJson(Map.of(
                "clusterId", clusterId.toString(),
                "configVersionId", version.getId().toString(),
                "activeServiceConfigJson", writeJson(activeServiceConfig)
        )));
        Job saved = jobService.createJob(job, List.of(step));
        version.setJobId(saved.getId());
        version.setStatus(ConfigVersionStatus.APPLYING);
        configVersionRepository.save(version);
        auditService.record("CONFIG_CHANGE", "CONFIG_APPLY_REQUESTED", "CONFIG_VERSION", version.getId().toString(),
                clusterId, "REQUESTED", Map.of("status", previousStatus.name()),
                Map.of("status", ConfigVersionStatus.APPLYING.name(), "jobId", saved.getId()),
                version.getApprovedBy() == null ? null : Map.of("approvedBy", version.getApprovedBy()),
                Map.of("version", version.getConfigVersion(), "restart", restart));
        activityAlertService.logActivity("INFO", "Queued config version v" + version.getConfigVersion()
                + " for backup and apply", clusterId);
        return saved;
    }

    @Transactional
    public ConfigVersion createRollback(UUID clusterId, UUID targetVersionId) {
        ConfigVersion target = ownedVersion(clusterId, targetVersionId);
        ConfigVersion active = configVersionRepository
                .findFirstByClusterIdAndServiceIdAndStatusOrderByConfigVersionDesc(
                        clusterId, target.getServiceId(), ConfigVersionStatus.APPLIED)
                .orElseThrow(() -> new IllegalStateException("There is no applied version to roll back."));
        if (active.getId().equals(target.getId())) {
            throw new IllegalStateException("The selected version is already active.");
        }
        Cluster cluster = clusterRepository.findById(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("Cluster not found."));

        ConfigVersion rollback = new ConfigVersion();
        rollback.setClusterId(clusterId);
        rollback.setServiceId(target.getServiceId());
        rollback.setHostId(target.getHostId());
        rollback.setComponent(target.getComponent());
        rollback.setConfigFileName(target.getConfigFileName());
        rollback.setConfigVersion(configVersionRepository.maxVersion(
                clusterId, target.getHostId(), target.getComponent(), target.getConfigFileName()) + 1);
        rollback.setOldConfig(active.getNewConfig());
        rollback.setNewConfig(target.getNewConfig());
        rollback.setRollbackVersion(target.getConfigVersion());
        rollback.setCreatedBy(io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        boolean approvalRequired = Boolean.TRUE.equals(target.getApprovalRequired())
                || "UAT".equalsIgnoreCase(cluster.getEnvironment()) || "PROD".equalsIgnoreCase(cluster.getEnvironment());
        rollback.setApprovalRequired(approvalRequired);
        rollback.setStatus(approvalRequired ? ConfigVersionStatus.PENDING_APPROVAL : ConfigVersionStatus.VALIDATED);
        rollback.setValidationResult(writeJson(preview(readMap(active.getNewConfig()), readMap(target.getNewConfig()), true)));
        ConfigVersion saved = configVersionRepository.save(rollback);
        auditService.record("CONFIG_CHANGE", "CONFIG_ROLLBACK_VERSION_CREATED", "CONFIG_VERSION", saved.getId().toString(),
                clusterId, "SUCCESS", readMap(active.getNewConfig()), readMap(target.getNewConfig()),
                Map.of("required", approvalRequired),
                Map.of("rollbackVersion", target.getConfigVersion(), "newVersion", saved.getConfigVersion()));
        return saved;
    }

    @Transactional
    public void markApplied(UUID versionId, String activeServiceConfigJson) {
        ConfigVersion version = configVersionRepository.findById(versionId).orElseThrow();
        configVersionRepository.findFirstByClusterIdAndServiceIdAndStatusOrderByConfigVersionDesc(
                        version.getClusterId(), version.getServiceId(), ConfigVersionStatus.APPLIED)
                .filter(previous -> !previous.getId().equals(version.getId()))
                .ifPresent(previous -> {
                    previous.setStatus(ConfigVersionStatus.SUPERSEDED);
                    configVersionRepository.save(previous);
                });
        serviceRepository.findById(version.getServiceId()).ifPresent(service -> {
            service.setConfigJson(activeServiceConfigJson);
            serviceRepository.save(service);
        });
        version.setStatus(ConfigVersionStatus.APPLIED);
        version.setAppliedAt(Instant.now());
        configVersionRepository.save(version);
        clusterRepository.findById(version.getClusterId()).ifPresent(cluster -> {
            cluster.setUpdatedBy(io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
            clusterRepository.save(cluster);
        });
        auditService.record("CONFIG_CHANGE", "CONFIG_VERSION_APPLIED", "CONFIG_VERSION", version.getId().toString(),
                version.getClusterId(), "SUCCESS", readMap(version.getOldConfig()), readMap(version.getNewConfig()),
                version.getApprovedBy() == null ? null : Map.of("approvedBy", version.getApprovedBy()),
                Map.of("version", version.getConfigVersion(), "hostId", version.getHostId(), "appliedAt", version.getAppliedAt()));
        activityAlertService.logActivity("INFO", "Applied config version v" + version.getConfigVersion()
                + " after agent backup", version.getClusterId());
    }

    @Transactional
    public void markFailed(UUID versionId) {
        configVersionRepository.findById(versionId).ifPresent(version -> {
            version.setStatus(ConfigVersionStatus.FAILED);
            configVersionRepository.save(version);
            auditService.record("CONFIG_CHANGE", "CONFIG_VERSION_APPLY_FAILED", "CONFIG_VERSION", version.getId().toString(),
                    version.getClusterId(), "FAILED", null, Map.of("status", ConfigVersionStatus.FAILED.name()),
                    null, Map.of("version", version.getConfigVersion(), "hostId", version.getHostId()));
        });
    }

    @Transactional
    public void markJobRolledBack(UUID versionId) {
        ConfigVersion version = configVersionRepository.findById(versionId).orElseThrow();
        ConfigVersion rollback = new ConfigVersion();
        rollback.setClusterId(version.getClusterId());
        rollback.setServiceId(version.getServiceId());
        rollback.setHostId(version.getHostId());
        rollback.setComponent(version.getComponent());
        rollback.setConfigFileName(version.getConfigFileName());
        rollback.setConfigVersion(configVersionRepository.maxVersion(version.getClusterId(), version.getHostId(),
                version.getComponent(), version.getConfigFileName()) + 1);
        rollback.setOldConfig(version.getNewConfig());
        rollback.setNewConfig(version.getOldConfig());
        rollback.setRollbackVersion(Math.max(1, version.getConfigVersion() - 1));
        rollback.setCreatedBy(io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        rollback.setApprovedBy(io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        rollback.setApprovalRequired(false);
        rollback.setStatus(ConfigVersionStatus.APPLIED);
        rollback.setAppliedAt(Instant.now());
        rollback.setValidationResult("{}");
        configVersionRepository.save(rollback);
        serviceRepository.findById(version.getServiceId()).ifPresent(service -> {
            Map<String, Object> stored = readMap(service.getConfigJson());
            stored.put("properties", readMap(version.getOldConfig()));
            service.setConfigJson(writeJson(stored));
            serviceRepository.save(service);
        });
        version.setStatus(ConfigVersionStatus.SUPERSEDED);
        configVersionRepository.save(version);
    }

    private void assertVersionIsNotStale(ConfigVersion version) {
        configVersionRepository.findFirstByClusterIdAndServiceIdAndStatusOrderByConfigVersionDesc(
                        version.getClusterId(), version.getServiceId(), ConfigVersionStatus.APPLIED)
                .ifPresent(active -> {
                    if (!readMap(active.getNewConfig()).equals(readMap(version.getOldConfig()))) {
                        throw new IllegalStateException("This version was created from an older active config. Create a new version from the current config.");
                    }
                });
    }

    private ConfigVersion ownedVersion(UUID clusterId, UUID versionId) {
        ConfigVersion version = configVersionRepository.findById(versionId)
                .orElseThrow(() -> new IllegalArgumentException("Configuration version not found."));
        if (!clusterId.equals(version.getClusterId())) {
            throw new IllegalArgumentException("Configuration version does not belong to this cluster.");
        }
        return version;
    }

    private List<Map<String, Object>> diff(Map<String, Object> oldConfig, Map<String, Object> newConfig) {
        Set<String> keys = new TreeSet<>();
        keys.addAll(oldConfig.keySet());
        keys.addAll(newConfig.keySet());
        List<Map<String, Object>> result = new ArrayList<>();
        for (String key : keys) {
            String oldValue = oldConfig.containsKey(key) ? stringValue(oldConfig.get(key)) : null;
            String newValue = newConfig.containsKey(key) ? stringValue(newConfig.get(key)) : null;
            if (Objects.equals(oldValue, newValue)) continue;
            String type = oldValue == null ? "ADDED" : newValue == null ? "REMOVED" : "MODIFIED";
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("key", key);
            item.put("type", type);
            item.put("oldValue", oldValue == null ? "" : oldValue);
            item.put("newValue", newValue == null ? "" : newValue);
            result.add(item);
        }
        return result;
    }

    private void validateReplication(Map<String, Object> config, List<String> errors) {
        Integer replication = firstInteger(config, "default.replication.factor", "offsets.topic.replication.factor");
        Integer minIsr = firstInteger(config, "min.insync.replicas", "transaction.state.log.min.isr");
        if (replication != null && minIsr != null && minIsr > replication) {
            errors.add("min.insync.replicas cannot be greater than the replication factor.");
        }
    }

    private Integer firstInteger(Map<String, Object> config, String... keys) {
        for (String key : keys) {
            if (!config.containsKey(key)) continue;
            try { return Integer.parseInt(stringValue(config.get(key))); } catch (NumberFormatException ignored) { return null; }
        }
        return null;
    }

    private String serializeProperties(Map<String, Object> properties) {
        StringBuilder result = new StringBuilder();
        properties.forEach((key, value) -> {
            if (!"servers".equals(key)) result.append(key).append('=').append(stringValue(value)).append('\n');
        });
        return result.toString();
    }

    private Map<String, Object> readMap(String json) {
        if (json == null || json.isBlank()) return new LinkedHashMap<>();
        try {
            return objectMapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {});
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid stored configuration JSON.", e);
        }
    }

    private String writeJson(Object value) {
        try { return objectMapper.writeValueAsString(value); }
        catch (Exception e) { throw new IllegalArgumentException("Unable to serialize configuration.", e); }
    }

    private String currentUser() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth == null || auth.getName() == null || auth.getName().isBlank() ? "anonymous" : auth.getName();
    }

    private String requireText(String value, String message) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(message);
        return value.trim();
    }

    private String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }
}
