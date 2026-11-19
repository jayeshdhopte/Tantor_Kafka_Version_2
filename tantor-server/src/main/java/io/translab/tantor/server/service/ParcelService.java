package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.HostParcel;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.util.*;

@Service
@RequiredArgsConstructor
public class ParcelService {
    private static final String DEFAULT_PARCEL_DIR = "/srv/apps/tantor/parcels";

    private final HostParcelRepository hostParcelRepository;
    private final HostRepository hostRepository;
    private final TaskRepository taskRepository;
    private final ObjectMapper objectMapper;
    private final HostStatusService hostStatusService;
    private final AuditService auditService;
    private final ArtifactRepositoryProperties artifactRepositoryProperties;
    private final AddNodeArtifactCatalog artifactCatalog;
    private final ExternalJavaUpgradeService externalJava;

    public List<HostParcel> listStates() {
        var states = new ArrayList<>(hostParcelRepository.findLatestStates());
        states.addAll(externalJava.states());
        return states;
    }

    @Transactional
    public List<HostParcel> distribute(UUID artifactId, ParcelActionRequest request) {
        return scheduleAction("DISTRIBUTE_PARCEL", artifactId, request);
    }

    @Transactional
    public List<HostParcel> activate(UUID artifactId, ParcelActionRequest request) {
        return scheduleAction("ACTIVATE_PARCEL", artifactId, request);
    }

    @Transactional
    public List<HostParcel> deactivate(UUID artifactId, ParcelActionRequest request) {
        return scheduleAction("DEACTIVATE_PARCEL", artifactId, request);
    }

    @Transactional
    public List<HostParcel> remove(UUID artifactId, ParcelActionRequest request) {
        return scheduleAction("REMOVE_PARCEL", artifactId, request);
    }

    @Transactional
    public void processTaskResult(Task task) {
        if (!isParcelCommand(task.getCommand())) {
            return;
        }

        hostParcelRepository.findFirstByLastTaskIdOrderByCreatedAtDescIdDesc(task.getId()).ifPresent(parcel -> {
            if ("DISTRIBUTE_PARCEL".equals(task.getCommand())
                    && Set.of("SUCCESS", "FAILED").contains(task.getStatus())) {
                hostParcelRepository.flush();
                hostParcelRepository.recordDownloadReceipt(task.getId());
            }
            if ("FAILED".equalsIgnoreCase(task.getStatus())) {
                HostParcel failed = copyEvent(parcel, actionForCommand(task.getCommand()));
                failed.setStatus("FAILED");
                failed.setErrorMsg(task.getErrorMsg());
                HostParcel saved = hostParcelRepository.save(failed);
                auditParcel(saved, task, "FAILED");
                return;
            }

            if ("RUNNING".equalsIgnoreCase(task.getStatus()) || "IN_PROGRESS".equalsIgnoreCase(task.getStatus())) {
                if (!inProgressStatus(task.getCommand()).equals(parcel.getStatus())) {
                    HostParcel running = copyEvent(parcel, actionForCommand(task.getCommand()));
                    running.setStatus(inProgressStatus(task.getCommand()));
                    running.setErrorMsg(null);
                    hostParcelRepository.save(running);
                }
                return;
            }

            if (!"SUCCESS".equalsIgnoreCase(task.getStatus())) {
                return;
            }

            switch (task.getCommand()) {
                case "DISTRIBUTE_PARCEL" -> {
                    // handled on the append-only result event below
                }
                case "ACTIVATE_PARCEL" -> {
                    deactivateOtherActiveParcels(parcel);
                }
                case "DEACTIVATE_PARCEL" -> {
                    // handled on the append-only result event below
                }
                case "REMOVE_PARCEL" -> {
                    // handled on the append-only result event below
                }
                default -> {
                    return;
                }
            }
            HostParcel completed = copyEvent(parcel, actionForCommand(task.getCommand()));
            completed.setStatus(switch (task.getCommand()) {
                case "DISTRIBUTE_PARCEL" -> "DISTRIBUTED";
                case "ACTIVATE_PARCEL" -> "ACTIVE";
                case "DEACTIVATE_PARCEL" -> "DEACTIVATED";
                case "REMOVE_PARCEL" -> "REMOVED";
                default -> parcel.getStatus();
            });
            completed.setActive("ACTIVATE_PARCEL".equals(task.getCommand()));
            completed.setErrorMsg(null);
            HostParcel saved = hostParcelRepository.save(completed);
            auditParcel(saved, task, "SUCCESS");
        });
    }

    private List<HostParcel> scheduleAction(String command, UUID artifactId, ParcelActionRequest request) {
        validateRequest(command, request);
        var external = request.getHostIds().stream().filter(id -> id != null && id.startsWith("external:")).toList();
        if (!external.isEmpty()) {
            var externalRequest = copyRequest(request, external);
            List<HostParcel> result = new ArrayList<>(switch (command) {
                case "DISTRIBUTE_PARCEL" -> externalJava.distribute(artifactId, externalRequest);
                case "REMOVE_PARCEL" -> externalJava.remove(artifactId, externalRequest);
                default -> throw new IllegalArgumentException("External Java packages support distribution and removal only.");
            });
            var internal = request.getHostIds().stream().filter(id -> id == null || !id.startsWith("external:")).toList();
            if (!internal.isEmpty()) result.addAll(scheduleAction(command, artifactId, copyRequest(request, internal)));
            return result;
        }


        boolean javaParcel = "JAVA".equalsIgnoreCase(request.getServiceType());
        AddNodeArtifactCatalog.Artifact approvedJdk = javaParcel && "DISTRIBUTE_PARCEL".equals(command)
                ? artifactCatalog.resolveType("JAVA", request.getVersion(), artifactId.toString())
                : null;
        List<HostParcel> scheduled = new ArrayList<>();
        for (String hostId : request.getHostIds()) {
            Host host = hostRepository.findById(hostId)
                    .orElseThrow(() -> new IllegalArgumentException("Host " + hostId + " was not found."));
            String connectivityStatus = hostStatusService.agentConnectivityStatus(host);
            if (!"ONLINE".equalsIgnoreCase(connectivityStatus)) {
                throw new IllegalArgumentException("Host " + hostId + " agent is not online. Current connectivity: " + connectivityStatus + ".");
            }

            HostParcel previous = hostParcelRepository.findFirstByHostIdAndArtifactIdOrderByCreatedAtDescIdDesc(hostId, artifactId)
                    .orElse(null);
            boolean isNew = previous == null;
            if (previous != null && javaParcel != "JAVA".equalsIgnoreCase(previous.getServiceType())) {
                throw new IllegalArgumentException("The parcel service type cannot be changed.");
            }
            if (javaParcel && !"DISTRIBUTE_PARCEL".equals(command) && !"REMOVE_PARCEL".equals(command)) {
                throw new IllegalArgumentException("Use the Java upgrade workflow to install a staged JDK.");
            }

            if (!isNew && !canRun(command, previous)) {
                throw new IllegalArgumentException("Parcel " + request.getVersion() + " on host " + hostId + " is in state " + previous.getStatus() + " and cannot run " + command + ".");
            }
            if (isNew && !"DISTRIBUTE_PARCEL".equals(command)) {
                throw new IllegalArgumentException("Parcel must be distributed to host " + hostId + " before it can be activated.");
            }

            HostParcel parcel = previous == null ? new HostParcel() : copyEvent(previous, actionForCommand(command));
            parcel.setHostId(hostId);
            parcel.setHostIp(primaryIp(host));
            parcel.setArtifactId(artifactId);
            parcel.setServiceType(defaultString(request.getServiceType(), "KAFKA"));
            parcel.setVersion(required(request.getVersion(), "version"));
            parcel.setFileName(request.getFileName());
            parcel.setChecksum(approvedJdk == null ? request.getChecksum() : approvedJdk.checksum());
            parcel.setParcelDir(resolveParcelDir(command, request, previous, hostId));
            parcel.setAction(actionForCommand(command));
            String initiator = defaultString(auditService.currentActor(), "system");
            parcel.setCreatedBy(initiator);
            parcel.setUpdatedBy(initiator);
            parcel.setStatus(inProgressStatus(command));
            parcel.setErrorMsg(null);
            if ("REMOVE_PARCEL".equals(command) || "DEACTIVATE_PARCEL".equals(command)) {
                parcel.setActive(false);
            }

            Task task = createTask(command, host, parcel);
            taskRepository.save(task);
            parcel.setLastTaskId(task.getId());
            HostParcel saved = hostParcelRepository.save(parcel);
            scheduled.add(saved);
            auditParcel(saved, task, "REQUESTED");
        }
        return scheduled;
    }

    private ParcelActionRequest copyRequest(ParcelActionRequest original, List<String> hosts) {
        ParcelActionRequest copy = new ParcelActionRequest();
        copy.setHostIds(hosts);copy.setChecksum(original.getChecksum());copy.setServiceType(original.getServiceType());copy.setVersion(original.getVersion());
        copy.setFileName(original.getFileName());copy.setParcelDir(original.getParcelDir());copy.setParcelDirs(original.getParcelDirs());return copy;
    }

    private Task createTask(String command, Host host, HostParcel parcel) {
        Task task = new Task();
        task.setHostId(host.getId());
        task.setCommand(command);
        task.setStatus("PENDING");
        if ("DISTRIBUTE_PARCEL".equals(command)) {
            task.setArtifactUrl(resolveAgentArtifactUrl(parcel.getArtifactId()));
            task.setChecksum(parcel.getChecksum());
        }

        Map<String, Object> params = new HashMap<>();
        params.put("artifact_id", parcel.getArtifactId().toString());
        params.put("service_type", parcel.getServiceType());
        params.put("version", parcel.getVersion());
        params.put("file_name", parcel.getFileName());
        params.put("parcel_dir", parcel.getParcelDir());
        params.put("checksum", parcel.getChecksum());
        params.put("initiated_by", parcel.getCreatedBy());
        if ("DISTRIBUTE_PARCEL".equals(command)) {
            String token=UUID.randomUUID().toString();
            params.put("download_context_token",token);
            task.setArtifactUrl(task.getArtifactUrl()+"?downloadToken="+token);
        }
        params.put("target_hostname", host.getHostname());
        params.put("target_host_ip", primaryIp(host));
        try {
            task.setParameters(objectMapper.writeValueAsString(params));
        } catch (JsonProcessingException e) {
            task.setParameters("{}");
        }
        return task;
    }

    private String primaryIp(Host host) {
        if (host.getHostIp() != null && !host.getHostIp().isBlank()) return host.getHostIp();
        if (host.getIpAddresses() == null || host.getIpAddresses().isBlank()) return null;
        try {
            List<String> values = objectMapper.readValue(host.getIpAddresses(),
                    objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
            return values.isEmpty() ? null : values.get(0);
        } catch (Exception ignored) {
            return null;
        }
    }

    private void validateRequest(String command, ParcelActionRequest request) {
        if (request == null || request.getHostIds() == null || request.getHostIds().isEmpty()) {
            throw new IllegalArgumentException("At least one host is required.");
        }
        required(request.getVersion(), "version");
    }

    private boolean canRun(String command, HostParcel parcel) {
        String status = parcel.getStatus() == null ? "" : parcel.getStatus();
        return switch (command) {
            case "DISTRIBUTE_PARCEL" -> Set.of("FAILED", "REMOVED").contains(status);
            case "ACTIVATE_PARCEL" -> Set.of("DISTRIBUTED", "DEACTIVATED", "ACTIVE").contains(status);
            case "DEACTIVATE_PARCEL" -> "ACTIVE".equals(status);
            case "REMOVE_PARCEL" -> Set.of("DISTRIBUTED", "DEACTIVATED", "FAILED", "REMOVED").contains(status);
            default -> false;
        };
    }

    private String inProgressStatus(String command) {
        return switch (command) {
            case "DISTRIBUTE_PARCEL" -> "DISTRIBUTING";
            case "ACTIVATE_PARCEL" -> "ACTIVATING";
            case "DEACTIVATE_PARCEL" -> "DEACTIVATING";
            case "REMOVE_PARCEL" -> "REMOVING";
            default -> "PENDING";
        };
    }

    private String resolveParcelDir(String command, ParcelActionRequest request, HostParcel parcel, String hostId) {
        String requestedDir = request == null ? null : request.getParcelDir();
        if (request != null && request.getParcelDirs() != null) {
            requestedDir = defaultString(request.getParcelDirs().get(hostId), requestedDir);
        }
        if ("DISTRIBUTE_PARCEL".equals(command)) {
            return defaultString(requestedDir, DEFAULT_PARCEL_DIR);
        }
        return defaultString(requestedDir, defaultString(parcel.getParcelDir(), DEFAULT_PARCEL_DIR));
    }

    private boolean isParcelCommand(String command) {
        return command != null && command.endsWith("_PARCEL");
    }

    private String actionForCommand(String command) {
        return switch (command) {
            case "DISTRIBUTE_PARCEL" -> "DISTRIBUTE";
            case "ACTIVATE_PARCEL" -> "ACTIVATE";
            case "DEACTIVATE_PARCEL" -> "DEACTIVATE";
            case "REMOVE_PARCEL" -> "REMOVE";
            default -> "UNKNOWN";
        };
    }

    private void auditParcel(HostParcel parcel, Task task, String status) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("parcelId", parcel.getId().toString());
        details.put("taskId", task.getId().toString());
        details.put("artifactId", parcel.getArtifactId().toString());
        details.put("hostId", parcel.getHostId());
        details.put("hostIp", String.valueOf(parcel.getHostIp()));
        details.put("destination", String.valueOf(parcel.getParcelDir()));
        details.put("version", parcel.getVersion());
        details.put("parcelStatus", parcel.getStatus());
        String actor = defaultString(parcel.getCreatedBy(), defaultString(auditService.currentActor(), "system"));
        details.put("initiatedBy", actor);
        details.put("executor", "REQUESTED".equals(status) ? "management-server" : "agent:" + task.getHostId());
        if (parcel.getErrorMsg() != null && !parcel.getErrorMsg().isBlank()) {
            details.put("error", parcel.getErrorMsg());
        }
        String detailsJson = "{}";
        try {
            detailsJson = objectMapper.writeValueAsString(details);
        } catch (Exception ignored) {}
        
        hostParcelRepository.recordArtifactAudit(
                actor,
                "PARCEL_" + parcel.getAction() + "_" + status,
                parcel.getArtifactId().toString(),
                status,
                detailsJson
        );
    }

    private HostParcel copyEvent(HostParcel source, String action) {
        HostParcel copy = new HostParcel();
        copy.setHostId(source.getHostId());
        copy.setHostIp(source.getHostIp());
        copy.setArtifactId(source.getArtifactId());
        copy.setServiceType(source.getServiceType());
        copy.setVersion(source.getVersion());
        copy.setFileName(source.getFileName());
        copy.setChecksum(source.getChecksum());
        copy.setParcelDir(source.getParcelDir());
        copy.setStatus(source.getStatus());
        copy.setActive(source.isActive());
        copy.setLastTaskId(source.getLastTaskId());
        copy.setErrorMsg(source.getErrorMsg());
        copy.setAction(action);
        copy.setCreatedBy(source.getCreatedBy());
        copy.setUpdatedBy(source.getCreatedBy());
        return copy;
    }

    private void deactivateOtherActiveParcels(HostParcel activatedParcel) {
        for (HostParcel other : hostParcelRepository.findLatestActive(activatedParcel.getHostId(), activatedParcel.getServiceType())) {
            if (!Objects.equals(other.getId(), activatedParcel.getId())) {
                HostParcel deactivated = copyEvent(other, "DEACTIVATE");
                deactivated.setCreatedBy(activatedParcel.getCreatedBy());
                deactivated.setUpdatedBy(activatedParcel.getCreatedBy());
                deactivated.setActive(false);
                deactivated.setStatus("DEACTIVATED");
                hostParcelRepository.save(deactivated);
            }
        }
    }

    private String resolveAgentArtifactUrl(UUID artifactId) {
        return joinArtifactRepoBase("/api/v1/artifacts/" + artifactId + "/download");
    }

    private String joinArtifactRepoBase(String pathAndQuery) {
        String base = artifactRepositoryProperties.getPublicUrl().toString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        String normalizedPath = pathAndQuery.startsWith("/") ? pathAndQuery : "/" + pathAndQuery;
        return base + normalizedPath;
    }

    private String pathAndQuery(URI uri) {
        String rawPath = uri.getRawPath() != null ? uri.getRawPath() : "";
        return uri.getRawQuery() == null ? rawPath : rawPath + "?" + uri.getRawQuery();
    }

    private boolean isLoopbackHost(String host) {
        return host != null && ("localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host) || "::1".equals(host));
    }

    private String required(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required.");
        }
        return value.trim();
    }

    private String defaultString(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    @Data
    public static class ParcelActionRequest {
        private List<String> hostIds;
        private String checksum;
        private String serviceType;
        private String version;
        private String fileName;
        private String parcelDir;
        private Map<String, String> parcelDirs = new HashMap<>();
    }
}
