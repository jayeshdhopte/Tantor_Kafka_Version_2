package io.translab.tantor.artifact.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Map;

import io.translab.tantor.artifact.repository.ArtifactJpaRepository;
import io.translab.tantor.artifact.domain.Artifact;
import java.util.Optional;
import java.util.UUID;

@Service
public class ArtifactAuditService {
    private final ArtifactAuditRepository repository;
    private final ObjectMapper objectMapper;
    private final ArtifactJpaRepository artifactRepository;

    public ArtifactAuditService(ArtifactAuditRepository repository, ObjectMapper objectMapper, ArtifactJpaRepository artifactRepository) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.artifactRepository = artifactRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(String actor, String action, String resourceId, String status,
                       Object oldValue, Object newValue, Object details, String ipAddress) {
        ArtifactAuditLog event = new ArtifactAuditLog();
        event.setUserName(actor == null || actor.isBlank() ? "system" : actor);
        event.setCategory("PACKAGE");
        event.setAction(action);
        event.setResourceType("ARTIFACT");
        if (isUuid(resourceId)) event.setArtifactId(resourceId);
        event.setStatus(status);
        event.setDetails(json(details));
        event.setCreatedAt(OffsetDateTime.now());
        
        String finalActor = actor == null || actor.isBlank() ? "system" : actor;
        event.setCreatedBy(finalActor);

        if (resourceId != null) {
            try {
                Optional<Artifact> opt = artifactRepository.findById(UUID.fromString(resourceId));
                if (opt.isPresent()) {
                    Artifact a = opt.get();
                    event.setHostIp(a.getHostIp());
                    event.setHostName(a.getHostname());
                    event.setChecksum(a.getChecksumSha256());
                }
            } catch (Exception e) {
                // ignore invalid UUID
            }
        }
        
        repository.saveAndFlush(event);
    }

    public Page<ArtifactAuditLog> recent(int page, int size) {
        return repository.findAllByOrderByCreatedAtDesc(PageRequest.of(Math.max(page, 0), Math.min(Math.max(size, 1), 500)));
    }

    public java.util.List<ArtifactAuditLog> getLogsForResource(String resourceId) {
        return repository.findByArtifactIdOrderByCreatedAtDesc(resourceId);
    }

    public String integrity() {
        return "NOT_ENABLED";
    }

    private String json(Object value) {
        if (value == null) return null;
        try { return objectMapper.writeValueAsString(value); }
        catch (Exception e) { return "{\"captureError\":true}"; }
    }

    private boolean isUuid(String value) {
        if (value == null || value.isBlank()) return false;
        try {
            UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

}
