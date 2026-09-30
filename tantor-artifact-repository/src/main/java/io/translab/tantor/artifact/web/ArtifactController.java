package io.translab.tantor.artifact.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.artifact.domain.Artifact;
import io.translab.tantor.artifact.domain.ArtifactStatus;
import io.translab.tantor.artifact.domain.ServiceType;
import io.translab.tantor.artifact.dto.ArtifactResponse;
import io.translab.tantor.artifact.dto.ManifestDto;
import io.translab.tantor.artifact.dto.PageResponse;
import io.translab.tantor.artifact.service.ArtifactService;
import io.translab.tantor.artifact.service.ManifestService;
import io.translab.tantor.artifact.service.StorageService;
import io.translab.tantor.artifact.audit.ArtifactAuditService;
import io.translab.tantor.artifact.util.RoleAuthenticationUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

@Tag(name = "Artifacts", description = "Upload, download, version and verify deployment artifacts")
@RestController
@RequestMapping("/api/v1/artifacts")
public class ArtifactController {
    private static final Logger log = LoggerFactory.getLogger(ArtifactController.class);

    private final ArtifactService artifactService;
    private final ManifestService manifestService;
    private final ObjectMapper objectMapper;
    private final ArtifactAuditService auditService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    public ArtifactController(ArtifactService artifactService,
                              ManifestService manifestService,
                              ObjectMapper objectMapper,
                              ArtifactAuditService auditService,
                              RoleAuthenticationUtil roleAuthenticationUtil) {
        this.artifactService = artifactService;
        this.manifestService = manifestService;
        this.objectMapper = objectMapper;
        this.auditService = auditService;
        this.roleAuthenticationUtil = roleAuthenticationUtil;
    }

    @Operation(summary = "Upload an artifact (multipart)")
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<ArtifactResponse> upload(
            @RequestParam ServiceType serviceType,
            @RequestParam String version,
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String classifier,
            @RequestParam(required = false) String storageDirectory,
            @RequestParam(required = false) String contentType,
            @RequestParam(required = false) String description,
            @RequestParam(required = false) String sha256,
            @RequestParam(required = false) String attributesJson,
            @RequestParam(defaultValue = "false") boolean overwrite,
            @RequestParam MultipartFile file,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) throws IOException {

        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ARTIFACT_UPLOAD)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        String actor = currentUser(authorization);
        String fileName = file.getOriginalFilename();
        ArtifactService.UploadCommand cmd = new ArtifactService.UploadCommand(
                serviceType,
                name != null ? name : fileName,
                version,
                classifier,
                storageDirectory,
                fileName,
                contentType != null ? contentType : file.getContentType(),
                description,
                sha256,
                parseAttributes(attributesJson),
                overwrite,
                actor);

        Artifact saved;
        try {
            saved = artifactService.upload(cmd, file.getInputStream());
        } catch (RuntimeException | IOException e) {
            safeAudit(actor, "PACKAGE_UPLOAD_FAILED", version, "FAILED", null, null,
                    Map.of("fileName", String.valueOf(fileName), "serviceType", serviceType.name(), "error", String.valueOf(e.getMessage())), request.getRemoteAddr());
            throw e;
        }
        auditUpload(saved, actor, request.getRemoteAddr());
        if (saved.getStatus() == ArtifactStatus.FAILED) {
            return ResponseEntity.status(422).body(ArtifactResponse.from(saved));
        }
        return ResponseEntity.status(201).body(ArtifactResponse.from(saved));
    }

    @Operation(summary = "Upload an artifact via raw streaming PUT (bypasses multipart buffering)")
    @PutMapping(consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<ArtifactResponse> uploadRaw(
            @RequestParam ServiceType serviceType,
            @RequestParam String version,
            @RequestParam String fileName,
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String classifier,
            @RequestParam(required = false) String storageDirectory,
            @RequestParam(required = false) String contentType,
            @RequestParam(required = false) String description,
            @RequestParam(required = false) String sha256,
            @RequestParam(required = false) String attributesJson,
            @RequestParam(defaultValue = "false") boolean overwrite,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) throws IOException {

        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ARTIFACT_UPLOAD)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        String actor = currentUser(authorization);
        ArtifactService.UploadCommand cmd = new ArtifactService.UploadCommand(
                serviceType,
                name != null ? name : fileName,
                version,
                classifier,
                storageDirectory,
                fileName,
                contentType,
                description,
                sha256,
                parseAttributes(attributesJson),
                overwrite,
                actor);

        Artifact saved;
        try {
            saved = artifactService.upload(cmd, request.getInputStream());
        } catch (RuntimeException | IOException e) {
            safeAudit(actor, "PACKAGE_UPLOAD_FAILED", version, "FAILED", null, null,
                    Map.of("fileName", fileName, "serviceType", serviceType.name(), "error", String.valueOf(e.getMessage())), request.getRemoteAddr());
            throw e;
        }
        auditUpload(saved, actor, request.getRemoteAddr());
        if (saved.getStatus() == ArtifactStatus.FAILED) {
            return ResponseEntity.status(422).body(ArtifactResponse.from(saved));
        }
        return ResponseEntity.status(201).body(ArtifactResponse.from(saved));
    }

    @Operation(summary = "List artifacts with optional service/status filters")
    @GetMapping
    public PageResponse<ArtifactResponse> list(
            @RequestParam(required = false) ServiceType serviceType,
            @RequestParam(required = false) ArtifactStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "createdAt") String sortBy,
            @RequestParam(defaultValue = "DESC") Sort.Direction direction) {

        Page<Artifact> result = artifactService.list(
                serviceType, status, PageRequest.of(page, size, Sort.by(direction, sortBy)));
        return PageResponse.from(result, ArtifactResponse::from);
    }

    @Operation(summary = "Get artifact metadata by id")
    @GetMapping("/{id}")
    public ArtifactResponse get(@PathVariable UUID id) {
        return ArtifactResponse.from(artifactService.get(id));
    }

    @Operation(summary = "Get the artifact manifest")
    @GetMapping("/{id}/manifest")
    public ManifestDto manifest(@PathVariable UUID id) {
        Artifact a = artifactService.get(id);
        return manifestService.build(a, Map.of());
    }

    @Operation(summary = "Download the artifact binary; agents verify the X-Checksum-SHA256 header")
    @GetMapping("/{id}/download")
    public ResponseEntity<Resource> download(@PathVariable UUID id, HttpServletRequest request) {
        Artifact a = artifactService.get(id);
        Path path = artifactService.resolveForDownload(id);
        Resource resource = new FileSystemResource(path);

        artifactService.logDownload(id, request.getRemoteAddr(), currentUser(), false);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"" + a.getFileName() + "\"")
                .header(HttpHeaders.ETAG, "\"" + a.getChecksumSha256() + "\"")
                .header("X-Checksum-SHA256", a.getChecksumSha256())
                .header("X-Checksum-MD5", a.getChecksumMd5() != null ? a.getChecksumMd5() : "")
                .contentLength(a.getFileSizeBytes())
                .contentType(MediaType.parseMediaType(a.getContentType()))
                .body(resource);
    }

    @Operation(summary = "Re-verify on-disk integrity against the recorded checksum")
    @PostMapping("/{id}/verify")
    public ResponseEntity<Map<String, Object>> verify(@PathVariable UUID id,
                                                      @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ARTIFACT_UPLOAD)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        boolean ok = artifactService.verifyIntegrity(id);
        return ResponseEntity.ok(Map.of("id", id, "verified", ok));
    }

    @Operation(summary = "Soft-delete an artifact (removes binary, retains audit row)")
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id,
                                       @RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.ARTIFACT_DELETE)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        String actor = currentUser(authorization);
        Artifact artifact = artifactService.get(id);
        artifactService.delete(id);
        safeAudit(actor, "PACKAGE_REMOVED", id.toString(), "SUCCESS",
                Map.of("status", artifact.getStatus().name(), "fileName", artifact.getFileName()),
                Map.of("status", "DELETED"), null, null);
        return ResponseEntity.noContent().build();
    }

    private void auditUpload(Artifact saved, String actor, String ipAddress) {
        safeAudit(actor, "PACKAGE_UPLOADED", saved.getId().toString(),
                saved.getStatus() == ArtifactStatus.FAILED ? "FAILED" : "SUCCESS", null,
                Map.of("serviceType", saved.getServiceType().name(), "version", saved.getVersion(),
                        "fileName", saved.getFileName(), "status", saved.getStatus().name(),
                        "sha256", saved.getChecksumSha256(), "size", saved.getFileSizeBytes()),
                Map.of("validationStatus", saved.getStatus().name()), ipAddress);
    }

    private void safeAudit(String actor, String action, String resourceId, String status,
                           Object oldValue, Object newValue, Object details, String ipAddress) {
        try {
            auditService.record(actor, action, resourceId, status, oldValue, newValue, details, ipAddress);
        } catch (Exception e) {
            log.warn("Artifact audit recording failed for action {} resource {}", action, resourceId, e);
        }
    }

    private Map<String, String> parseAttributes(String attributesJson) {
        if (attributesJson == null || attributesJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(attributesJson, new TypeReference<Map<String, String>>() {});
        } catch (IOException e) {
            throw new IllegalArgumentException("attributesJson is not valid JSON: " + e.getMessage());
        }
    }

    /**
     * Placeholder for the authenticated principal. Authentication/RBAC is
     * delivered in Phase 3; until then uploads are attributed to "system".
     */
    private String currentUser() {
        return "system";
    }

    private String currentUser(String authorization) {
        return roleAuthenticationUtil.username(authorization);
    }
}
