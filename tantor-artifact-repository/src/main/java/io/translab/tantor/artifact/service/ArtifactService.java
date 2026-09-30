package io.translab.tantor.artifact.service;

import io.translab.tantor.artifact.config.StorageProperties;
import io.translab.tantor.artifact.domain.Artifact;
import io.translab.tantor.artifact.domain.ArtifactStatus;
import io.translab.tantor.artifact.domain.ServiceType;
import io.translab.tantor.artifact.dto.ChecksumResult;
import io.translab.tantor.artifact.dto.ManifestDto;
import io.translab.tantor.artifact.exception.ArtifactNotFoundException;
import io.translab.tantor.artifact.exception.ArtifactAlreadyExistsException;
import io.translab.tantor.artifact.exception.ChecksumMismatchException;
import io.translab.tantor.artifact.repository.ArtifactJpaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

/**
 * Coordinates the database index ({@link ArtifactJpaRepository}) with the
 * on-disk store ({@link StorageService}) so the two never drift. Every mutating
 * method either fully succeeds or rolls back both sides.
 */
@Service
public class ArtifactService {

    private static final Logger log = LoggerFactory.getLogger(ArtifactService.class);

    private final ArtifactJpaRepository repository;
    private final StorageService storageService;
    private final ManifestService manifestService;
    private final StorageProperties properties;
    private final PackageValidator packageValidator;

    public ArtifactService(ArtifactJpaRepository repository,
                           StorageService storageService,
                           ManifestService manifestService,
                           StorageProperties properties,
                           PackageValidator packageValidator) {
        this.repository = repository;
        this.storageService = storageService;
        this.manifestService = manifestService;
        this.properties = properties;
        this.packageValidator = packageValidator;
    }

    /** Parameters for an upload. */
    public record UploadCommand(
            ServiceType serviceType,
            String name,
            String version,
            String classifier,
            String storageDirectory,
            String fileName,
            String contentType,
            String description,
            String declaredSha256,
            Map<String, String> attributes,
            boolean overwrite,
            String createdBy
    ) {}

    @Transactional
    public Artifact upload(UploadCommand cmd, InputStream data) {
        StorageService.validateFileName(cmd.fileName());
        validateCoordinate(cmd.version(), "version");
        if (cmd.classifier() != null) validateCoordinate(cmd.classifier(), "classifier");
        String classifier = blankToNull(cmd.classifier());
        // 1. Stream bytes to temporary disk location and compute checksums.
        StorageService.TempStoreResult tempStore = storageService.storeTemporarily(cmd.fileName(), data);
        ChecksumResult cs = tempStore.checksumResult();
        Path tempFile = tempStore.tempPath();

        // Serialize uploads for the same identity/checksum so two concurrent
        // requests cannot both pass the active-ledger duplicate check.
        repository.acquireUploadLock("artifact-version:" + cmd.serviceType() + ":" + cmd.version());
        repository.acquireUploadLock("artifact-checksum:" + cs.sha256());

        if (repository.countActiveByServiceTypeAndVersion(cmd.serviceType(), cmd.version()) > 0) {
            storageService.deleteTemp(tempFile);
            throw new ArtifactAlreadyExistsException("Artifact already exists for " + cmd.serviceType() + " " + cmd.version());
        }
        if (repository.countActiveByChecksumSha256(cs.sha256()) > 0) {
            storageService.deleteTemp(tempFile);
            throw new ArtifactAlreadyExistsException("An artifact with the same SHA-256 checksum already exists");
        }

        // 2. Enforce declared checksum (reject before heavy validation)
        if (properties.isEnforceChecksum() && cmd.declaredSha256() != null
                && !cmd.declaredSha256().equalsIgnoreCase(cs.sha256())) {
            storageService.deleteTemp(tempFile);
            throw new ChecksumMismatchException(
                    "Declared SHA-256 %s does not match computed %s"
                            .formatted(cmd.declaredSha256(), cs.sha256()));
        }

        // 3. Prepare index row
        Artifact artifact = new Artifact();
        artifact.setId(UUID.randomUUID());
        String artifactDir = uploadDirectory(cmd, classifier, artifact.getId());

        artifact.setServiceType(cmd.serviceType());
        artifact.setVersion(cmd.version());
        artifact.setAction("UPLOAD");
        artifact.setRootArtifactId(artifact.getId());
        artifact.setFileName(cmd.fileName());
        artifact.setRelativePath(artifactDir + "/" + cmd.fileName());
        artifact.setFileSizeBytes(cs.sizeBytes());
        artifact.setContentType(cmd.contentType() != null ? cmd.contentType() : "application/gzip");
        artifact.setChecksumSha256(cs.sha256());
        artifact.setChecksumMd5(cs.md5());
        artifact.setCreatedBy(cmd.createdBy() != null ? cmd.createdBy() : "system");
        artifact.setUpdatedBy(artifact.getCreatedBy());

        try {
            artifact.setHostIp(getRealHostIp());
            artifact.setHostname(getRealHostName());
            artifact.setUserName(cmd.createdBy() != null ? cmd.createdBy() : "system");
        } catch (Exception e) {
            artifact.setHostIp(null);
            artifact.setHostname(null);
        }

        try {
            // 4. Validate package contents (extraction test, Kafka version check, malware scan)
            packageValidator.validate(tempFile, cmd.serviceType(), cmd.version(), cmd.fileName());

            // 5. Move to final and set status AVAILABLE
            storageService.moveToFinal(tempFile, artifactDir, cmd.fileName());
            artifact.setFullFilePath(storageService.resolveBinary(artifactDir, cmd.fileName()).toAbsolutePath().normalize().toString());
            artifact.setStatus(ArtifactStatus.AVAILABLE);

            ManifestDto manifest = manifestService.build(artifact, cmd.attributes());
            String manifestJson = manifestService.toJson(manifest);
            storageService.writeManifest(
                    artifactDir, manifestJson);

        } catch (Exception e) {
            // Rejected bytes stay outside the downloadable artifact tree for
            // forensic review; only successfully validated files become AVAILABLE.
            storageService.quarantine(tempFile, cmd.fileName());
            artifact.setStatus(ArtifactStatus.FAILED);
        }

        Artifact saved = repository.save(artifact);

        log.info("Artifact {} registered: {} {} ({} bytes, status: {})",
                saved.getId(), saved.getServiceType(), saved.getVersion(), saved.getFileSizeBytes(), saved.getStatus());
        return saved;
    }

    @Transactional(readOnly = true)
    public Artifact get(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new ArtifactNotFoundException("No artifact with id " + id));
    }

    @Transactional(readOnly = true)
    public Page<Artifact> list(ServiceType serviceType, ArtifactStatus status, Pageable pageable) {
        return repository.search(serviceType, status, pageable);
    }

    /** Resolve the on-disk path for a downloadable artifact. */
    @Transactional(readOnly = true)
    public Path resolveForDownload(UUID id) {
        Artifact a = get(id);
        if (a.getStatus() != ArtifactStatus.AVAILABLE) {
            throw new ArtifactNotFoundException(
                    "Artifact " + id + " is not downloadable (status=" + a.getStatus() + ")");
        }
        return storageService.resolveBinary(
                a.getRelativePath().substring(0, a.getRelativePath().lastIndexOf('/')),
                a.getFileName());
    }

    @Transactional
    public void logDownload(UUID artifactId, String remoteAddr, String by, boolean verified) {
        Artifact source = get(artifactId);
        Artifact entry = actionFrom(source, "DOWNLOAD", source.getStatus(), by);
        entry.setDownloadedBy(by == null || by.isBlank() ? "agent" : by);
        entry.setDownloadedAt(java.time.OffsetDateTime.now());
        entry.setVerifiedChecksum(verified);
        repository.save(entry);
    }

    /**
     * Re-read the stored binary and confirm it still matches the recorded
     * SHA-256. Flips the row to CORRUPTED if it does not.
     */
    @Transactional
    public boolean verifyIntegrity(UUID id) {
        Artifact a = get(id);
        String relDir = a.getRelativePath().substring(0, a.getRelativePath().lastIndexOf('/'));
        ChecksumResult cs;
        try (InputStream in = storageService.openStream(relDir, a.getFileName())) {
            cs = new ChecksumService().digest(in);
        } catch (Exception e) {
            repository.save(actionFrom(a, "VERIFY_FAILED", ArtifactStatus.CORRUPTED, "system"));
            return false;
        }
        boolean ok = cs.sha256().equalsIgnoreCase(a.getChecksumSha256());
        if (!ok) {
            log.warn("Integrity check FAILED for {}: expected {} got {}",
                    id, a.getChecksumSha256(), cs.sha256());
            repository.save(actionFrom(a, "VERIFY_FAILED", ArtifactStatus.CORRUPTED, "system"));
        } else {
            repository.save(actionFrom(a, "VERIFY", a.getStatus(), "system"));
        }
        return ok;
    }

    /** Soft-delete: remove the binary from disk, mark the row DELETED. */
    @Transactional
    public void delete(UUID id) {
        Artifact a = get(id);
        String relDir = a.getRelativePath().substring(0, a.getRelativePath().lastIndexOf('/'));
        storageService.deleteBinary(relDir, a.getFileName());
        repository.save(actionFrom(a, "DELETE", ArtifactStatus.DELETED, "system"));
        log.info("Artifact {} soft-deleted", id);
    }

    private Artifact actionFrom(Artifact source, String action, ArtifactStatus status, String actor) {
        Artifact event = new Artifact();
        event.setId(UUID.randomUUID());
        event.setRootArtifactId(source.getRootArtifactId() == null ? source.getId() : source.getRootArtifactId());
        event.setAction(action);
        event.setServiceType(source.getServiceType());
        event.setVersion(source.getVersion());
        event.setFileName(source.getFileName());
        event.setRelativePath(source.getRelativePath());
        event.setFullFilePath(source.getFullFilePath());
        event.setFileSizeBytes(source.getFileSizeBytes());
        event.setContentType(source.getContentType());
        event.setChecksumSha256(source.getChecksumSha256());
        event.setChecksumMd5(source.getChecksumMd5());
        event.setStatus(status);
        event.setCreatedBy(actor == null || actor.isBlank() ? "system" : actor);
        event.setUpdatedBy(event.getCreatedBy());
        return event;
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    private static void validateCoordinate(String value, String label) {
        if (value == null || value.isBlank() || value.contains("/") || value.contains("\\") || value.contains("..")) {
            throw new IllegalArgumentException("Artifact " + label + " contains an unsafe path value");
        }
    }

    private String uploadDirectory(UploadCommand cmd, String classifier, UUID artifactId) {
        String requested = blankToNull(cmd.storageDirectory());
        if (requested == null) {
            return storageService.relativeDir(cmd.serviceType(), cmd.version(), classifier) + "/" + artifactId;
        }
        String normalized = requested.replace('\\', '/').replaceAll("^/+|/+$", "");
        if (normalized.isBlank() || normalized.equals("..") || normalized.startsWith("../") || normalized.contains("/../")) {
            throw new IllegalArgumentException("Storage directory must be a safe repository-relative path");
        }
        return normalized + "/" + cmd.serviceType().directory() + "/" + cmd.version() + "/" + artifactId;
    }
    private String getRealHostIp() {
        try {
            org.springframework.web.context.request.RequestAttributes attrs = org.springframework.web.context.request.RequestContextHolder.getRequestAttributes();
            if (attrs instanceof org.springframework.web.context.request.ServletRequestAttributes servletAttrs) {
                String localAddr = servletAttrs.getRequest().getLocalAddr();
                if (localAddr != null && !localAddr.startsWith("127.") && !localAddr.equals("0:0:0:0:0:0:0:1") && !localAddr.equals("::1")) {
                    return localAddr;
                }
            }
        } catch (Exception ignored) {}
        try {
            java.util.Enumeration<java.net.NetworkInterface> interfaces = java.net.NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                java.net.NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp()) continue;
                java.util.Enumeration<java.net.InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    java.net.InetAddress addr = addresses.nextElement();
                    if (addr instanceof java.net.Inet4Address && !addr.isLoopbackAddress()) {
                        return addr.getHostAddress();
                    }
                }
            }
            String fallback = java.net.InetAddress.getLocalHost().getHostAddress();
            return isUsableHostAddress(fallback) ? fallback : null;
        } catch (Exception e) {
            return null;
        }
    }

    private String getRealHostName() {
        try {
            org.springframework.web.context.request.RequestAttributes attrs = org.springframework.web.context.request.RequestContextHolder.getRequestAttributes();
            if (attrs instanceof org.springframework.web.context.request.ServletRequestAttributes servletAttrs) {
                String localName = servletAttrs.getRequest().getLocalName();
                if (localName != null && !localName.equalsIgnoreCase("localhost") && !localName.equalsIgnoreCase("127.0.0.1") && !localName.equals("0:0:0:0:0:0:0:1") && !localName.equals("::1")) {
                    return localName;
                }
            }
        } catch (Exception ignored) {}
        try {
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            return isLoopbackName(hostName) ? null : hostName;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isUsableHostAddress(String value) {
        return value != null
                && !value.isBlank()
                && !value.startsWith("127.")
                && !value.equals("0:0:0:0:0:0:0:1")
                && !value.equals("::1")
                && !"localhost".equalsIgnoreCase(value);
    }

    private boolean isLoopbackName(String value) {
        return value == null
                || value.isBlank()
                || "localhost".equalsIgnoreCase(value)
                || "127.0.0.1".equals(value)
                || "0:0:0:0:0:0:0:1".equals(value)
                || "::1".equals(value);
    }
}
