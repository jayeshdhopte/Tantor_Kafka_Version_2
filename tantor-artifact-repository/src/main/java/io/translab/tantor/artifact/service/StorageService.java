package io.translab.tantor.artifact.service;

import io.translab.tantor.artifact.config.StorageProperties;
import io.translab.tantor.artifact.domain.ServiceType;
import io.translab.tantor.artifact.dto.ChecksumResult;
import io.translab.tantor.artifact.exception.StorageException;
import io.translab.tantor.artifact.exception.UploadLimitExceededException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.Semaphore;
import java.util.UUID;

/**
 * Owns the on-disk repository layout:
 *
 * <pre>
 *   {basePath}/artifacts/{serviceDir}/{version}[/{classifier}]/{fileName}
 *   {basePath}/artifacts/{serviceDir}/{version}[/{classifier}]/manifest.json
 * </pre>
 *
 * Writes go to a temp file first and are atomically moved into place, so a
 * crashed upload never leaves a half-written binary that an agent might pull.
 */
@Service
public class StorageService {

    private static final Logger log = LoggerFactory.getLogger(StorageService.class);
    private static final String ARTIFACTS_DIR = "artifacts";
    public static final String MANIFEST_FILE = "manifest.json";

    private final StorageProperties properties;
    private final ChecksumService checksumService;
    private Path artifactsRoot;
    private Semaphore uploadSlots;

    public StorageService(StorageProperties properties, ChecksumService checksumService) {
        this.properties = properties;
        this.checksumService = checksumService;
    }

    @PostConstruct
    void init() {
        try {
            this.artifactsRoot = Paths.get(properties.getBasePath(), ARTIFACTS_DIR)
                    .toAbsolutePath().normalize();
            for (ServiceType type : ServiceType.values()) {
                Files.createDirectories(artifactsRoot.resolve(type.directory()));
            }
            uploadSlots = new Semaphore(properties.getMaxConcurrentUploads(), true);
            log.info("Tantor artifact repository initialised at {}", artifactsRoot);
        } catch (IOException e) {
            throw new StorageException("Unable to initialise repository at " + properties.getBasePath(), e);
        }
    }

    /** Repository-relative directory for a coordinate, e.g. {@code kafka/3.7.0/2.13}. */
    public String relativeDir(ServiceType type, String version, String classifier) {
        StringBuilder sb = new StringBuilder(type.directory()).append('/').append(version);
        if (classifier != null && !classifier.isBlank()) {
            sb.append('/').append(classifier);
        }
        return sb.toString();
    }

    public record TempStoreResult(Path tempPath, ChecksumResult checksumResult) {}

    /**
     * Stream an upload into a temporary file in the repository, computing checksums in the same pass.
     *
     * @return the path to the temporary file and computed checksums
     */
    public TempStoreResult storeTemporarily(String fileNameHint, InputStream data) {
        validateFileName(fileNameHint);
        if (!uploadSlots.tryAcquire()) {
            throw new UploadLimitExceededException("Too many concurrent uploads; try again shortly");
        }
        try {
            ensureFreeSpace();
            Path tmp = Files.createTempFile(artifactsRoot, ".upload-", "-" + fileNameHint);
            ChecksumResult result;
            try (OutputStream out = Files.newOutputStream(tmp)) {
                result = checksumService.copyAndDigest(data, out, properties.getMaxUploadBytes());
            } catch (RuntimeException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
            return new TempStoreResult(tmp, result);
        } catch (IOException e) {
            throw new StorageException("Failed to store temporary upload for " + fileNameHint, e);
        } finally {
            uploadSlots.release();
        }
    }

    /**
     * Move a temporarily stored file to its final destination in the repository.
     */
    public void moveToFinal(Path tempPath, ServiceType type, String version, String classifier, String fileName) {
        String relDir = relativeDir(type, version, classifier);
        moveToFinal(tempPath, relDir, fileName);
    }

    public void moveToFinal(Path tempPath, String relDir, String fileName) {
        Path targetDir = resolveSafe(relDir);
        try {
            Files.createDirectories(targetDir);
            validateFileName(fileName);
            Path target = targetDir.resolve(fileName).normalize();
            if (!target.startsWith(targetDir)) {
                throw new StorageException("Path traversal blocked for artifact filename");
            }
            Files.move(tempPath, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            log.info("Stored {}", relDir + "/" + fileName);
        } catch (IOException e) {
            throw new StorageException("Failed to move to final path: " + relDir + "/" + fileName, e);
        }
    }

    /**
     * Safely delete a temporary file.
     */
    public void deleteTemp(Path tempPath) {
        if (tempPath == null) return;
        try {
            Files.deleteIfExists(tempPath);
        } catch (IOException e) {
            log.warn("Failed to delete temp file {}", tempPath);
        }
    }

    /** Write the per-artifact manifest.json next to the binary. */
    public void writeManifest(String relativeDir, String manifestJson) {
        Path dir = resolveSafe(relativeDir);
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve(MANIFEST_FILE), manifestJson);
        } catch (IOException e) {
            throw new StorageException("Failed to write manifest for " + relativeDir, e);
        }
    }

    /** Resolve a stored binary to an absolute, validated path. */
    public Path resolveBinary(String relativeDir, String fileName) {
        return resolveSafe(relativeDir).resolve(fileName);
    }

    public boolean exists(String relativeDir, String fileName) {
        return Files.isRegularFile(resolveBinary(relativeDir, fileName));
    }

    public InputStream openStream(String relativeDir, String fileName) {
        try {
            return Files.newInputStream(resolveBinary(relativeDir, fileName));
        } catch (IOException e) {
            throw new StorageException("Failed to open " + relativeDir + "/" + fileName, e);
        }
    }

    /** Delete the binary (and manifest) for a coordinate; directory left in place. */
    public void deleteBinary(String relativeDir, String fileName) {
        try {
            Files.deleteIfExists(resolveBinary(relativeDir, fileName));
            Files.deleteIfExists(resolveSafe(relativeDir).resolve(MANIFEST_FILE));
        } catch (IOException e) {
            throw new StorageException("Failed to delete " + relativeDir + "/" + fileName, e);
        }
    }

    public Path artifactsRoot() {
        return artifactsRoot;
    }

    /**
     * Resolve a repository-relative path and guarantee it stays inside the
     * repository root, defeating any {@code ../} traversal in a coordinate.
     */
    private Path resolveSafe(String relativeDir) {
        Path resolved = artifactsRoot.resolve(relativeDir).normalize();
        if (!resolved.startsWith(artifactsRoot)) {
            throw new StorageException("Path traversal blocked for: " + relativeDir);
        }
        return resolved;
    }

    /**
     * Keeps rejected bytes outside the downloadable artifact tree for forensic
     * review. Quarantined files are never referenced by an AVAILABLE artifact.
     */
    public void quarantine(Path tempPath, String fileName) {
        if (tempPath == null || !Files.exists(tempPath)) return;
        try {
            validateFileName(fileName);
            Path quarantineDir = artifactsRoot.resolve("quarantine").normalize();
            Files.createDirectories(quarantineDir);
            Files.move(tempPath, quarantineDir.resolve(UUID.randomUUID() + "-" + fileName),
                    StandardCopyOption.ATOMIC_MOVE);
            log.warn("Quarantined rejected upload {}", fileName);
        } catch (Exception e) {
            log.warn("Unable to quarantine rejected upload {}; deleting temporary bytes", fileName, e);
            deleteTemp(tempPath);
        }
    }

    private void ensureFreeSpace() throws IOException {
        long usable = Files.getFileStore(artifactsRoot).getUsableSpace();
        long required = Math.addExact(properties.getMinimumFreeSpaceBytes(), properties.getMaxUploadBytes());
        if (usable < required) {
            throw new UploadLimitExceededException("Insufficient repository disk space for a safe upload");
        }
    }

    public static void validateFileName(String value) {
        if (value == null || value.isBlank() || !value.equals(Paths.get(value).getFileName().toString())
                || value.contains("\\") || value.contains("/") || value.equals(".") || value.equals("..")) {
            throw new IllegalArgumentException("Artifact filename must be a single safe filename");
        }
    }
}
