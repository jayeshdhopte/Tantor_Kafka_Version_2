package io.translab.tantor.artifact.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.artifact.config.StorageProperties;
import io.translab.tantor.artifact.domain.Artifact;
import io.translab.tantor.artifact.domain.ArtifactStatus;
import io.translab.tantor.artifact.dto.BundleManifest;
import io.translab.tantor.artifact.dto.ManifestDto;
import io.translab.tantor.artifact.exception.StorageException;
import io.translab.tantor.artifact.exception.UploadLimitExceededException;
import io.translab.tantor.artifact.repository.ArtifactJpaRepository;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Air-gapped transfer support. A bundle is a single {@code .tar.gz} that mirrors
 * the repository layout plus a top-level {@code bundle-manifest.json}:
 *
 * <pre>
 *   bundle-manifest.json
 *   artifacts/kafka/3.7.0/kafka_2.13-3.7.0.tgz
 *   artifacts/kafka/3.7.0/manifest.json
 *   artifacts/prometheus/2.54.0/prometheus-2.54.0.linux-amd64.tar.gz
 *   artifacts/prometheus/2.54.0/manifest.json
 * </pre>
 *
 * Export it on an internet-connected staging server, move it across the
 * air-gap on physical media, and import it on the customer's isolated Tantor
 * server. Every binary is re-checksummed on import via {@link ArtifactService#upload}.
 */
@Service
public class AirGapBundleService {

    private static final Logger log = LoggerFactory.getLogger(AirGapBundleService.class);
    private static final String BUNDLE_MANIFEST = "bundle-manifest.json";

    private final ArtifactJpaRepository repository;
    private final StorageService storageService;
    private final ManifestService manifestService;
    private final ArtifactService artifactService;
    private final ObjectMapper objectMapper;
    private final StorageProperties properties;

    public AirGapBundleService(ArtifactJpaRepository repository,
                               StorageService storageService,
                               ManifestService manifestService,
                               ArtifactService artifactService,
                               ObjectMapper objectMapper,
                               StorageProperties properties) {
        this.repository = repository;
        this.storageService = storageService;
        this.manifestService = manifestService;
        this.artifactService = artifactService;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /**
     * Stream a bundle of the given artifacts (or all AVAILABLE ones if the list
     * is empty) to {@code out}.
     */
    public void export(List<UUID> ids, OutputStream out) {
        List<Artifact> artifacts = ids == null || ids.isEmpty()
                ? repository.findByStatusAndAction(ArtifactStatus.AVAILABLE, "UPLOAD")
                : repository.findAllById(ids).stream()
                    .filter(a -> a.getStatus() == ArtifactStatus.AVAILABLE).toList();

        List<BundleManifest.Entry> entries = new ArrayList<>();
        try (BufferedOutputStream bos = new BufferedOutputStream(out);
             GzipCompressorOutputStream gz = new GzipCompressorOutputStream(bos);
             TarArchiveOutputStream tar = new TarArchiveOutputStream(gz)) {

            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            for (Artifact a : artifacts) {
                String relDir = dirOf(a.getRelativePath());
                String entryPath = "artifacts/" + a.getRelativePath();
                String manifestPath = "artifacts/" + relDir + "/" + StorageService.MANIFEST_FILE;

                Path binary = storageService.resolveBinary(relDir, a.getFileName());
                addFileEntry(tar, entryPath, binary);

                String manifestJson = manifestService.toJson(manifestService.build(a, null));
                addBytesEntry(tar, manifestPath, manifestJson.getBytes(StandardCharsets.UTF_8));

                entries.add(new BundleManifest.Entry(entryPath, manifestPath, a.getChecksumSha256()));
            }

            BundleManifest bundle = new BundleManifest(
                    BundleManifest.CURRENT_SCHEMA_VERSION, OffsetDateTime.now(), hostname(), entries);
            addBytesEntry(tar, BUNDLE_MANIFEST,
                    objectMapper.writeValueAsBytes(bundle));

            tar.finish();
            log.info("Exported air-gap bundle with {} artifact(s)", entries.size());
        } catch (IOException e) {
            throw new StorageException("Failed to export air-gap bundle", e);
        }
    }

    /**
     * Import a previously exported bundle. Extracts to a temp directory, then
     * re-ingests each artifact through the normal upload path (which re-verifies
     * the SHA-256 declared in the per-artifact manifest).
     *
     * @return number of artifacts imported
     */
    public int importBundle(InputStream in) {
        Path tmp;
        try {
            tmp = Files.createTempDirectory("tantor-bundle-");
        } catch (IOException e) {
            throw new StorageException("Cannot create temp dir for bundle import", e);
        }
        try {
            extract(new BoundedInputStream(in, properties.getMaxBundleBytes()), tmp);
            int imported = 0;
            Path artifactsDir = tmp.resolve("artifacts");
            if (!Files.isDirectory(artifactsDir)) {
                throw new StorageException("Bundle is missing an 'artifacts/' directory");
            }
            try (Stream<Path> walk = Files.walk(artifactsDir)) {
                List<Path> manifests = walk
                        .filter(p -> p.getFileName().toString().equals(StorageService.MANIFEST_FILE))
                        .toList();
                for (Path manifestPath : manifests) {
                    imported += importOne(manifestPath);
                }
            }
            log.info("Imported {} artifact(s) from bundle", imported);
            return imported;
        } catch (IOException e) {
            throw new StorageException("Failed to import air-gap bundle", e);
        } finally {
            deleteQuietly(tmp);
        }
    }

    private int importOne(Path manifestPath) throws IOException {
        ManifestDto manifest = manifestService.fromJson(Files.readString(manifestPath));
        Path binary = manifestPath.resolveSibling(manifest.fileName());
        if (!Files.isRegularFile(binary)) {
            log.warn("Skipping {}: binary {} not present in bundle",
                    manifestPath, manifest.fileName());
            return 0;
        }
        ArtifactService.UploadCommand cmd = new ArtifactService.UploadCommand(
                manifest.serviceType(),
                manifest.name(),
                manifest.version(),
                manifest.classifier(),
                null,
                manifest.fileName(),
                manifest.contentType(),
                "Imported from air-gap bundle",
                manifest.sha256(),       // declared checksum -> re-verified on ingest
                manifest.attributes(),
                true,                    // overwrite on import
                "airgap-import");
        try (InputStream bin = Files.newInputStream(binary)) {
            artifactService.upload(cmd, bin);
        }
        return 1;
    }

    // --- tar helpers ------------------------------------------------------

    private void addFileEntry(TarArchiveOutputStream tar, String path, Path file) throws IOException {
        TarArchiveEntry entry = new TarArchiveEntry(file.toFile(), path);
        entry.setSize(Files.size(file));
        tar.putArchiveEntry(entry);
        Files.copy(file, tar);
        tar.closeArchiveEntry();
    }

    private void addBytesEntry(TarArchiveOutputStream tar, String path, byte[] data) throws IOException {
        TarArchiveEntry entry = new TarArchiveEntry(path);
        entry.setSize(data.length);
        tar.putArchiveEntry(entry);
        tar.write(data);
        tar.closeArchiveEntry();
    }

    private void extract(InputStream in, Path targetRoot) throws IOException {
        try (GzipCompressorInputStream gz = new GzipCompressorInputStream(in);
             TarArchiveInputStream tar = new TarArchiveInputStream(gz)) {
            TarArchiveEntry entry;
            int entries = 0;
            long expanded = 0;
            while ((entry = tar.getNextEntry()) != null) {
                if (++entries > properties.getMaxArchiveEntries()) {
                    throw new UploadLimitExceededException("Bundle contains too many archive entries");
                }
                if (entry.getSize() > properties.getMaxArchiveEntryBytes()) {
                    throw new UploadLimitExceededException("Bundle entry exceeds the configured size limit");
                }
                if (entry.isSymbolicLink() || entry.isLink()) {
                    throw new StorageException("Bundle contains a symbolic or hard link: " + entry.getName());
                }
                Path resolved = targetRoot.resolve(entry.getName()).normalize();
                // Guard against zip-slip / tar traversal.
                if (!resolved.startsWith(targetRoot)) {
                    throw new StorageException("Blocked path traversal in bundle: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(resolved);
                } else {
                    Files.createDirectories(resolved.getParent());
                    try (OutputStream out = Files.newOutputStream(resolved)) {
                        byte[] buffer = new byte[8192];
                        int read;
                        long entryBytes = 0;
                        while ((read = tar.read(buffer)) != -1) {
                            entryBytes += read;
                            expanded += read;
                            if (entryBytes > properties.getMaxArchiveEntryBytes()
                                    || expanded > properties.getMaxArchiveExpandedBytes()) {
                                throw new UploadLimitExceededException("Bundle expands beyond the configured size limit");
                            }
                            out.write(buffer, 0, read);
                        }
                    }
                }
            }
        }
    }

    private void deleteQuietly(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) { }
                });
        } catch (IOException e) {
            log.warn("Failed to clean temp dir {}", dir, e);
        }
    }

    private static String dirOf(String relativePath) {
        return relativePath.substring(0, relativePath.lastIndexOf('/'));
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static final class BoundedInputStream extends java.io.FilterInputStream {
        private final long maxBytes;
        private long bytesRead;

        private BoundedInputStream(InputStream in, long maxBytes) {
            super(in);
            this.maxBytes = maxBytes;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) increment(1);
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = super.read(buffer, offset, length);
            if (read > 0) increment(read);
            return read;
        }

        private void increment(int count) {
            if (count > maxBytes - bytesRead) {
                throw new UploadLimitExceededException("Bundle exceeds the configured maximum of " + maxBytes + " bytes");
            }
            bytesRead += count;
        }
    }
}
