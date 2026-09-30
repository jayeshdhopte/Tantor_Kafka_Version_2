package io.translab.tantor.artifact.service;

import io.translab.tantor.artifact.domain.ServiceType;
import io.translab.tantor.artifact.config.StorageProperties;
import io.translab.tantor.artifact.exception.PackageValidationException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
public class PackageValidator {

    private static final Logger log = LoggerFactory.getLogger(PackageValidator.class);
    private final StorageProperties properties;
    private final MalwareScanner malwareScanner;

    public PackageValidator(StorageProperties properties, MalwareScanner malwareScanner) {
        this.properties = properties;
        this.malwareScanner = malwareScanner;
    }

    public void validate(Path archivePath, ServiceType serviceType, String expectedVersion, String fileName) {
        if (serviceType == ServiceType.JMX_EXPORTER) {
            validateJar(archivePath, fileName);
            malwareScanner.scan(archivePath);
            return;
        }

        validateArchiveExtension(fileName);
        validateStructureAndVersion(archivePath, serviceType, expectedVersion);
        malwareScanner.scan(archivePath);
    }

    private void validateArchiveExtension(String fileName) {
        String normalized = fileName == null ? "" : fileName.toLowerCase(java.util.Locale.ROOT);
        if (!normalized.endsWith(".tgz") && !normalized.endsWith(".tar.gz")) {
            throw new PackageValidationException("Invalid package extension. Only .tgz and .tar.gz are supported.");
        }
    }

    private void validateJar(Path jarPath, String fileName) {
        if (fileName == null || !fileName.toLowerCase(java.util.Locale.ROOT).endsWith(".jar")) {
            throw new PackageValidationException("Invalid JMX exporter extension. Only .jar is supported.");
        }

        try (InputStream in = Files.newInputStream(jarPath)) {
            byte[] magic = in.readNBytes(2);
            if (magic.length < 2 || magic[0] != 'P' || magic[1] != 'K') {
                throw new PackageValidationException("Invalid JMX exporter jar. The file is not a valid Java archive.");
            }
        } catch (PackageValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new PackageValidationException("JMX exporter jar validation failed: " + e.getMessage(), e);
        }

        long compressedSize;
        try {
            compressedSize = Math.max(1L, Files.size(jarPath));
        } catch (IOException e) {
            throw new PackageValidationException("Unable to inspect JMX exporter archive size", e);
        }
        long expanded = 0;
        int entries = 0;
        try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(jarPath))) {
            ZipEntry entry;
            byte[] buffer = new byte[8192];
            while ((entry = zip.getNextEntry()) != null) {
                if (++entries > properties.getMaxArchiveEntries()) {
                    throw new PackageValidationException("JAR contains too many entries");
                }
                String name = entry.getName();
                if (name == null || name.startsWith("/") || name.contains("\\")
                        || name.equals("..") || name.startsWith("../") || name.contains("/../")) {
                    throw new PackageValidationException("JAR contains an unsafe entry: " + name);
                }
                int read;
                long entryBytes = 0;
                while ((read = zip.read(buffer)) != -1) {
                    entryBytes += read;
                    expanded += read;
                    if (entryBytes > properties.getMaxArchiveEntryBytes()
                            || expanded > properties.getMaxArchiveExpandedBytes()
                            || expanded / compressedSize > properties.getMaxArchiveCompressionRatio()) {
                        throw new PackageValidationException("JAR expands beyond the configured safety limit");
                    }
                }
            }
        } catch (PackageValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new PackageValidationException("JMX exporter archive validation failed: " + e.getMessage(), e);
        }

        log.info("JMX exporter jar validation passed for {}", jarPath);
    }

    private void validateStructureAndVersion(Path archivePath, ServiceType serviceType, String expectedVersion) {
        boolean isKafka = serviceType == ServiceType.KAFKA;
        boolean foundExpectedVersion = false;
        boolean extractionTestPassed = false;

        try (CountingInputStream fi = new CountingInputStream(Files.newInputStream(archivePath));
             InputStream bi = new java.io.BufferedInputStream(fi);
             GzipCompressorInputStream gzi = new GzipCompressorInputStream(bi);
             TarArchiveInputStream ti = new TarArchiveInputStream(gzi)) {

            TarArchiveEntry entry;
            int entries = 0;
            long expanded = 0;
            byte[] buffer = new byte[8192];
            while ((entry = ti.getNextTarEntry()) != null) {
                if (++entries > properties.getMaxArchiveEntries()) {
                    throw new PackageValidationException("Archive contains too many entries");
                }
                rejectUnsafeEntry(entry);
                if (entry.getSize() > properties.getMaxArchiveEntryBytes()) {
                    throw new PackageValidationException("Archive entry exceeds the configured size limit");
                }
                extractionTestPassed = true; // successfully read at least one entry without corruption
                String name = entry.getName();
                
                if (isKafka) {
                    // Check for Kafka jar to verify version
                    if (name.contains("libs/kafka_") && name.endsWith(".jar")) {
                        if (name.contains(expectedVersion)) {
                            foundExpectedVersion = true;
                        }
                    }
                }
                int read;
                long entryBytes = 0;
                while ((read = ti.read(buffer)) != -1) {
                    entryBytes += read;
                    expanded += read;
                    if (entryBytes > properties.getMaxArchiveEntryBytes() || expanded > properties.getMaxArchiveExpandedBytes()) {
                        throw new PackageValidationException("Archive expands beyond the configured size limit");
                    }
                    long compressed = Math.max(1L, fi.getByteCount());
                    if (expanded / compressed > properties.getMaxArchiveCompressionRatio()) {
                        throw new PackageValidationException("Archive compression ratio exceeds the configured safety limit");
                    }
                }
            }
        } catch (PackageValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new PackageValidationException("Package extraction test failed. The archive may be corrupted: " + e.getMessage(), e);
        }

        if (!extractionTestPassed) {
            throw new PackageValidationException("Package is empty or corrupted.");
        }

        if (isKafka && !foundExpectedVersion) {
            throw new PackageValidationException(
                    String.format("Kafka version detection failed. Expected version '%s' but could not find corresponding kafka jars in the package.", expectedVersion)
            );
        }
        
        log.info("Package structure and version validation passed for {}", archivePath);
    }

    private void rejectUnsafeEntry(TarArchiveEntry entry) {
        String name = entry.getName();
        if (name == null || name.startsWith("/") || name.startsWith("\\") || name.contains("\\")
                || name.equals("..") || name.startsWith("../") || name.contains("/../")
                || entry.isSymbolicLink() || entry.isLink()) {
            throw new PackageValidationException("Archive contains an unsafe entry: " + name);
        }
    }

    private static final class CountingInputStream extends FilterInputStream {
        private long byteCount;

        private CountingInputStream(InputStream in) { super(in); }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) byteCount++;
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = super.read(buffer, offset, length);
            if (read > 0) byteCount += read;
            return read;
        }

        private long getByteCount() { return byteCount; }
    }
}
