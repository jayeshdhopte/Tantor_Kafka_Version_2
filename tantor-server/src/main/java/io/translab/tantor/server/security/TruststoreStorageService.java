package io.translab.tantor.server.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Set;
import java.util.UUID;

@Service
@Slf4j
public class TruststoreStorageService {

    private final Path truststoreDir;

    public TruststoreStorageService(@Value("${tantor.security.truststore-dir}") String dirPath) {
        this.truststoreDir = Paths.get(dirPath);
        initDirectory();
    }

    private void initDirectory() {
        try {
            if (!Files.exists(truststoreDir)) {
                Files.createDirectories(truststoreDir);
                // Try setting 700 permissions on UNIX-like systems
                try {
                    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwx------");
                    Files.setPosixFilePermissions(truststoreDir, perms);
                } catch (UnsupportedOperationException e) {
                    log.debug("Posix permissions not supported on this OS for directory {}", truststoreDir);
                }
            }
        } catch (IOException e) {
            log.error("Failed to initialize truststore directory: {}", truststoreDir, e);
            throw new RuntimeException("Could not initialize security directory", e);
        }
    }

    public String saveTruststore(UUID clusterId, String truststoreType, String base64Content) {
        if (base64Content == null || base64Content.isBlank()) return null;
        
        String ext = resolveExtension(truststoreType);
        Path targetFile = truststoreDir.resolve(clusterId.toString() + ext);
        
        try {
            String cleanBase64 = normalizeBase64(base64Content);
            byte[] decoded = Base64.getDecoder().decode(cleanBase64);
            Files.write(targetFile, decoded);
            
            try {
                Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
                Files.setPosixFilePermissions(targetFile, perms);
            } catch (UnsupportedOperationException e) {
                log.debug("Posix permissions not supported for file {}", targetFile);
            }
            
            return targetFile.toAbsolutePath().toString();
        } catch (Exception e) {
            log.error("Failed to save truststore file for cluster {}", clusterId, e);
            throw new RuntimeException("Failed to save truststore file", e);
        }
    }

    public String ensureTruststoreFile(UUID clusterId, String truststoreType, String base64Content, String existingPath) {
        if (existingPath != null && !existingPath.isBlank() && Files.exists(Paths.get(existingPath))) {
            return existingPath;
        }
        return saveTruststore(clusterId, truststoreType, base64Content);
    }

    public void deleteTruststore(UUID clusterId, String truststoreType) {
        String ext = resolveExtension(truststoreType);
        Path targetFile = truststoreDir.resolve(clusterId.toString() + ext);
        try {
            Files.deleteIfExists(targetFile);
        } catch (IOException e) {
            log.warn("Failed to delete truststore file {}", targetFile, e);
        }
    }

    private String normalizeBase64(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        int commaIndex = trimmed.indexOf(',');
        if (commaIndex >= 0) {
            trimmed = trimmed.substring(commaIndex + 1);
        }
        return trimmed.replaceAll("\\s", "");
    }

    private String resolveExtension(String type) {
        if (type == null) return ".jks";
        String normalized = type.toUpperCase();
        if (normalized.startsWith("KEYSTORE_")) {
            normalized = normalized.substring("KEYSTORE_".length());
        }
        return switch (normalized) {
            case "PKCS12" -> ".p12";
            case "PEM" -> ".pem";
            default -> ".jks";
        };
    }
}
