package io.translab.tantor.artifact.dto;

import io.translab.tantor.artifact.domain.ServiceType;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * The manifest is the contract an agent reads before installing an artifact.
 * It is stored as JSONB on the {@code artifact} row and also written next to
 * the binary on disk as {@code manifest.json}, so an air-gapped bundle is
 * self-describing without a database.
 *
 * @param schemaVersion manifest schema version, for forward compatibility
 * @param serviceType   which service this artifact belongs to
 * @param name          human-friendly artifact name
 * @param version       artifact version (e.g. Kafka "3.7.0")
 * @param classifier    optional qualifier (e.g. scala "2.13", arch "amd64")
 * @param fileName      the binary's file name
 * @param sizeBytes     binary size in bytes
 * @param sha256        SHA-256 of the binary (lower-case hex)
 * @param md5           MD5 of the binary (lower-case hex), optional
 * @param contentType   MIME type of the binary
 * @param createdAt     when the artifact was registered
 * @param attributes    free-form install hints (min Java version, entrypoint, etc.)
 */
public record ManifestDto(
        int schemaVersion,
        ServiceType serviceType,
        String name,
        String version,
        String classifier,
        String fileName,
        long sizeBytes,
        String sha256,
        String md5,
        String contentType,
        OffsetDateTime createdAt,
        Map<String, String> attributes
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
}
