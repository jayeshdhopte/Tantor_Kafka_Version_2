package io.translab.tantor.artifact.dto;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Top-level manifest written into an air-gapped bundle (.tar.gz). On import,
 * the target repository walks {@link #entries()} and verifies each checksum
 * before admitting the binary.
 *
 * @param schemaVersion bundle schema version
 * @param generatedAt   when the bundle was exported
 * @param sourceServer  hostname of the exporting server (informational)
 * @param entries       one row per artifact contained in the bundle
 */
public record BundleManifest(
        int schemaVersion,
        OffsetDateTime generatedAt,
        String sourceServer,
        List<Entry> entries
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;

    /**
     * @param entryPath  path of the binary inside the tar (e.g. {@code artifacts/kafka/3.7.0/kafka_2.13-3.7.0.tgz})
     * @param manifestPath path of the per-artifact manifest.json inside the tar
     * @param sha256     expected SHA-256 of the binary
     */
    public record Entry(String entryPath, String manifestPath, String sha256) {
    }
}
