package io.translab.tantor.artifact.domain;

/**
 * Lifecycle of an artifact record.
 *
 * <pre>
 *   UPLOADING --(bytes written + checksum verified)--> AVAILABLE
 *   AVAILABLE --(integrity re-check fails)-----------> CORRUPTED
 *   AVAILABLE --(operator action)--------------------> QUARANTINED
 *   any       --(soft delete)------------------------> DELETED
 * </pre>
 */
public enum ArtifactStatus {
    /** Row exists but bytes are still being written / verified. */
    UPLOADING,
    /** Fully written, checksum verified, downloadable by agents. */
    AVAILABLE,
    /** On-disk bytes no longer match the recorded checksum. */
    CORRUPTED,
    /** Manually withheld from deployment (e.g. CVE found). */
    QUARANTINED,
    /** Soft-deleted; file removed from disk, row retained for audit. */
    DELETED,
    /** Package validation failed (e.g., checksum mismatch, corruption). */
    FAILED,
    /** Version is obsolete and no longer recommended. */
    DEPRECATED
}
