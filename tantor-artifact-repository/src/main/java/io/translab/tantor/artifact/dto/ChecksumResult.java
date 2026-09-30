package io.translab.tantor.artifact.dto;

/** Computed checksums and byte count for a streamed file. */
public record ChecksumResult(String sha256, String md5, long sizeBytes) {
}
