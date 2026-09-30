package io.translab.tantor.artifact.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Strongly typed binding for the {@code tantor.repository.*} configuration.
 */
@Validated
@ConfigurationProperties(prefix = "tantor.repository")
public class StorageProperties {

    /** Filesystem root under which the {@code /artifacts} tree lives. */
    @NotBlank
    private String basePath = "/var/lib/tantor/repository";

    /** When true, a declared checksum that disagrees with the computed one fails the upload. */
    private boolean enforceChecksum = true;

    /** Buffer size for streaming file IO. */
    @Min(64 * 1024)
    private int streamBufferBytes = 1024 * 1024;

    @Min(1)
    private long maxUploadBytes = 536_870_912L;

    @Min(1)
    private long maxBundleBytes = 536_870_912L;

    @Min(1)
    private long minimumFreeSpaceBytes = 5_368_709_120L;

    @Min(1)
    private int maxConcurrentUploads = 2;

    @Min(1)
    private int maxArchiveEntries = 10_000;

    @Min(1)
    private long maxArchiveEntryBytes = 1_073_741_824L;

    @Min(1)
    private long maxArchiveExpandedBytes = 2_147_483_648L;

    @Min(1)
    private int maxArchiveCompressionRatio = 100;

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public boolean isEnforceChecksum() {
        return enforceChecksum;
    }

    public void setEnforceChecksum(boolean enforceChecksum) {
        this.enforceChecksum = enforceChecksum;
    }

    public int getStreamBufferBytes() {
        return streamBufferBytes;
    }

    public void setStreamBufferBytes(int streamBufferBytes) {
        this.streamBufferBytes = streamBufferBytes;
    }

    public long getMaxUploadBytes() { return maxUploadBytes; }
    public void setMaxUploadBytes(long maxUploadBytes) { this.maxUploadBytes = maxUploadBytes; }
    public long getMaxBundleBytes() { return maxBundleBytes; }
    public void setMaxBundleBytes(long maxBundleBytes) { this.maxBundleBytes = maxBundleBytes; }
    public long getMinimumFreeSpaceBytes() { return minimumFreeSpaceBytes; }
    public void setMinimumFreeSpaceBytes(long minimumFreeSpaceBytes) { this.minimumFreeSpaceBytes = minimumFreeSpaceBytes; }
    public int getMaxConcurrentUploads() { return maxConcurrentUploads; }
    public void setMaxConcurrentUploads(int maxConcurrentUploads) { this.maxConcurrentUploads = maxConcurrentUploads; }
    public int getMaxArchiveEntries() { return maxArchiveEntries; }
    public void setMaxArchiveEntries(int maxArchiveEntries) { this.maxArchiveEntries = maxArchiveEntries; }
    public long getMaxArchiveEntryBytes() { return maxArchiveEntryBytes; }
    public void setMaxArchiveEntryBytes(long maxArchiveEntryBytes) { this.maxArchiveEntryBytes = maxArchiveEntryBytes; }
    public long getMaxArchiveExpandedBytes() { return maxArchiveExpandedBytes; }
    public void setMaxArchiveExpandedBytes(long maxArchiveExpandedBytes) { this.maxArchiveExpandedBytes = maxArchiveExpandedBytes; }
    public int getMaxArchiveCompressionRatio() { return maxArchiveCompressionRatio; }
    public void setMaxArchiveCompressionRatio(int maxArchiveCompressionRatio) { this.maxArchiveCompressionRatio = maxArchiveCompressionRatio; }
}
