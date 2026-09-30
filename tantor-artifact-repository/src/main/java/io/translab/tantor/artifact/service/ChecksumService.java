package io.translab.tantor.artifact.service;

import io.translab.tantor.artifact.dto.ChecksumResult;
import io.translab.tantor.artifact.exception.StorageException;
import io.translab.tantor.artifact.exception.UploadLimitExceededException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Computes SHA-256 and MD5 over a stream in a single pass, optionally while
 * copying the bytes to a destination. Doing both digests during the same read
 * means a multi-hundred-megabyte Kafka tarball is hashed without ever being
 * fully buffered in memory or read twice.
 */
@Service
public class ChecksumService {

    private static final int BUFFER = 1024 * 1024;

    /**
     * Read {@code source} to completion, copy it to {@code sink}, and return the
     * digests of everything that flowed through.
     */
    public ChecksumResult copyAndDigest(InputStream source, OutputStream sink) {
        return copyAndDigest(source, sink, Long.MAX_VALUE);
    }

    public ChecksumResult copyAndDigest(InputStream source, OutputStream sink, long maxBytes) {
        MessageDigest sha256 = newDigest("SHA-256");
        MessageDigest md5 = newDigest("MD5");
        long total = 0;
        byte[] buf = new byte[BUFFER];
        try {
            int read;
            while ((read = source.read(buf)) != -1) {
                if (read > maxBytes - total) {
                    throw new UploadLimitExceededException("Upload exceeds the configured maximum of " + maxBytes + " bytes");
                }
                sha256.update(buf, 0, read);
                md5.update(buf, 0, read);
                sink.write(buf, 0, read);
                total += read;
            }
            sink.flush();
        } catch (IOException e) {
            throw new StorageException("Failed while streaming and digesting bytes", e);
        }
        return new ChecksumResult(hex(sha256.digest()), hex(md5.digest()), total);
    }

    /** Digest an existing stream without copying it anywhere (used for re-verification). */
    public ChecksumResult digest(InputStream source) {
        return copyAndDigest(source, OutputStream.nullOutputStream());
    }

    private MessageDigest newDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new StorageException(algorithm + " not available in this JVM", e);
        }
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
