package io.translab.tantor.artifact.service;

import io.translab.tantor.artifact.dto.ChecksumResult;
import io.translab.tantor.artifact.exception.UploadLimitExceededException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChecksumServiceTest {

    private final ChecksumService service = new ChecksumService();

    @Test
    void computesKnownDigestsForHello() {
        byte[] input = "hello".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream sink = new ByteArrayOutputStream();

        ChecksumResult result = service.copyAndDigest(new ByteArrayInputStream(input), sink);

        assertThat(result.sha256())
                .isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
        assertThat(result.md5()).isEqualTo("5d41402abc4b2a76b9719d911017c592");
        assertThat(result.sizeBytes()).isEqualTo(5);
        assertThat(sink.toByteArray()).isEqualTo(input);
    }

    @Test
    void digestOnlyDoesNotRequireASink() {
        byte[] input = new byte[3 * 1024 * 1024]; // 3 MB of zeros, forces multi-buffer reads
        ChecksumResult result = service.digest(new ByteArrayInputStream(input));
        assertThat(result.sizeBytes()).isEqualTo(input.length);
        assertThat(result.sha256()).hasSize(64);
    }

    @Test
    void rejectsAStreamThatExceedsTheConfiguredLimit() {
        assertThatThrownBy(() -> service.copyAndDigest(
                new ByteArrayInputStream(new byte[6]), new ByteArrayOutputStream(), 5))
                .isInstanceOf(UploadLimitExceededException.class);
    }
}
