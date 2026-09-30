package io.translab.tantor.artifact.service;

import io.translab.tantor.artifact.config.StorageProperties;
import io.translab.tantor.artifact.domain.Artifact;
import io.translab.tantor.artifact.domain.ArtifactStatus;
import io.translab.tantor.artifact.domain.ServiceType;
import io.translab.tantor.artifact.dto.ChecksumResult;
import io.translab.tantor.artifact.dto.ManifestDto;
import io.translab.tantor.artifact.exception.ChecksumMismatchException;
import io.translab.tantor.artifact.exception.ArtifactAlreadyExistsException;
import io.translab.tantor.artifact.repository.ArtifactJpaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ArtifactServiceTest {

    @Mock ArtifactJpaRepository repository;
    @Mock StorageService storageService;
    @Mock ManifestService manifestService;

    @Mock PackageValidator packageValidator;

    StorageProperties properties;
    ArtifactService service;

    @BeforeEach
    void setUp() {
        properties = new StorageProperties();
        properties.setEnforceChecksum(true);
        service = new ArtifactService(repository,
                storageService, manifestService, properties, packageValidator);

        lenient().when(storageService.relativeDir(any(), anyString(), any()))
                .thenReturn("kafka/3.7.0");
        lenient().when(manifestService.build(any(), any()))
                .thenReturn(new ManifestDto(1, ServiceType.KAFKA, "kafka", "3.7.0", null,
                        "kafka_2.13-3.7.0.tgz", 100L, "sha", "md5", "application/gzip", null, Map.of()));
        lenient().when(manifestService.toJson(any())).thenReturn("{}");
        lenient().when(repository.save(any(Artifact.class))).thenAnswer(inv -> inv.getArgument(0));
        lenient().when(storageService.resolveBinary(anyString(), anyString())).thenReturn(Path.of("repository", "artifact.tgz"));
    }

    @Test
    void uploadStoresAndMarksAvailable() {
        when(storageService.storeTemporarily(eq("kafka_2.13-3.7.0.tgz"), any(InputStream.class)))
                .thenReturn(new StorageService.TempStoreResult(Path.of("temp"), new ChecksumResult("abc123", "md5val", 100L)));

        Artifact result = service.upload(cmd(null, false), data());

        assertThat(result.getStatus()).isEqualTo(ArtifactStatus.AVAILABLE);
        assertThat(result.getChecksumSha256()).isEqualTo("abc123");
        assertThat(result.getRelativePath()).startsWith("kafka/3.7.0/")
                .endsWith("/kafka_2.13-3.7.0.tgz");
        verify(storageService).writeManifest(eq("kafka/3.7.0/" + result.getId()), anyString());
    }

    @Test
    void repeatedUploadIsRejected() {
        when(storageService.storeTemporarily(eq("kafka_2.13-3.7.0.tgz"), any(InputStream.class)))
                .thenReturn(new StorageService.TempStoreResult(Path.of("temp"), new ChecksumResult("abc123", "md5val", 100L)));

        when(repository.countActiveByServiceTypeAndVersion(ServiceType.KAFKA, "3.7.0"))
                .thenReturn(1L);

        assertThatThrownBy(() -> service.upload(cmd(null, false), data()))
                .isInstanceOf(ArtifactAlreadyExistsException.class);
        verify(repository, never()).save(any(Artifact.class));
    }

    @Test
    void uploadFailsAndCleansUpOnChecksumMismatch() {
        when(storageService.storeTemporarily(any(), any()))
                .thenReturn(new StorageService.TempStoreResult(Path.of("temp"), new ChecksumResult("computed-real", "md5", 100L)));

        assertThatThrownBy(() -> service.upload(cmd("declared-wrong", false), data()))
                .isInstanceOf(ChecksumMismatchException.class);

        verify(storageService, times(1)).deleteTemp(any());
        verify(repository, never()).save(any());
    }

    @Test
    void unsafeFilenameIsRejectedBeforeAnyUploadIsStored() {
        ArtifactService.UploadCommand unsafe = new ArtifactService.UploadCommand(
                ServiceType.KAFKA, "kafka", "3.7.0", null, null, "../malicious.tgz",
                "application/gzip", "test", null, Map.of(), false, "tester");

        assertThatThrownBy(() -> service.upload(unsafe, data()))
                .isInstanceOf(IllegalArgumentException.class);
        verify(storageService, never()).storeTemporarily(anyString(), any());
    }

    private ArtifactService.UploadCommand cmd(String declaredSha, boolean overwrite) {
        return new ArtifactService.UploadCommand(
                ServiceType.KAFKA, "kafka", "3.7.0", null, null, "kafka_2.13-3.7.0.tgz",
                "application/gzip", "test", declaredSha, Map.of(), overwrite, "tester");
    }

    private InputStream data() {
        return new ByteArrayInputStream("payload".getBytes());
    }
}
