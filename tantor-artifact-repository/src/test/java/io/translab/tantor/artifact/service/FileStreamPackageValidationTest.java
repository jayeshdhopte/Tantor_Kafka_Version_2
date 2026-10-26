package io.translab.tantor.artifact.service;

import io.translab.tantor.artifact.config.StorageProperties;
import io.translab.tantor.artifact.domain.ServiceType;
import io.translab.tantor.artifact.exception.PackageValidationException;
import org.apache.commons.compress.archivers.tar.*;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class FileStreamPackageValidationTest {
    @TempDir Path temporary;
    Path parcel(Map<String,String> entries) throws Exception {
        Path file=temporary.resolve(UUID.randomUUID()+".tgz");
        try(var gzip=new GzipCompressorOutputStream(Files.newOutputStream(file));var tar=new TarArchiveOutputStream(gzip)) {
            for(var entry:entries.entrySet()) {
                byte[] bytes=entry.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                var header=new TarArchiveEntry(entry.getKey());header.setSize(bytes.length);
                tar.putArchiveEntry(header);tar.write(bytes);tar.closeArchiveEntry();
            }
        }
        return file;
    }
    @Test void rejectsMixedBundledBuildsBeforeMalwareScanOrPublication() throws Exception {
        var scanner=mock(MalwareScanner.class);var validator=new PackageValidator(new StorageProperties(),scanner);
        Path file=parcel(Map.of("kafka/libs/kafka_2.13-4.3.0.jar","runtime",
                "kafka/libs/connect-file-4.3.0.jar","apache",
                "kafka/connect-plugins/filestream-connectors/connect-file-8.1.1-ccs.jar","vendor"));
        assertThatThrownBy(()->validator.validate(file,ServiceType.KAFKA,"4.3.0","parcel.tgz"))
                .isInstanceOf(PackageValidationException.class).hasMessageContaining("Inconsistent FileStream parcel");
        verifyNoInteractions(scanner);
    }
    @Test void acceptsOneBuildAndIdenticalCopiesRegardlessOfTheirNames() throws Exception {
        var scanner=mock(MalwareScanner.class);var validator=new PackageValidator(new StorageProperties(),scanner);
        Path file=parcel(Map.of("kafka/libs/kafka_2.13-4.3.0.jar","runtime",
                "kafka/libs/connect-file-4.3.0.jar","canonical",
                "kafka/connect-plugins/tantor-filestream/connect-file-4.3.0.jar","canonical"));
        validator.validate(file,ServiceType.KAFKA,"4.3.0","parcel.tgz");verify(scanner).scan(file);
    }
    @Test void kafkaOnlyParcelsRemainSupported() throws Exception {
        var validator=new PackageValidator(new StorageProperties(),mock(MalwareScanner.class));
        validator.validate(parcel(Map.of("kafka/libs/kafka_2.13-4.3.0.jar","runtime")),ServiceType.KAFKA,"4.3.0","parcel.tgz");
    }
}
