package io.translab.tantor.artifact.web;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;
import java.util.Base64;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Requires a Docker daemon (Testcontainers spins up PostgreSQL 16).
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class ArtifactControllerIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("docker.io/library/postgres:16.14@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b")
                    .withDatabaseName("tantor");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        Path repo = Files.createTempDirectory("tantor-it-repo-");
        registry.add("tantor.repository.base-path", repo::toString);
        registry.add("tantor.cors.allowed-origins", () -> "http://localhost:5173");
    }

    @Autowired
    MockMvc mockMvc;

    @Test
    void rejectsUnauthenticatedUpload() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "kafka_2.13-3.7.0.tgz", "application/gzip", "payload".getBytes());

        mockMvc.perform(multipart("/api/v1/artifacts")
                        .file(file)
                        .param("serviceType", "KAFKA")
                        .param("version", "3.7.0"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void authorizedUploadThenDownloadRoundTrip() throws Exception {
        byte[] payload = kafkaArchive();
        MockMultipartFile file = new MockMultipartFile(
                "file", "kafka_2.13-3.7.0.tgz", "application/gzip", payload);

        String body = mockMvc.perform(multipart("/api/v1/artifacts")
                        .file(file)
                        .param("serviceType", "KAFKA")
                        .param("version", "3.7.0")
                        .header("Authorization", adminAuthorization()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status", is("AVAILABLE")))
                .andExpect(jsonPath("$.serviceType", is("KAFKA")))
                .andReturn().getResponse().getContentAsString();

        String id = body.replaceAll(".*\"id\"\\s*:\\s*\"([0-9a-f\\-]+)\".*", "$1");

        mockMvc.perform(get("/api/v1/artifacts/{id}/download", id))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Checksum-SHA256",
                        org.hamcrest.Matchers.matchesPattern("[0-9a-f]{64}")))
                .andExpect(header().longValue("Content-Length", payload.length));
    }

    private String adminAuthorization() {
        String header = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
        String payload = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"sub\":\"integration-test\",\"roles\":[\"admin\"]}".getBytes(StandardCharsets.UTF_8));
        return "Bearer " + header + "." + payload + ".";
    }

    private byte[] kafkaArchive() throws Exception {
        byte[] jarContents = "test-kafka-jar".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream archive = new ByteArrayOutputStream();
        try (GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(archive);
             TarArchiveOutputStream tar = new TarArchiveOutputStream(gzip)) {
            TarArchiveEntry entry = new TarArchiveEntry("kafka_2.13-3.7.0/libs/kafka_2.13-3.7.0.jar");
            entry.setSize(jarContents.length);
            tar.putArchiveEntry(entry);
            tar.write(jarContents);
            tar.closeArchiveEntry();
            tar.finish();
        }
        return archive.toByteArray();
    }
}
