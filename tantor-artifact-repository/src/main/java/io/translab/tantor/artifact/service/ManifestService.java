package io.translab.tantor.artifact.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.artifact.domain.Artifact;
import io.translab.tantor.artifact.dto.ManifestDto;
import io.translab.tantor.artifact.exception.StorageException;
import org.springframework.stereotype.Service;

import java.util.Map;

/** Builds and (de)serializes artifact manifests. */
@Service
public class ManifestService {

    private final ObjectMapper objectMapper;

    public ManifestService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ManifestDto build(Artifact a, Map<String, String> attributes) {
        return new ManifestDto(
                ManifestDto.CURRENT_SCHEMA_VERSION,
                a.getServiceType(),
                a.getFileName(),
                a.getVersion(),
                null,
                a.getFileName(),
                a.getFileSizeBytes(),
                a.getChecksumSha256(),
                a.getChecksumMd5(),
                a.getContentType(),
                a.getCreatedAt(),
                attributes == null ? Map.of() : attributes
        );
    }

    public String toJson(ManifestDto manifest) {
        try {
            return objectMapper.writeValueAsString(manifest);
        } catch (JsonProcessingException e) {
            throw new StorageException("Failed to serialize manifest", e);
        }
    }

    public ManifestDto fromJson(String json) {
        try {
            return objectMapper.readValue(json, ManifestDto.class);
        } catch (JsonProcessingException e) {
            throw new StorageException("Failed to parse manifest", e);
        }
    }
}
