package io.translab.tantor.artifact.config;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorsPropertiesTest {
    @Test
    void rejectsMissingAndMalformedOrigins() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(new CorsProperties())).isNotEmpty();
            CorsProperties malformed = new CorsProperties();
            malformed.setAllowedOrigins(List.of("not-a-url"));
            assertThat(validator.validate(malformed)).isNotEmpty();
        }
    }

    @Test
    void productionRejectsHttpAndLoopbackOrigins() {
        CorsProperties cors = new CorsProperties();
        cors.setAllowedOrigins(List.of("http://localhost:5173"));
        StorageProperties storage = new StorageProperties();
        MockEnvironment environment = new MockEnvironment();
        environment.setActiveProfiles("production");
        var diagnostics = new ArtifactConfigurationDiagnostics(cors, storage, new MalwareScanProperties(), environment);
        assertThatThrownBy(diagnostics::afterPropertiesSet).isInstanceOf(IllegalStateException.class);
    }
}
