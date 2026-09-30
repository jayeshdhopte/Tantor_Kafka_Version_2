package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationSanitizerTest {

    private final ConfigurationSanitizer sanitizer = new ConfigurationSanitizer(new ObjectMapper());

    @Test
    void redactsSensitiveValuesAtEveryNestedConfigurationLevel() {
        Map<String, Object> safe = sanitizer.sanitize(Map.of(
                "broker", Map.of("ssl.keystore.password", "do-not-return-this"),
                "sasl.jaas.config", "org.example.Login required password=secret;",
                "listeners", List.of("SASL_SSL://broker:9093"),
                "retention.ms", "604800000"));

        Map<?, ?> broker = (Map<?, ?>) safe.get("broker");
        assertThat(broker.get("ssl.keystore.password")).isEqualTo("[REDACTED]");
        assertThat(safe.get("sasl.jaas.config")).isEqualTo("[REDACTED]");
        assertThat(safe.get("retention.ms")).isEqualTo("604800000");
        assertThat(((List<?>) safe.get("listeners")).get(0)).isEqualTo("SASL_SSL://broker:9093");
    }
}
