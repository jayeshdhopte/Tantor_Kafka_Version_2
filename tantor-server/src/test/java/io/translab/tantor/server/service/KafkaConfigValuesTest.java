package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaConfigValuesTest {
    @Test void numericByteLimitsMatchKafkasDecimalReadback() {
        assertThat(KafkaConfigValues.canonical("message.max.bytes", " 15728679")).isEqualTo("15728679");
        assertThat(KafkaConfigValues.same("log.segment.bytes", "+01073741879", "1073741879")).isTrue();
        assertThat(KafkaConfigValues.same("message.max.bytes", " 15728679", "15728688")).isFalse();
    }
    @Test void preservesStringWhitespaceNullAndInvalidInputForValidation() {
        assertThat(KafkaConfigValues.same("broker.rack", " rack1", "rack1")).isFalse();
        assertThat(KafkaConfigValues.canonical("message.max.bytes", "invalid")).isEqualTo("invalid");
        assertThat(KafkaConfigValues.same("message.max.bytes", null, "0")).isFalse();
    }
}
