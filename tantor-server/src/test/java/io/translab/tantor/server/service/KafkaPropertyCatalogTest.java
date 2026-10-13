package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class KafkaPropertyCatalogTest {
    @Test void catalogUsesOnlySharedRecognizedNonSensitiveDefinitions() {
        var first = Map.<String, Object>of("controller.quorum.fetch.timeout.ms", Map.of("isSensitive", false),
                "metadata.max.idle.interval.ms", Map.of("isSensitive", false), "num.network.threads", Map.of("isSensitive", false),
                "log.retention.hours", Map.of("isSensitive", false), "ssl.keystore.location", Map.of("isSensitive", false),
                "controller.secret.setting", Map.of("isSensitive", false), "controller.sensitive.setting", Map.of("isSensitive", true),
                "node.id", Map.of("isSensitive", false), "controller.version.only.setting", Map.of("isSensitive", false));
        var second = new java.util.HashMap<>(first); second.remove("controller.version.only.setting");
        assertThat(KafkaPropertyCatalog.catalog(Map.of(1, first, 2, second)).get("controller"))
                .containsExactly("controller.quorum.fetch.timeout.ms", "metadata.max.idle.interval.ms", "num.network.threads");
        assertThat(KafkaPropertyCatalog.catalog(Map.of()).get("controller")).isEmpty();
    }
    @Test void manuallySubmittedUnknownAndNativeSensitiveNamesAreRejected() {
        var live = Map.of(1, Map.<String, Object>of("controller.quorum.fetch.timeout.ms", Map.of("isSensitive", false),
                "controller.sensitive.setting", Map.of("isSensitive", true)));
        KafkaPropertyCatalog.requireRecognizedControllerSetting("controller.quorum.fetch.timeout.ms", live);
        assertThatThrownBy(() -> KafkaPropertyCatalog.requireRecognizedControllerSetting("controller.typo", live)).hasMessageContaining("supported");
        assertThatThrownBy(() -> KafkaPropertyCatalog.requireRecognizedControllerSetting("controller.sensitive.setting", live)).hasMessageContaining("non-sensitive");
        assertThat(KafkaPropertyCatalog.editableName("listeners")).isFalse();
        assertThat(KafkaPropertyCatalog.editableName("ssl.truststore.location")).isFalse();
    }
}
