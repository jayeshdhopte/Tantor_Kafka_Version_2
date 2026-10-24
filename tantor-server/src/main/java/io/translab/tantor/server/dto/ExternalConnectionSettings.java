package io.translab.tantor.server.dto;

import lombok.Data;
import java.util.List;

/** Options for browsing/monitoring existing Kafka; never provisions services. */
@Data
public class ExternalConnectionSettings {
    private Metrics metrics;
    private List<Mask> masking = List.of();

    @Data public static class Metrics {
        private boolean enabled = true;
        private Integer brokerPort = 7071;
        private Integer controllerPort = 7072;
    }
    @Data public static class Mask {
        private String type = "MASK";
        private List<String> fields = List.of();
        private String fieldsPattern;
        private String topicKeysPattern;
        private String topicValuesPattern = ".*";
        private String replacement = "***";
    }
    @Data public static class Request {
        private String bootstrapServers;
        private ExternalConnectionSettings settings = new ExternalConnectionSettings();
        private List<SaveConnectionRequest> schemaRegistry = List.of();
        private List<SaveConnectionRequest> kafkaConnect = List.of();
    }
}
