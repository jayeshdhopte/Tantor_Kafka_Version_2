package io.translab.tantor.server.config;

import jakarta.validation.constraints.Min;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;
import java.util.List;

/** Configurable admission policy, not universal Kafka runtime requirements. */
@Data @Component @Validated @ConfigurationProperties(prefix = "tantor.add-node-readiness")
public class AddNodeReadinessProperties {
    @Min(10) private int ttlSeconds = 120;
    @Min(1) private int timeoutSeconds = 30;
    @Min(1) private int minAvailableMemoryMb = 4096;
    @Min(1) private int minDiskGb = 20;
    @Min(1) private int minInodes = 10000;
    @Min(1) private int maxQuorumLag = 10;
    @Min(1) private int maxTopics = 1000;
    private List<Integer> allowedJavaMajors = List.of(17, 21);
    private List<String> clientProbeHostIds = List.of();
    // Explicit candidate profile; never changes the runtime of existing services.
    private String candidateRuntimeUser = "";
    private String candidateJavaHome = "";
    private String candidateLogPath = "";
    private List<String> candidateDataPaths = List.of();
    private List<String> supportedKafkaVersions = List.of("4.1.0", "4.1.1", "4.1.2", "4.3.0");
}
