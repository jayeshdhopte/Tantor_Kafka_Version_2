package io.translab.tantor.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.List;

/** Candidate defaults follow add_host/group_vars/all.yml; all paths are checked on the actual host. */
@Component @Data @ConfigurationProperties(prefix="tantor.add-node-expansion")
public class KafkaExpansionProperties {
    private boolean executionEnabled=false;
    private List<String> controllerDataPaths=List.of("/data/kafka/controller-data/data");
    private String controllerMetadataPath="/data/kafka/controller-data/metadata";
    private String controllerLogPath="/var/log/tantor-kafka/controller";
    private String controllerHeapXmx="2g";
    private int controllerJmxPort=0;
    private int brokerJmxPort=0;
    private long osReserveMb=2048;
    private int registrationTimeoutSeconds=180;
    private int stableSamples=3;
    private int stabilityPollSeconds=10;
}
