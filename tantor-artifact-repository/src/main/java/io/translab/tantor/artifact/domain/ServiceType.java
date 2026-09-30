package io.translab.tantor.artifact.domain;

import java.util.Arrays;

/**
 * The ecosystem services Tantor can deploy. Each maps to a sub-directory under
 * {@code /artifacts} in the on-disk repository layout.
 */
public enum ServiceType {

    KAFKA("kafka"),
    KAFKA_CONNECT("connect"),
    SCHEMA_REGISTRY("schema-registry"),
    KSQLDB("ksqldb"),
    CRUISE_CONTROL("cruise-control"),
    JMX_EXPORTER("jmx-exporter"),
    KAFKA_EXPORTER("kafka-exporter"),
    PROMETHEUS("prometheus"),
    GRAFANA("grafana");

    private final String directory;

    ServiceType(String directory) {
        this.directory = directory;
    }

    /** Sub-directory name under the repository root, e.g. {@code kafka}. */
    public String directory() {
        return directory;
    }

    public static ServiceType fromDirectory(String dir) {
        return Arrays.stream(values())
                .filter(s -> s.directory.equalsIgnoreCase(dir))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown service directory: " + dir));
    }
}
