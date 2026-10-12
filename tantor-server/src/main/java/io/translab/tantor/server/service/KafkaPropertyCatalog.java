package io.translab.tantor.server.service;

import java.util.*;

/** Uses DescribeConfigs from the running cluster, including default-valued entries. */
public final class KafkaPropertyCatalog {
    private KafkaPropertyCatalog() {}
    private static final Set<String> PROTECTED = Set.of("node.id", "broker.id", "process.roles", "log.dirs",
            "metadata.log.dir", "listeners", "advertised.listeners", "controller.quorum.voters",
            "controller.quorum.bootstrap.servers", "inter.broker.listener.name", "cluster.id", "zookeeper.connect");
    private static final Set<String> SHARED_CONTROLLER = Set.of("num.io.threads", "num.network.threads",
            "default.replication.factor", "num.partitions", "auto.leader.rebalance.enable",
            "leader.imbalance.check.interval.seconds", "unclean.leader.election.enable",
            "unclean.leader.election.interval.ms", "broker.session.timeout.ms", "server.max.startup.time.ms",
            "authorizer.class.name", "early.start.listeners", "create.topic.policy.class.name", "alter.config.policy.class.name");

    public static boolean editableName(String key) {
        return key != null && key.matches("[A-Za-z][A-Za-z0-9._-]{0,127}") && !PROTECTED.contains(key)
                && !key.toLowerCase(Locale.ROOT).matches(".*(password|secret|token|jaas|credential|keystore|truststore).*");
    }

    public static boolean controllerSetting(String key) {
        return SHARED_CONTROLLER.contains(key) || key.startsWith("controller.") || key.startsWith("metadata.")
                || key.startsWith("socket.") || key.startsWith("connections.") || key.startsWith("connection.")
                || key.startsWith("request.") || key.startsWith("metrics.") || key.startsWith("metric.");
    }

    public static Map<String, List<String>> catalog(Map<Integer, Map<String, Object>> live) {
        if (live.isEmpty()) return Map.of("controller", List.of());
        List<String> controllers = live.values().iterator().next().keySet().stream()
                .filter(KafkaPropertyCatalog::editableName).filter(KafkaPropertyCatalog::controllerSetting)
                .filter(key -> live.values().stream().allMatch(config -> config.get(key) instanceof Map<?, ?> entry
                        && !Boolean.TRUE.equals(entry.get("isSensitive"))))
                .sorted().toList();
        return Map.of("controller", controllers);
    }

    public static void requireRecognizedControllerSetting(String key, Map<Integer, Map<String, Object>> live) {
        // Existing API callers may use any recognized Kafka definition; UI suggestions use the role-specific list.
        if (live.isEmpty() || live.values().stream().anyMatch(config -> !(config.get(key) instanceof Map<?, ?> entry)
                || Boolean.TRUE.equals(entry.get("isSensitive"))))
            throw new IllegalArgumentException("Kafka does not report a supported non-sensitive property " + key + " on every broker. Refresh the configuration catalog.");
    }
}
