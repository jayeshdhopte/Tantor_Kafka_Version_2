package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import java.util.*;

/** One target per operating-system process; combined roles cannot be split. */
public final class RollingRestartPlan {
    private RollingRestartPlan() {}
    public static String key(ClusterServiceAssignment service) {
        return service.getHostId() + ":" + unit(service);
    }
    public static String unit(ClusterServiceAssignment service) {
        if(service.getHostId().startsWith("external:")) { try { return new com.fasterxml.jackson.databind.ObjectMapper().readTree(service.getConfigJson()).get("service_name").asText(); } catch(Exception e){throw new IllegalArgumentException("Discovered service name missing",e);} }
        return unit(service.getRole());
    }
    public static String unit(String role) {
        return switch (role) {
            case "controller" -> "controller";
            case "broker" -> "broker";
            case "broker_controller", "broker_zookeeper" -> "kafka";
            default -> throw new IllegalArgumentException("Unsupported Kafka process: " + role);
        };
    }
    public static boolean broker(ClusterServiceAssignment service) {
        return Set.of("broker", "broker_controller", "broker_zookeeper").contains(service.getRole());
    }
    public static boolean controller(ClusterServiceAssignment service) {
        return Set.of("controller", "broker_controller").contains(service.getRole());
    }
    public static List<ClusterServiceAssignment> topology(Cluster cluster) {
        Map<String, ClusterServiceAssignment> unique = new LinkedHashMap<>();
        for (var service : cluster.getServices() == null ? List.<ClusterServiceAssignment>of() : cluster.getServices()) {
            if (broker(service) || controller(service)) unique.putIfAbsent(key(service), service);
        }
        return new ArrayList<>(unique.values());
    }
    public static List<ClusterServiceAssignment> select(Cluster cluster, String scope, List<String> targets) {
        String normalized = scope == null ? "BROKERS" : scope.toUpperCase(Locale.ROOT);
        var topology = topology(cluster);
        if ("SELECTED".equals(normalized)) {
            if (targets == null || targets.isEmpty()) throw new IllegalArgumentException("Select at least one Kafka process.");
            Set<String> available = new HashSet<>();
            topology.forEach(service -> available.add(key(service)));
            if (!available.containsAll(targets)) throw new IllegalArgumentException("Selected process no longer belongs to this cluster. Reload the topology.");
        }
        var selected = topology.stream().filter(service -> switch (normalized) {
            case "ALL" -> true;
            case "BROKERS" -> broker(service);
            case "CONTROLLERS" -> controller(service);
            case "SELECTED" -> targets.contains(key(service));
            default -> throw new IllegalArgumentException("Scope must be BROKERS, CONTROLLERS, ALL or SELECTED.");
        }).toList();
        if (selected.isEmpty()) throw new IllegalArgumentException("No Kafka processes match this scope.");
        return selected;
    }
}
