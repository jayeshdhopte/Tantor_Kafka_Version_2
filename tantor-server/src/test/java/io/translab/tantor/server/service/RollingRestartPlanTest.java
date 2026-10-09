package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class RollingRestartPlanTest {
    private ClusterServiceAssignment service(String host, String role, int node) {
        var service = new ClusterServiceAssignment();
        service.setHostId(host); service.setRole(role); service.setNodeId(node); return service;
    }
    @Test void filtersAddonsAndRestartsCombinedProcessOnce() {
        Cluster cluster = new Cluster();
        var combined = service("one", "broker_controller", 1);
        cluster.setServices(List.of(combined, combined, service("two", "controller", 101),
                service("two", "broker", 2), service("one", "schema-registry", 0), service("two", "kafka-connect", 0)));
        assertThat(RollingRestartPlan.select(cluster, "ALL", List.of())).hasSize(3);
        assertThat(RollingRestartPlan.select(cluster, "BROKERS", List.of())).extracting(ClusterServiceAssignment::getRole)
                .containsExactly("broker_controller", "broker");
        assertThat(RollingRestartPlan.select(cluster, "CONTROLLERS", List.of())).extracting(ClusterServiceAssignment::getRole)
                .containsExactly("broker_controller", "controller");
    }
    @Test void selectedProcessesDistinguishBrokerAndControllerOnSameHost() {
        Cluster cluster = new Cluster();
        cluster.setServices(List.of(service("one", "controller", 101), service("one", "broker", 1)));
        assertThat(RollingRestartPlan.select(cluster, "SELECTED", List.of("one:broker"))).extracting(ClusterServiceAssignment::getRole).containsExactly("broker");
        assertThatThrownBy(() -> RollingRestartPlan.select(cluster, "SELECTED", List.of("two:broker"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RollingRestartPlan.select(cluster, "SELECTED", List.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
