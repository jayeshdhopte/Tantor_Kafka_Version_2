package io.translab.tantor.server.domain.canonical;

import java.util.List;
import java.util.Objects;

public record CanonicalClusterNodesResponse(
        CanonicalClusterContract cluster,
        List<CanonicalNodeContract> nodes) {

    public CanonicalClusterNodesResponse {
        Objects.requireNonNull(cluster, "cluster must not be null");
        nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes must not be null"));
    }
}
