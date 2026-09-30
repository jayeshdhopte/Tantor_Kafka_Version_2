package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.canonical.CanonicalClusterContract;
import io.translab.tantor.server.domain.canonical.CanonicalClusterNodesResponse;
import io.translab.tantor.server.domain.canonical.CanonicalClusterType;
import io.translab.tantor.server.domain.canonical.CanonicalKafkaMode;
import io.translab.tantor.server.service.CanonicalClusterNodeResolver;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusterReadCompatibilityControllerCanonicalNodesTest {

    @Test
    void canonicalNodesEndpointReturnsResolverResponse() {
        ClusterController clusterController = mock(ClusterController.class);
        CanonicalClusterNodeResolver resolver = mock(CanonicalClusterNodeResolver.class);
        ClusterReadCompatibilityController controller =
                new ClusterReadCompatibilityController(clusterController, resolver);
        UUID clusterUuid = UUID.randomUUID();
        CanonicalClusterNodesResponse expected = new CanonicalClusterNodesResponse(
                new CanonicalClusterContract(
                        clusterUuid,
                        "kafka-cluster-id",
                        CanonicalClusterType.INTERNAL,
                        CanonicalKafkaMode.KRAFT),
                List.of());
        when(resolver.resolve(clusterUuid)).thenReturn(expected);

        var response = controller.nodes(clusterUuid);

        assertThat(response.getBody()).isSameAs(expected);
        verify(resolver).resolve(clusterUuid);
    }
}
