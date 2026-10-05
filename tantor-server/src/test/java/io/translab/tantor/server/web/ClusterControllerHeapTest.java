package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterControllerHeapTest {

    @Test
    void acceptsSeparateMinimumAndMaximumAndLegacySingleSize() {
        assertThat(ClusterController.heapRangeError("6G", "4G", "6G")).isNull();
        assertThat(ClusterController.heapRangeError("2G", "1024M", "2G")).isNull();
        assertThat(ClusterController.heapRangeError("1G", null, null)).isNull();
    }

    @Test
    void rejectsInvalidOrReversedHeapRange() {
        assertThat(ClusterController.heapRangeError("6G", "6G", "4G"))
                .isEqualTo("Min Heap cannot exceed Max Heap.");
        assertThat(ClusterController.heapRangeError(null, "1G", null))
                .contains("Min Heap and Max Heap");
        assertThat(ClusterController.heapRangeError("6G", "invalid", "6G"))
                .contains("Min Heap and Max Heap");
    }

    @Test
    void readsHeapRangeFromDeploymentRequest() throws Exception {
        ClusterController.ServiceAssignmentReq service = new ObjectMapper().readValue(
                "{\"heap_size\":\"6G\",\"heap_xms\":\"4G\",\"heap_xmx\":\"6G\"}",
                ClusterController.ServiceAssignmentReq.class);

        assertThat(service.getHeap_size()).isEqualTo("6G");
        assertThat(service.getHeap_xms()).isEqualTo("4G");
        assertThat(service.getHeap_xmx()).isEqualTo("6G");
    }
}
