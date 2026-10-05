package io.translab.tantor.server.web;

import io.translab.tantor.server.service.NodeNameConflictException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;

class ApiExceptionHandlerNodeNameTest {
    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    void duplicateNodeNameReturnsClearConflict() {
        var response = handler.handleNodeNameConflict(new NodeNameConflictException("kafka-node-1"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().getErrorCode()).isEqualTo("NODE_NAME_CONFLICT");
        assertThat(response.getBody().getMessage()).contains("kafka-node-1", "different host-id");
    }

    @Test
    void uniqueIndexRaceReturnsSameConflict() {
        var response = handler.handleDataIntegrity(new DataIntegrityViolationException(
                "duplicate key value violates unique constraint ux_kf_discovery_agents_node_name_ci"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().getErrorCode()).isEqualTo("NODE_NAME_CONFLICT");
        assertThat(response.getBody().getMessage()).contains("different host-id");
    }
}
