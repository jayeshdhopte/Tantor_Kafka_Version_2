package io.translab.tantor.server.web;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterControllerAgentConnectivityTest {

    @Test
    void allAgentsConnectedIsGreenConnectedState() {
        var state = ClusterController.agentConnectivityState(3, 3);

        assertThat(state.health()).isEqualTo("CONNECTED");
        assertThat(state.label()).isEqualTo("Agent Connected");
        assertThat(state.telemetry()).isEqualTo("Full");
    }

    @Test
    void someAgentsConnectedIsYellowPartialState() {
        var state = ClusterController.agentConnectivityState(2, 3);

        assertThat(state.health()).isEqualTo("PARTIAL");
        assertThat(state.label()).isEqualTo("Partially Connected");
        assertThat(state.telemetry()).isEqualTo("Partial");
    }

    @Test
    void noAgentsConnectedIsRedNotConnectedState() {
        var state = ClusterController.agentConnectivityState(0, 3);

        assertThat(state.health()).isEqualTo("NOT_CONNECTED");
        assertThat(state.label()).isEqualTo("Agent Not Connected");
        assertThat(state.telemetry()).isEqualTo("None");
    }
}
