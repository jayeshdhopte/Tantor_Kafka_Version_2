package io.translab.tantor.server.domain.canonical;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CanonicalContractTest {

    @Test
    void identityUsesOnlyClusterKafkaNodeAndRole() {
        UUID clusterUuid = UUID.randomUUID();
        CanonicalNodeIdentity identity = new CanonicalNodeIdentity(
                clusterUuid, "kafka-cluster-1", 2, CanonicalNodeRole.BROKER);

        CanonicalNodeContract firstObservation = new CanonicalNodeContract(
                identity, "192.168.3.164", CanonicalAgentStatus.ONLINE,
                CanonicalTelemetryStatus.LIVE);
        CanonicalNodeContract laterObservation = new CanonicalNodeContract(
                identity, "broker-2.example.test", CanonicalAgentStatus.OFFLINE,
                CanonicalTelemetryStatus.STALE);

        assertEquals(firstObservation.identity(), laterObservation.identity());
        assertNotEquals(firstObservation.host(), laterObservation.host());
        assertNotEquals(firstObservation.agentStatus(), laterObservation.agentStatus());
    }

    @Test
    void identityRejectsIncompleteKafkaIdentity() {
        UUID clusterUuid = UUID.randomUUID();

        assertThrows(IllegalArgumentException.class, () ->
                new CanonicalNodeIdentity(clusterUuid, " ", 1, CanonicalNodeRole.BROKER));
        assertThrows(IllegalArgumentException.class, () ->
                new CanonicalNodeIdentity(clusterUuid, "kafka-cluster-1", 1,
                        CanonicalNodeRole.UNKNOWN));
        assertThrows(IllegalArgumentException.class, () ->
                new CanonicalNodeIdentity(clusterUuid, "kafka-cluster-1", -1,
                        CanonicalNodeRole.BROKER));
    }

    @Test
    void clusterAllowsUnknownModeAndPendingKafkaId() {
        UUID clusterUuid = UUID.randomUUID();

        CanonicalClusterContract contract = new CanonicalClusterContract(
                clusterUuid, null, CanonicalClusterType.INTERNAL, CanonicalKafkaMode.UNKNOWN);

        assertEquals(clusterUuid, contract.clusterUuid());
        assertEquals(CanonicalKafkaMode.UNKNOWN, contract.mode());
    }
}
