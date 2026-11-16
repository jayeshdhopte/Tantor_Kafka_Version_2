package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

class ExternalBrokerRecoveryTest {
  static ObjectNode physical(String phase, String storage, boolean active) {
    return new ObjectMapper()
        .createObjectNode()
        .put("phase", phase)
        .put("storageState", storage)
        .put("serviceActive", active)
        .put("storageEmpty", "MISSING".equals(storage))
        .put("formatterObservationComplete", true)
        .put("formatterActive", false)
        .put("runtimeVerified", true)
        .put("processVerified", true);
  }

  @Test
  void recoveryUsesPhysicalEvidenceAndNeverRepeatsUncertainFormat() {
    assertEquals(
        "ACCEPTED", ExternalBrokerRecovery.checkpoint(physical("ABSENT", "MISSING", false)));
    assertEquals(
        "VALIDATING", ExternalBrokerRecovery.checkpoint(physical("CLAIMED", "MISSING", false)));
    assertEquals(
        "PREPARED", ExternalBrokerRecovery.checkpoint(physical("PREPARED", "MISSING", false)));
    assertEquals(
        "FORMATTED",
        ExternalBrokerRecovery.checkpoint(physical("FORMAT_INTENT", "MATCHING", false)));
    assertEquals(
        "FORMATTED",
        ExternalBrokerRecovery.checkpoint(physical("START_INTENT", "MATCHING", false)));
    assertEquals(
        "STARTED_PENDING_VERIFICATION",
        ExternalBrokerRecovery.checkpoint(physical("START_INTENT", "MATCHING", true)));
    for (String phase : List.of("FORMAT_INTENT", "FORMATTED", "STARTED"))
      for (String state : List.of("MISSING", "PARTIAL", "FOREIGN"))
        assertThrows(
            IllegalStateException.class,
            () -> ExternalBrokerRecovery.checkpoint(physical(phase, state, false)));
    for (String field :
        List.of("formatterObservationComplete", "runtimeVerified", "processVerified")) {
      var p = physical("STARTED", "MATCHING", true).put(field, false);
      assertThrows(IllegalStateException.class, () -> ExternalBrokerRecovery.checkpoint(p));
    }
    assertThrows(
        IllegalStateException.class,
        () ->
            ExternalBrokerRecovery.checkpoint(
                physical("STARTED", "MATCHING", true).put("formatterActive", true)));
    assertThrows(
        IllegalStateException.class,
        () ->
            ExternalBrokerRecovery.checkpoint(
                physical("ABSENT", "MISSING", false).put("storageEmpty", false)));
  }

  static ExternalBrokerAdditionLedger.Operation operation() {
    UUID id = UUID.randomUUID();
    return new ExternalBrokerAdditionLedger.Operation(
        id,
        UUID.randomUUID(),
        "candidate",
        UUID.randomUUID(),
        3,
        "VERIFYING",
        null,
        null,
        ExternalBrokerAdditionTest.payload(id));
  }

  static ObjectNode receipt(ExternalBrokerAdditionLedger.Operation op, Instant at) {
    return physical("STARTED", "MATCHING", true)
        .put("schemaVersion", "add-broker/v1")
        .put("operationId", op.id().toString())
        .put("hostId", op.agentId())
        .put("planDigest", op.payload().digest())
        .put("nodeId", op.nodeId())
        .put("kafkaClusterId", op.payload().plan().kafkaClusterId())
        .put("serviceName", op.payload().plan().serviceName())
        .put("configPath", op.payload().plan().configPath())
        .put("observedAt", at.toString());
  }

  static ExternalBrokerVerification.Evidence evidence(ExternalBrokerAdditionLedger.Operation op) {
    Instant now = Instant.now();
    var samples = new ArrayList<ExternalBrokerVerification.Sample>();
    for (int offset : new int[] {12, 6, 0}) {
      var at = now.minusSeconds(offset);
      samples.add(
          new ExternalBrokerVerification.Sample(
              at, "healthy unfenced broker; unchanged voters", receipt(op, at)));
    }
    return new ExternalBrokerVerification.Evidence(op.id(), op.payload().digest(), samples, now);
  }

  @Test
  void publicationRequiresThreeFreshSeparatedOperationBoundSamples() {
    var op = operation();
    var e = evidence(op);
    assertDoesNotThrow(() -> ExternalBrokerVerification.validate(op, e));
    assertThrows(
        IllegalStateException.class,
        () ->
            ExternalBrokerVerification.validate(
                op,
                new ExternalBrokerVerification.Evidence(
                    UUID.randomUUID(), e.planDigest(), e.samples(), e.verifiedAt())));
    assertThrows(
        IllegalStateException.class,
        () ->
            ExternalBrokerVerification.validate(
                op,
                new ExternalBrokerVerification.Evidence(
                    op.id(), e.planDigest(), e.samples().subList(0, 2), e.verifiedAt())));
    var repeated = Collections.nCopies(3, e.samples().getLast());
    assertThrows(
        IllegalStateException.class,
        () ->
            ExternalBrokerVerification.validate(
                op,
                new ExternalBrokerVerification.Evidence(
                    op.id(), e.planDigest(), repeated, e.verifiedAt())));
    assertThrows(
        IllegalStateException.class,
        () ->
            ExternalBrokerVerification.validate(
                op,
                new ExternalBrokerVerification.Evidence(
                    op.id(), e.planDigest(), e.samples(), Instant.now().minusSeconds(61))));
    ((ObjectNode) e.samples().getLast().receipt()).put("nodeId", 99);
    assertThrows(
        org.springframework.web.server.ResponseStatusException.class,
        () -> ExternalBrokerVerification.validate(op, e));
  }
}
