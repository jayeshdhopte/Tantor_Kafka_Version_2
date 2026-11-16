package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

class ExternalControllerRecoveryTest {
  @Test
  void controllerCommonGateDoesNotRequireAnUnusedBrokerProfile() {
    var checks = new ArrayList<io.translab.tantor.server.dto.AddNodeReadiness.Check>();
    for (String code : ExternalAddNodeReadinessService.CONTROLLER_COMMON_CODES)
      checks.add(
          new io.translab.tantor.server.dto.AddNodeReadiness.Check(
              code, code, "COMMON", "PASS", "required", "observed", "", Instant.now(), "FIXTURE"));
    checks.add(
        new io.translab.tantor.server.dto.AddNodeReadiness.Check(
            "B03",
            "Broker listener",
            "BROKER",
            "FAIL",
            "broker port",
            "unrelated application",
            "",
            Instant.now(),
            "FIXTURE"));
    var preview =
        new ExternalAddNodeReadinessService.Preview(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "candidate",
            "a".repeat(64),
            Instant.now(),
            Instant.now().plusSeconds(120),
            "BLOCKED",
            checks,
            false);
    assertTrue(preview.controllerCommonPassed());
    assertTrue(
        new ObjectMapper()
            .findAndRegisterModules()
            .valueToTree(preview)
            .path("controllerCommonPassed")
            .asBoolean());
    checks.add(
        new io.translab.tantor.server.dto.AddNodeReadiness.Check(
            "C15",
            "Duplicate checksum",
            "COMMON",
            "FAIL",
            "required",
            "mismatch",
            "",
            Instant.now(),
            "FIXTURE"));
    assertFalse(preview.controllerCommonPassed());
    checks.removeLast();
    checks.removeIf(c -> "C15".equals(c.code()));
    assertFalse(preview.controllerCommonPassed());
  }

  static KafkaExpansionPlan.Frozen frozen() throws Exception {
    var mapper = new ObjectMapper().findAndRegisterModules();
    var tree = (ObjectNode) mapper.readTree(new KafkaExpansionPlanTest().fixture());
    tree.put("schemaVersion", "add-quorum-expansion/v2");
    tree.put("layout", "CONTROLLER");
    var process = (ObjectNode) tree.path("processes").get(0);
    tree.set("processes", mapper.createArrayNode().add(process));
    var settings = ExternalAddNodeReadinessServiceTest.settings();
    tree.set("deploymentSettings", mapper.valueToTree(settings));
    tree.put("javaHome", settings.javaHome());
    tree.put("runtimeUser", settings.runtimeUser());
    tree.put("parcelDir", "/opt/tantor/parcels");
    process.set("dataPaths", mapper.valueToTree(settings.controllerDataPaths()));
    process.put("metadataPath", settings.controllerMetadataPath());
    process.put("logPath", settings.controllerLogPath());
    process.put("heapXmx", settings.controllerHeapXmx());
    return mapper.treeToValue(tree, KafkaExpansionPlan.class).freeze(mapper);
  }

  static ObjectNode report(KafkaExpansionPlan.Frozen frozen) {
    var report = new KafkaExpansionNativeEvidenceTest().report(frozen);
    var state = (ObjectNode) report.path("observation");
    state
        .put("mountsVerified", true)
        .put("formatterObservationComplete", true)
        .put("formatterActive", false)
        .put("admissionObservationComplete", true)
        .put("admissionActive", false)
        .put("storageEmpty", false);
    return report;
  }

  static KafkaExpansionNativeEvidence.Observation observation(
      String phase, String storage, boolean active) {
    var r = new ObjectMapper().createObjectNode().put("phase", phase);
    var s =
        new ObjectMapper()
            .createObjectNode()
            .put("storageState", storage)
            .put("serviceActive", active)
            .put("storageEmpty", "MISSING".equals(storage))
            .put("configOwned", true)
            .put("unitOwned", true)
            .put("runtimeVerified", active)
            .put("mountsVerified", true)
            .put("formatterObservationComplete", true)
            .put("formatterActive", false)
            .put("admissionObservationComplete", true)
            .put("admissionActive", false);
    return new KafkaExpansionNativeEvidence.Observation(
        r, s, Instant.now(), "candidate:controller");
  }

  @Test
  void defaultGateCannotInheritBrokerRelease() {
    var broker = mock(ExternalBrokerAdditionPlanning.class);
    when(broker.released()).thenReturn(true);
    var release = new ExternalControllerRelease(broker);
    assertFalse(release.enabled());
    assertThrows(IllegalStateException.class, release::require);
    org.springframework.test.util.ReflectionTestUtils.setField(release, "enabled", true);
    assertFalse(release.enabled());
    org.springframework.test.util.ReflectionTestUtils.setField(release, "brokerAccepted", true);
    assertTrue(release.enabled());
    when(broker.released()).thenReturn(false);
    assertFalse(release.enabled());
  }

  @Test
  void formatAndAdmissionRecoveryNeverTreatTimeoutAsAbsence() {
    assertEquals(
        "ACCEPTED",
        ExternalControllerRecovery.checkpoint(observation("ABSENT", "MISSING", false), false));
    assertEquals(
        "PREPARED",
        ExternalControllerRecovery.checkpoint(observation("PREPARED", "MISSING", false), false));
    assertEquals(
        "FORMATTED",
        ExternalControllerRecovery.checkpoint(
            observation("FORMAT_INTENT", "MATCHING", false), true));
    assertEquals(
        "OBSERVER_STARTED",
        ExternalControllerRecovery.checkpoint(
            observation("ADMISSION_INTENT", "MATCHING", true), true));
    for (String storage : List.of("MISSING", "PARTIAL", "FOREIGN", "UNKNOWN"))
      assertThrows(
          IllegalStateException.class,
          () ->
              ExternalControllerRecovery.checkpoint(
                  observation("FORMAT_INTENT", storage, false), true));
    assertThrows(
        IllegalStateException.class,
        () -> ExternalControllerRecovery.checkpoint(observation("ABSENT", "MISSING", false), true));
    assertThrows(
        IllegalStateException.class,
        () ->
            ExternalControllerRecovery.checkpoint(
                observation("ADMISSION_SENT", "MATCHING", false), true));
    for (String flag : List.of("formatterActive", "admissionActive")) {
      var p = observation("STARTED", "MATCHING", true);
      ((ObjectNode) p.state()).put(flag, true);
      assertThrows(
          IllegalStateException.class, () -> ExternalControllerRecovery.checkpoint(p, true));
    }
  }

  @Test
  void selectedControllerPlanUsesStandardProfileAndNoParticipantRestart() throws Exception {
    var frozen = frozen();
    assertEquals("controller.service", frozen.plan().unit("controller"));
    assertEquals(
        "/opt/tantor/kafka/config/controller.properties", frozen.plan().configPath("controller"));
    assertTrue(
        frozen.steps().stream()
            .noneMatch(
                s ->
                    Set.of(
                            KafkaExpansionWorkflow.Kind.STOP,
                            KafkaExpansionWorkflow.Kind.APPLY_VOTERS,
                            KafkaExpansionWorkflow.Kind.STAGE_VOTERS)
                        .contains(s.kind())));
    assertEquals(
        List.of("controller"),
        frozen.plan().processes().stream().map(KafkaExpansionPlan.Process::role).toList());
  }
}
