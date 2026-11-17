package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;

class ExternalSeparateProcessTest {
  static KafkaExpansionPlan.Frozen frozen() throws Exception {
    var mapper = new ObjectMapper().findAndRegisterModules();
    var controller = ExternalControllerRecoveryTest.frozen();
    ObjectNode tree = mapper.valueToTree(controller.plan());
    tree.put("layout", "SEPARATE");
    var s = controller.plan().deploymentSettings();
    ObjectNode broker = ((ObjectNode) tree.path("processes").get(0)).deepCopy();
    broker
        .put("role", "broker")
        .put("nodeId", 2)
        .put("port", 9092)
        .put("metadataPath", s.brokerMetadataPath())
        .put("logPath", s.brokerLogPath())
        .put("heapXmx", s.brokerHeapXmx())
        .put("brokerRack", "");
    broker.set("dataPaths", mapper.valueToTree(s.brokerDataPaths()));
    ((com.fasterxml.jackson.databind.node.ArrayNode) tree.path("processes")).add(broker);
    return mapper.treeToValue(tree, KafkaExpansionPlan.class).freeze(mapper);
  }

  static ObjectNode report(KafkaExpansionPlan.Frozen f, String role) {
    var report = ExternalControllerRecoveryTest.report(f);
    if (role.equals("controller")) return report;
    var b = f.plan().process("broker");
    ((ObjectNode) report.path("receipt"))
        .put("role", role)
        .put("nodeId", b.nodeId())
        .put("unit", f.plan().unit(role))
        .put("configPath", f.plan().configPath(role));
    var dirs = new ObjectMapper().createObjectNode();
    for (var path : b.dataPaths()) dirs.put(path, "BBBBBBBBBBBBBBBBBBBBBA");
    if (!b.metadataPath().isEmpty()) dirs.put(b.metadataPath(), "CCCCCCCCCCCCCCCCCCCCCA");
    ((ObjectNode) report.path("receipt")).set("directoryIds", dirs);
    ((ObjectNode) report.path("observation")).set("directoryIds", dirs);
    ((ObjectNode) report.path("observation"))
        .put(
            "metadataDirectoryId",
            dirs.path(b.metadataPath().isEmpty() ? b.dataPaths().getFirst() : b.metadataPath())
                .asText());
    return report;
  }

  @Test
  void isolatedSequentialProfilesAndControllerFirstWorkflow() throws Exception {
    var f = frozen();
    var p = f.plan();
    assertEquals(2, p.process("broker").nodeId());
    assertEquals(102, p.process("controller").nodeId());
    assertEquals("broker.service", p.unit("broker"));
    assertEquals("controller.service", p.unit("controller"));
    assertEquals("/opt/tantor/kafka/config/broker.properties", p.configPath("broker"));
    assertFalse(p.process("broker").dataPaths().equals(p.process("controller").dataPaths()));
    assertTrue(f.steps().stream().noneMatch(s -> s.kind() == KafkaExpansionWorkflow.Kind.STOP));
    var e = new KafkaExpansionNativeEvidence(new ObjectMapper().findAndRegisterModules());
    for (var role : List.of("controller", "broker")) {
      var r = report(f, role);
      e.runningCandidate(
          new KafkaExpansionNativeEvidence.Observation(
              r.path("receipt"),
              r.path("observation"),
              java.time.Instant.now(),
              "candidate:" + role));
    }
  }

  @Test
  void separateExecutionNeedsItsOwnGateAndControllerAcceptance() {
    var broker = mock(ExternalBrokerAdditionPlanning.class);
    when(broker.released()).thenReturn(true);
    var gate = new ExternalControllerRelease(broker);
    org.springframework.test.util.ReflectionTestUtils.setField(gate, "enabled", true);
    org.springframework.test.util.ReflectionTestUtils.setField(gate, "brokerAccepted", true);
    assertTrue(gate.enabled());
    assertFalse(gate.enabled(true));
    org.springframework.test.util.ReflectionTestUtils.setField(gate, "separateEnabled", true);
    assertFalse(gate.enabled(true));
    org.springframework.test.util.ReflectionTestUtils.setField(gate, "controllerAccepted", true);
    assertTrue(gate.enabled(true));
  }
}
