package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.translab.tantor.server.domain.*;
import java.time.Instant;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ExternalBrokerVerification {
  private final ExternalBrokerAdditionPlanning planning;
  private final ExternalKafkaManagement external;
  private final BrokerAdditionKafka kafka;

  public record Sample(Instant observedAt, String health, JsonNode receipt) {}

  public record Evidence(
      UUID operationId, String planDigest, List<Sample> samples, Instant verifiedAt) {}

  public Sample sample(ExternalBrokerAdditionLedger.Operation op, JsonNode receipt) {
    ExternalBrokerAdditionLedger.validateProof(op, "INSPECT_ADD_BROKER", receipt);
    if (!"STARTED_PENDING_VERIFICATION".equals(ExternalBrokerRecovery.checkpoint(receipt)))
      throw new IllegalStateException(
          "Candidate broker is not active with verified owned storage/runtime.");
    planning.assertCurrent(op.clusterId(), op.payload());
    var p = op.payload();
    var snapshot = external.resolve(op.clusterId());
    snapshot.setKafkaClusterId(p.plan().kafkaClusterId());
    var candidate = new ClusterServiceAssignment();
    candidate.setCluster(snapshot);
    candidate.setHostId("external-agent:" + op.agentId());
    candidate.setNodeId(op.nodeId());
    candidate.setRole("broker");
    var services = new ArrayList<>(snapshot.getServices());
    services.add(candidate);
    snapshot.setServices(services);
    Map<String, Object> config = new HashMap<>();
    config.put("controller_quorum_voters", p.plan().quorumVoters());
    String health =
        kafka.verifyExternal(
            snapshot,
            new BrokerAdditionLedger.Payload(
                p.plan(), p.properties(), p.digest(), p.voterBaseline(), p.runtime()),
            config);
    return new Sample(Instant.now(), health, receipt);
  }

  public static void validate(ExternalBrokerAdditionLedger.Operation op, Evidence evidence) {
    if (evidence == null
        || !op.id().equals(evidence.operationId())
        || !op.payload().digest().equals(evidence.planDigest())
        || evidence.samples() == null
        || evidence.samples().size() != 3
        || evidence.verifiedAt() == null
        || evidence.verifiedAt().isBefore(Instant.now().minusSeconds(60))
        || evidence.verifiedAt().isAfter(Instant.now().plusSeconds(5)))
      throw new IllegalStateException("Fresh operation-bound stable health evidence is required.");
    Instant previous = null;
    for (var sample : evidence.samples()) {
      if (sample == null
          || sample.observedAt() == null
          || sample.health() == null
          || sample.health().isBlank()
          || sample.observedAt().isAfter(evidence.verifiedAt())
          || sample.observedAt().isBefore(evidence.verifiedAt().minusSeconds(300))
          || previous != null && sample.observedAt().isBefore(previous.plusSeconds(5)))
        throw new IllegalStateException(
            "Stable health samples must be complete, ordered and separated by five seconds.");
      ExternalBrokerAdditionLedger.validateProof(op, "INSPECT_ADD_BROKER", sample.receipt());
      if (!"STARTED_PENDING_VERIFICATION"
          .equals(ExternalBrokerRecovery.checkpoint(sample.receipt())))
        throw new IllegalStateException("Stable sample lacks a healthy owned candidate.");
      Instant receiptAt = Instant.parse(sample.receipt().path("observedAt").asText());
      if (receiptAt.isAfter(sample.observedAt().plusSeconds(5))
          || receiptAt.isBefore(sample.observedAt().minusSeconds(60)))
        throw new IllegalStateException("Candidate inspection must accompany the health sample.");
      previous = sample.observedAt();
    }
    if (previous.isBefore(evidence.verifiedAt().minusSeconds(15)))
      throw new IllegalStateException("Final stable health sample is stale.");
  }
}
