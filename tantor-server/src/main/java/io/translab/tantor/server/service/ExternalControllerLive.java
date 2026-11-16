package io.translab.tantor.server.service;

import io.translab.tantor.server.config.AddNodeReadinessProperties;
import io.translab.tantor.server.domain.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.Service;

/** Independent Kafka API proof; a candidate receipt cannot assert voter membership. */
@Service
@RequiredArgsConstructor
public class ExternalControllerLive {
  private final KafkaExpansionQuorum quorum;
  private final KafkaAdminService kafka;
  private final ExternalKafkaManagement external;
  private final AddNodeKafkaReadiness readiness;
  private final AddNodeReadinessProperties policy;

  public void baseline(KafkaExpansionPlan p) {
    health(p, false);
    quorum.healthyBaseline(
        ExternalControllerPlanning.baseline(p),
        quorum.observe(endpoints(p, false)),
        policy.getMaxQuorumLag(),
        Instant.now());
  }

  public KafkaExpansionLiveVerifier.ControllerProof controller(
      KafkaExpansionPlan.Frozen frozen, String directory, boolean voter) {
    var p = frozen.plan();
    var c = p.process("controller");
    var candidate =
        new KafkaExpansionQuorum.Candidate(
            c.nodeId(), directory, ExternalControllerPlanning.endpoint(p.candidateIp(), c.port()));
    var live = quorum.observe(endpoints(p, true));
    var result =
        voter
            ? quorum.member(
                ExternalControllerPlanning.baseline(p),
                live,
                candidate,
                policy.getMaxQuorumLag(),
                false,
                Instant.now())
            : quorum.caughtUp(
                ExternalControllerPlanning.baseline(p),
                live,
                candidate,
                policy.getMaxQuorumLag(),
                Instant.now());
    return new KafkaExpansionLiveVerifier.ControllerProof(
        UUID.randomUUID(),
        p.operationId(),
        frozen.digest(),
        c.nodeId(),
        directory,
        p.voterDigest(),
        result.observedAt(),
        !result.voter(),
        result.voter());
  }

  public String health(KafkaExpansionPlan p, boolean expanded) {
    var cluster = external.resolve(p.clusterId());
    cluster.setKafkaClusterId(p.kafkaClusterId());
    if (expanded) {
      var services = new ArrayList<>(cluster.getServices());
      var c = new ClusterServiceAssignment();
      c.setCluster(cluster);
      c.setHostId("external-agent:" + p.candidateHostId());
      c.setNodeId(p.process("controller").nodeId());
      c.setRole("controller");
      services.add(c);
      cluster.setServices(services);
    }
    try {
      var description =
          kafka
              .getAdminClient(cluster.getId())
              .describeCluster(
                  new DescribeClusterOptions().includeFencedBrokers(false).timeoutMs(15000));
      if (!p.kafkaClusterId().equals(description.clusterId().get(20, TimeUnit.SECONDS)))
        throw new IllegalStateException("Kafka ID changed.");
      var expected =
          p.participants().stream()
              .filter(m -> m.role().contains("broker"))
              .map(KafkaExpansionPlan.Participant::nodeId)
              .collect(java.util.stream.Collectors.toSet());
      var actual =
          description.nodes().get(20, TimeUnit.SECONDS).stream()
              .map(org.apache.kafka.common.Node::id)
              .collect(java.util.stream.Collectors.toSet());
      if (!expected.equals(actual))
        throw new IllegalStateException(
            "Existing broker membership changed during controller addition.");
      var checks = readiness.observe(cluster, Map.of()).checks();
      for (String code : List.of("C01", "C02", "C03", "C05", "C06", "C07", "C08", "B09"))
        if (checks.stream().filter(c -> code.equals(c.code()) && "PASS".equals(c.status())).count()
            != 1)
          throw new KafkaExpansionQuorum.NotReady(
              "Controller cluster health is not proven for " + code + ".");
      kafka
          .getAdminClient(cluster.getId())
          .listTopics(new ListTopicsOptions().listInternal(true).timeoutMs(15000))
          .names()
          .get(20, TimeUnit.SECONDS);
      return "Existing unfenced brokers, exact cluster ID, leader/membership, log directories,"
          + " ISR/partition health and topic-list smoke verified.";
    } catch (KafkaExpansionQuorum.NotReady e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("External controller cluster verification incomplete.", e);
    }
  }

  private List<String> endpoints(KafkaExpansionPlan p, boolean candidate) {
    var endpoints = new TreeSet<String>();
    p.voters()
        .forEach(v -> endpoints.add(ExternalControllerPlanning.endpoint(v.hostIp(), v.port())));
    if (candidate)
      endpoints.add(
          ExternalControllerPlanning.endpoint(p.candidateIp(), p.process("controller").port()));
    return List.copyOf(endpoints);
  }
}
