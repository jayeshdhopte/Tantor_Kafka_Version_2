package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.repository.*;
import java.time.Instant;
import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/** Frozen broker-only proposal; no task dispatch or identity reservation. */
@Service
@RequiredArgsConstructor
public class ExternalBrokerAdditionPlanning {
  private final ExternalAddNodeReadinessService readiness;
  private final ExternalKafkaManagement external;
  private final ExternalClusterNodeRepository nodes;
  private final BrokerAdditionKafka kafka;
  private final AddNodeArtifactCatalog artifacts;
  private final ArtifactRepositoryProperties artifactPolicy;
  private final KafkaDeploymentProperties deployment;
  private final ObjectMapper mapper;
  private final JdbcTemplate jdbc;

  @Value("${tantor.external-add-node.execution-enabled:false}")
  private boolean enabled;

  @Value("${tantor.external-add-node.phase2-accepted:false}")
  private boolean readinessAccepted;

  public record Payload(
      ExternalAddNodeReadinessService.Request request,
      BrokerAdditionPlan plan,
      String properties,
      BrokerAdditionLedger.RuntimeOptions runtime,
      String voterBaseline,
      String artifactId,
      String digest,
      String bindingHash) {
    public Payload(
        ExternalAddNodeReadinessService.Request request,
        BrokerAdditionPlan plan,
        String properties,
        BrokerAdditionLedger.RuntimeOptions runtime,
        String voterBaseline,
        String artifactId,
        String digest) {
      this(request, plan, properties, runtime, voterBaseline, artifactId, digest, "");
    }

    public Payload {
      bindingHash = Objects.toString(bindingHash, "");
    }
  }

  public record Review(
      UUID previewId,
      String contextHash,
      Instant expiresAt,
      BrokerAdditionPlan proposedPlan,
      String planDigest,
      String serviceName,
      String configPath,
      boolean executionEnabled,
      List<String> blockers) {}

  public record Frozen(ExternalAddNodeReadinessService.Preview preview, Payload payload) {}

  public boolean released() {
    return enabled && readinessAccepted;
  }

  public void requireRelease() {
    if (!released())
      throw conflict(
          "External broker execution is held until Phase 2 live acceptance and the external"
              + " execution release flag are enabled.");
  }

  public Review review(UUID clusterId, UUID previewId) {
    var frozen = freeze(clusterId, previewId, null, null);
    var p = frozen.payload();
    var blockers = new ArrayList<String>();
    if (!released())
      blockers.add(
          "External execution is not enabled for this release. Complete environment acceptance"
              + " before scheduling a broker addition.");
    try {
      readiness.requireExecutionAgent(p.plan().hostId());
    } catch (RuntimeException unavailable) {
      blockers.add(
          "The candidate needs a fresh compatible external broker-add agent with explicit execution"
              + " opt-in.");
    }
    return new Review(
        previewId,
        frozen.preview().contextHash(),
        frozen.preview().expiresAt(),
        p.plan(),
        p.digest(),
        p.plan().serviceName() + ".service",
        p.plan().configPath(),
        blockers.isEmpty(),
        List.copyOf(blockers));
  }

  public Frozen freeze(UUID clusterId, UUID previewId, UUID ownerJob, UUID operationId) {
    var evidence = readiness.executionEvidence(clusterId, previewId, ownerJob);
    var preview = evidence.preview();
    var r = evidence.request();
    var v = evidence.inspectionParameters();
    var snapshot = external.resolve(clusterId);
    snapshot.setKafkaClusterId(text(v, "kafkaClusterId"));
    Set<Integer> reserved =
        new HashSet<>(
            jdbc.query(
                "select node_id from kf_external_broker_additions where cluster_id=? and"
                    + " state<>'CANCELLED_SAFE' and (cast(? as text) is null or job_id<>?)",
                (rs, n) -> rs.getInt(1),
                clusterId,
                ownerJob == null ? null : ownerJob.toString(),
                ownerJob == null ? null : ownerJob.toString()));
    var allocation = AddNodeIdentityAllocator.broker(snapshot, reserved);
    if (!allocation.available()) throw conflict(allocation.reason());
    UUID id =
        operationId == null
            ? UUID.nameUUIDFromBytes(
                (clusterId + ":" + previewId + ":" + preview.contextHash())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            : operationId;
    var settings = r.deploymentSettings();
    var plan =
        new BrokerAdditionPlan(
            "add-broker/v3",
            id,
            r.agentId(),
            preview.contextHash(),
            text(v, "kafkaClusterId"),
            allocation.nodeId(),
            r.hostIp(),
            r.brokerPort(),
            text(v, "kafkaVersion"),
            settings.runtimeUser(),
            settings.javaHome(),
            r.parcelDir(),
            text(v, "artifactChecksum"),
            settings.brokerDataPaths(),
            settings.brokerMetadataPath(),
            settings.brokerLogPath(),
            settings.installPath(),
            settings.brokerHeapXmx(),
            text(v, "quorumMode"),
            strings(v, "controllerEndpoints"),
            text(v, "quorumVoters"),
            r.brokerRack(),
            settings);
    plan.validate();
    var artifact = artifacts.resolve(plan.kafkaVersion(), r.artifactId());
    if (!artifact.checksum().equalsIgnoreCase(plan.artifactChecksum()))
      throw conflict("Approved Kafka artifact changed since readiness.");
    var existing =
        nodes.findByClusterId(clusterId).stream()
            .filter(n -> Boolean.TRUE.equals(n.getIsBroker()))
            .toList();
    Set<Integer> ports = new HashSet<>();
    for (var n : existing) ports.add(n.getJmxExporterPort() == null ? 0 : n.getJmxExporterPort());
    if (ports.size() != 1)
      throw conflict("Resolve existing broker monitoring inheritance before adding a broker.");
    int port = ports.iterator().next();
    if (port < 0 || port > 65535 || port == plan.brokerPort())
      throw conflict("JMX exporter port must be valid and distinct from the broker listener.");
    String jmxId = "", jmxHash = "";
    if (port > 0) {
      var jmx =
          artifacts.resolveType("JMX_EXPORTER", null, artifactPolicy.getJmxExporterArtifactId());
      jmxId = jmx.id();
      jmxHash = jmx.checksum();
    }
    if (deployment.getLimitNofile() < 1024)
      throw conflict("Broker service file limit is below the supported minimum.");
    var runtime =
        new BrokerAdditionLedger.RuntimeOptions(jmxId, jmxHash, port, deployment.getLimitNofile());
    String properties = kafka.properties(snapshot, plan), baseline = kafka.voterBaseline(snapshot);
    String digest =
        BrokerAdditionLedger.hash(json(plan) + "\n" + properties + "\n" + json(runtime));
    return new Frozen(
        preview,
        new Payload(
            r,
            plan,
            properties,
            runtime,
            baseline,
            artifact.id(),
            digest,
            readiness.executionBinding(clusterId, r)));
  }

  public Map<String, String> parameters(Payload payload) {
    return Map.of(
        "broker_plan",
        json(payload.plan()),
        "server_properties",
        payload.properties(),
        "runtime_options",
        json(payload.runtime()),
        "plan_digest",
        payload.digest());
  }

  public void assertCurrent(UUID clusterId, Payload p) {
    assertBinding(clusterId, p);
    readiness.assertExecutionScope(clusterId, p.plan().hostId());
    var snapshot = external.resolve(clusterId);
    snapshot.setKafkaClusterId(p.plan().kafkaClusterId());
    if (!p.voterBaseline().equals(kafka.voterBaseline(snapshot))
        || !p.properties().equals(kafka.properties(snapshot, p.plan())))
      throw conflict("Live voters or inherited broker configuration changed.");
    if (!p.plan()
        .artifactChecksum()
        .equalsIgnoreCase(artifacts.resolve(p.plan().kafkaVersion(), p.artifactId()).checksum()))
      throw conflict("Approved Kafka payload changed.");
    if (p.runtime().jmxPort() > 0
        && !p.runtime()
            .jmxChecksum()
            .equalsIgnoreCase(
                artifacts
                    .resolveType("JMX_EXPORTER", null, p.runtime().jmxArtifactId())
                    .checksum())) throw conflict("Approved monitoring payload changed.");
  }

  void assertBinding(UUID clusterId, Payload p) {
    requireRelease();
    if (p.runtime().limitNofile() != deployment.getLimitNofile())
      throw conflict("Broker runtime policy changed after review.");
    if (!p.bindingHash().matches("[a-f0-9]{64}"))
      throw conflict(
          "Legacy broker operation lacks a stable recovery binding. Preserve it for explicit"
              + " migration/review.");
    if (!p.bindingHash().equals(readiness.executionBinding(clusterId, p.request())))
      throw conflict("Frozen participant, candidate or connection binding changed.");
  }

  public String json(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot encode external broker plan.", e);
    }
  }

  private static String text(Map<String, Object> v, String key) {
    return Objects.toString(v.get(key), "");
  }

  private static List<String> strings(Map<String, Object> v, String key) {
    if (!(v.get(key) instanceof List<?> list)
        || list.stream().anyMatch(x -> !(x instanceof String)))
      throw conflict("Missing inherited controller endpoints.");
    return list.stream().map(String.class::cast).toList();
  }

  private static ResponseStatusException conflict(String message) {
    return new ResponseStatusException(HttpStatus.CONFLICT, message);
  }
}
