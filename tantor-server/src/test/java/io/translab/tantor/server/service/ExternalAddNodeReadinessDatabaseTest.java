package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;

/** Actual PostgreSQL SQL/claim contract, isolated schema rolled back on loopback. */
@EnabledIfEnvironmentVariable(
    named = "TANTOR_ADD_NODE_CHECK_URL",
    matches = "jdbc:postgresql://127\\.0\\.0\\.1:65440/.*")
class ExternalAddNodeReadinessDatabaseTest {
  @Test
  void previewClaimResultReplayAndContextInvalidation() {
    var source =
        new DriverManagerDataSource(System.getenv("TANTOR_ADD_NODE_CHECK_URL"), "postgres", "");
    var jdbc = new JdbcTemplate(source);
    var tx = new TransactionTemplate(new DataSourceTransactionManager(source));
    tx.executeWithoutResult(
        transaction -> {
          transaction.setRollbackOnly();
          String schema =
              "external_readiness_check_" + UUID.randomUUID().toString().replace("-", "");
          jdbc.execute("create schema " + schema);
          jdbc.execute("set local search_path to " + schema);
          jdbc.execute(
              "create table kf_external_clusters(id uuid primary key); create table"
                  + " kf_discovery_agents(id varchar(255) primary key)");
          for (String migration :
              List.of(
                  "V107__external_add_node_host_observations.sql",
                  "V108__external_add_node_readiness.sql")) {
            try (var stream = getClass().getResourceAsStream("/db/migration/" + migration)) {
              assertNotNull(stream);
              jdbc.execute(
                  new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          var f = new ExternalAddNodeReadinessServiceTest();
          f.setup();
          f.scope.setAllowedHostIds(List.of("candidate", "peer"));
          f.policy.setClientProbeHostIds(List.of("peer"));
          var peer = new DiscoveryAgent();
          peer.setId("peer");
          peer.setHostname("peer");
          peer.setIpAddresses("[\"192.0.2.20\",\"192.0.2.21\"]");
          peer.setVersion(f.agent.getVersion());
          peer.setCanExecuteTasks(true);
          peer.setStatus("ONLINE");
          peer.setLastHeartbeat(OffsetDateTime.now());
          peer.setClusterId(f.clusterId);
          when(f.agents.findById("peer")).thenReturn(Optional.of(peer));
          when(f.agents.findAll()).thenReturn(List.of(f.agent, peer));
          var node = new ExternalClusterNode();
          node.setId(UUID.randomUUID());
          node.setClusterId(f.clusterId);
          node.setAgentId("peer");
          node.setHost("192.0.2.20");
          node.setNodeId(1);
          node.setIsBroker(true);
          node.setIsController(true);
          node.setConfigFile("/opt/kafka/config/server.properties");
          node.setSystemdService("kafka.service");
          when(f.nodes.findByClusterId(f.clusterId)).thenReturn(List.of(node));
          when(f.inspections.candidates(f.clusterId))
              .thenReturn(
                  new ExternalAddNodeInspectionService.Review(
                      "schema",
                      f.clusterId,
                      "HOST_INSPECTION_ONLY",
                      false,
                      List.of(
                          new ExternalAddNodeInspectionService.Candidate(
                              "candidate",
                              "candidate",
                              Instant.now(),
                              "HOST_OBSERVED",
                              true,
                              false,
                              List.of())),
                      List.of()));
          when(f.external.resolve(f.clusterId)).thenReturn(new Cluster());
          var checks = new ArrayList<Check>();
          for (String code : List.of("C01", "C02", "C03", "C05", "C06", "C07", "C08", "B09"))
            checks.add(
                new Check(
                    code,
                    "Live fixture",
                    "COMMON",
                    "PASS",
                    "Measured fixture",
                    "Measured fixture",
                    "",
                    Instant.now(),
                    "KAFKA_API"));
          when(f.live.observe(any(), anyMap()))
              .thenReturn(
                  new AddNodeKafkaReadiness.Observation(
                      checks,
                      Set.of(1),
                      List.of("192.0.2.20:9092"),
                      List.of("192.0.2.20:9093"),
                      "DYNAMIC",
                      Set.of("rack1"),
                      true));
          when(f.artifacts.resolve("4.3.0", ""))
              .thenReturn(
                  new AddNodeArtifactCatalog.Artifact(
                      UUID.randomUUID().toString(), "4.3.0", "a".repeat(64)));
          var service =
              new ExternalAddNodeReadinessService(
                  f.inspections,
                  f.clusters,
                  f.nodes,
                  f.agents,
                  f.external,
                  f.live,
                  f.kafka,
                  f.artifacts,
                  f.policy,
                  f.scope,
                  f.conflicts,
                  jdbc,
                  f.mapper);
          jdbc.update("insert into kf_external_clusters values(?)", f.clusterId);
          jdbc.update("insert into kf_discovery_agents values('candidate'),('peer')");
          for (var agent : List.of(f.agent, peer)) {
            var report =
                new ExternalAddNodeInspectionService.Observation(
                    ExternalAddNodeInspectionService.SCHEMA,
                    agent.getId(),
                    agent.getVersion(),
                    Instant.now(),
                    agent == peer ? List.of("192.0.2.20", "192.0.2.21") : List.of("192.0.2.10"),
                    true,
                    true,
                    true,
                    agent == peer ? 1 : 0,
                    0,
                    agent == peer ? 1 : 0,
                    false);
            try {
              jdbc.update(
                  "insert into kf_external_add_node_observations(agent_id,observed_at,report)"
                      + " values(?,now(),?)",
                  agent.getId(),
                  f.mapper.writeValueAsString(report));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          var preview = service.create(f.clusterId, ExternalAddNodeReadinessServiceTest.request());
          assertEquals("CHECKING", preview.status());
          assertFalse(preview.deploymentEnabled());
          assertThrows(
              org.springframework.web.server.ResponseStatusException.class,
              () -> service.create(f.clusterId, ExternalAddNodeReadinessServiceTest.request()));
          var candidate = service.claim("candidate");
          var probe = service.claim("peer");
          assertEquals("INSPECT", candidate.get("command"));
          assertEquals("PROBE", probe.get("command"));
          assertTrue(service.claim("candidate").isEmpty());
          var parameters = (Map<?, ?>) candidate.get("parameters");
          assertTrue(parameters.get("expiresAt") instanceof String);
          var peers = (List<?>) parameters.get("peerProbes");
          assertEquals("BROKER,CLIENT,CONTROLLER", ((Map<?, ?>) peers.getFirst()).get("roles"));
          var report =
              f.mapper
                  .createObjectNode()
                  .put("schemaVersion", "add-node/v1")
                  .put("hostId", "candidate")
                  .put("contextHash", parameters.get("contextHash").toString());
          var observations = report.putArray("checks");
          for (String code : ExternalAddNodeReadinessService.AGENT_CODES)
            observations
                .addObject()
                .put("code", code)
                .put("name", "Native fixture")
                .put("phase", "COMMON")
                .put("status", "PASS")
                .put("expected", "Measured fixture")
                .put("observed", "Measured fixture")
                .put("remedy", "")
                .put("observedAt", Instant.now().toString())
                .put("source", "AGENT");
          var result =
              new ExternalAddNodeReadinessService.Result(
                  (UUID) candidate.get("claimToken"), "SUCCESS", report, null);
          service.complete("candidate", (UUID) candidate.get("id"), result);
          service.complete("candidate", (UUID) candidate.get("id"), result);
          assertEquals("CHECKING", service.get(f.clusterId, preview.id()).status());
          service.complete(
              "peer",
              (UUID) probe.get("id"),
              new ExternalAddNodeReadinessService.Result(
                  (UUID) probe.get("claimToken"),
                  "SUCCESS",
                  f.mapper.createObjectNode().put("status", "PASS"),
                  null));
          assertEquals("PASS", service.get(f.clusterId, preview.id()).status());
          peer.setIpAddresses("[\"192.0.2.20\"]");
          assertEquals(
              "PASS",
              service.get(f.clusterId, preview.id()).status(),
              "Ordinary transport-IP heartbeat must not invalidate the same observed host.");
          assertFalse(service.get(f.clusterId, preview.id()).deploymentEnabled());
          assertThrows(
              org.springframework.web.server.ResponseStatusException.class,
              () ->
                  service.complete(
                      "candidate",
                      (UUID) candidate.get("id"),
                      new ExternalAddNodeReadinessService.Result(
                          UUID.randomUUID(), "SUCCESS", report, null)));
          f.agent.setVersion("3.4.0-changed-an.1");
          assertEquals("STALE", service.get(f.clusterId, preview.id()).status());
        });
  }
}
