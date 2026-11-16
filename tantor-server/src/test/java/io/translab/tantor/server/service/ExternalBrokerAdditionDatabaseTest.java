package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;

/** Real SQL in a private loopback schema. No application inventory is touched. */
@EnabledIfEnvironmentVariable(
    named = "TANTOR_ADD_NODE_CHECK_URL",
    matches = "jdbc:postgresql://127\\.0\\.0\\.1:65440/.*")
class ExternalBrokerAdditionDatabaseTest {
  @Test
  void reservationClaimsReplayAndPublicationGuards() {
    var source =
        new DriverManagerDataSource(System.getenv("TANTOR_ADD_NODE_CHECK_URL"), "postgres", "");
    var jdbc = new JdbcTemplate(source);
    new TransactionTemplate(new DataSourceTransactionManager(source))
        .executeWithoutResult(
            tx -> {
              tx.setRollbackOnly();
              String schema =
                  "external_broker_check_" + UUID.randomUUID().toString().replace("-", "");
              jdbc.execute("create schema " + schema);
              jdbc.execute("set local search_path to " + schema);
              jdbc.execute(
                  "create table kf_external_clusters(id uuid primary key); create table"
                      + " kf_discovery_agents(id varchar(255) primary key,cluster_id uuid); create"
                      + " table kf_jobs(id varchar(36) primary key,type varchar(64),status"
                      + " varchar(32),resource_key varchar(255)); create table kf_job_steps(id uuid"
                      + " primary key,job_id varchar(36)); create table"
                      + " kf_external_add_node_previews(id uuid primary key,cluster_id"
                      + " uuid,agent_id varchar(255),context_hash varchar(64),expires_at"
                      + " timestamptz); create table kf_external_cluster_nodes(id uuid primary"
                      + " key,cluster_id uuid,agent_id varchar(255),node_id integer,host"
                      + " varchar(255),config_file text,systemd_service text,is_broker"
                      + " boolean,is_controller boolean,jmx_exporter_port integer); create table"
                      + " kf_external_service_tasks(id uuid primary key,agent_id"
                      + " varchar(255),command varchar(40),status varchar(32)); create table"
                      + " kf_external_kafka_tasks(id uuid primary key,agent_id varchar(255),command"
                      + " varchar(40),status varchar(32))");
              jdbc.execute(
                  "create table kf_external_add_node_tasks(id uuid primary key,preview_id"
                      + " uuid,status varchar(16),deadline timestamptz)");
              try (var stream =
                  getClass()
                      .getResourceAsStream("/db/migration/V109__external_broker_addition.sql")) {
                assertNotNull(stream);
                jdbc.execute(
                    new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              var planning = mock(ExternalBrokerAdditionPlanning.class);
              var readiness = mock(ExternalAddNodeReadinessService.class);
              var jobs = mock(JobService.class);
              var clusters = mock(ExternalClusterRepository.class);
              var mapper = new ObjectMapper().findAndRegisterModules();
              when(planning.json(any()))
                  .thenAnswer(call -> mapper.writeValueAsString(call.getArgument(0)));
              UUID clusterId = UUID.randomUUID(),
                  previewId = UUID.randomUUID(),
                  id = UUID.randomUUID();
              var payload = ExternalBrokerAdditionTest.payload(id);
              var cluster = new ExternalCluster();
              cluster.setId(clusterId);
              when(clusters.findForServiceUpdate(clusterId)).thenReturn(Optional.of(cluster));
              var preview =
                  new ExternalAddNodeReadinessService.Preview(
                      previewId,
                      clusterId,
                      "candidate",
                      "a".repeat(64),
                      Instant.now(),
                      Instant.now().plusSeconds(120),
                      "PASS",
                      List.of(),
                      false);
              when(planning.freeze(clusterId, previewId, null, null))
                  .thenReturn(new ExternalBrokerAdditionPlanning.Frozen(preview, payload));
              when(planning.parameters(payload))
                  .thenReturn(Map.of("broker_plan", "fixture", "plan_digest", payload.digest()));
              jdbc.update("insert into kf_external_clusters values(?)", clusterId);
              jdbc.update("insert into kf_discovery_agents(id) values('candidate')");
              jdbc.update(
                  "insert into kf_external_add_node_previews"
                      + " values(?,?,'candidate',?,now()+interval '120 seconds')",
                  previewId,
                  clusterId,
                  "a".repeat(64));
              List<JobStep> steps = new ArrayList<>();
              when(jobs.createJob(any(Job.class), anyList()))
                  .thenAnswer(
                      call -> {
                        Job job = call.getArgument(0);
                        job.setId(UUID.randomUUID());
                        jdbc.update(
                            "insert into kf_jobs values(?,?,?,?)",
                            job.getId().toString(),
                            job.getType().name(),
                            "PENDING",
                            job.getResourceKey());
                        for (JobStep step : (List<JobStep>) call.getArgument(1)) {
                          step.setId(UUID.randomUUID());
                          step.setJob(job);
                          steps.add(step);
                          jdbc.update(
                              "insert into kf_job_steps values(?,?)",
                              step.getId(),
                              job.getId().toString());
                        }
                        return job;
                      });
              when(jobs.getSteps(any())).thenReturn(steps);
              var ledger =
                  new ExternalBrokerAdditionLedger(
                      jdbc, mapper, jobs, clusters, planning, readiness);
              jdbc.update(
                  "insert into kf_external_service_tasks values(?,'candidate','INSTALL','RUNNING')",
                  UUID.randomUUID());
              assertThrows(
                  org.springframework.web.server.ResponseStatusException.class,
                  () ->
                      ledger.accept(
                          clusterId,
                          previewId,
                          "a".repeat(64),
                          payload.digest(),
                          "fixture-request-key-1234"));
              jdbc.update("delete from kf_external_service_tasks");
              var accepted =
                  ledger.accept(
                      clusterId,
                      previewId,
                      "a".repeat(64),
                      payload.digest(),
                      "fixture-request-key-1234");
              assertEquals(3, ledger.get(id).nodeId());
              assertEquals(
                  accepted,
                  ledger.accept(
                      clusterId,
                      previewId,
                      "a".repeat(64),
                      payload.digest(),
                      "fixture-request-key-1234"));
              assertEquals(accepted, ledger.lookup(clusterId, "fixture-request-key-1234"));
              assertThrows(
                  org.springframework.web.server.ResponseStatusException.class,
                  () ->
                      ledger.accept(
                          clusterId,
                          previewId,
                          "d".repeat(64),
                          payload.digest(),
                          "fixture-request-key-1234"));
              assertEquals(1, jdbc.queryForObject("select count(*) from kf_jobs", Integer.class));
              blocked(
                  jdbc,
                  () ->
                      jdbc.update(
                          "insert into kf_external_service_tasks"
                              + " values(?,'candidate','INSTALL','PENDING')",
                          UUID.randomUUID()));
              blocked(
                  jdbc,
                  () ->
                      jdbc.update(
                          "insert into kf_external_kafka_tasks"
                              + " values(?,'candidate','RESTART_SERVICE','PENDING')",
                          UUID.randomUUID()));
              jdbc.update(
                  "insert into kf_external_kafka_tasks"
                      + " values(?,'candidate','READ_KAFKA_PROPERTIES','PENDING')",
                  UUID.randomUUID());
              var guard = new ExternalBrokerAdditionGuard(jdbc);
              assertTrue(guard.reservedAgent("candidate"));
              assertThrows(IllegalStateException.class, () -> guard.assertMutable(clusterId, null));
              assertDoesNotThrow(() -> guard.assertMutable(clusterId, accepted.jobId()));
              blocked(
                  jdbc,
                  () ->
                      jdbc.update(
                          "update kf_discovery_agents set cluster_id=? where id='candidate'",
                          clusterId));
              blocked(
                  jdbc,
                  () ->
                      jdbc.update(
                          "insert into kf_external_cluster_nodes(id,cluster_id,agent_id,node_id)"
                              + " values(?,?,'candidate',3)",
                          UUID.randomUUID(),
                          clusterId));
              blocked(
                  jdbc,
                  () ->
                      jdbc.update(
                          "insert into kf_jobs values(?,'ROLLING_RESTART','PENDING',?)",
                          UUID.randomUUID().toString(),
                          "cluster:" + clusterId));
              jdbc.update(
                  "update kf_jobs set status='IN_PROGRESS' where id=?",
                  accepted.jobId().toString());
              ledger.validating(id);
              var task =
                  ledger.dispatch(
                      id, steps.getFirst().getId(), "VALIDATE_ADD_BROKER_PLAN", "VALIDATING");
              assertEquals(
                  task.id(),
                  ledger
                      .dispatch(
                          id, steps.getFirst().getId(), "VALIDATE_ADD_BROKER_PLAN", "VALIDATING")
                      .id());
              var claim = ledger.claim("candidate");
              assertEquals(task.id(), claim.get("id"));
              assertTrue(ledger.claim("candidate").isEmpty());
              var proof =
                  mapper
                      .createObjectNode()
                      .put("schemaVersion", "add-broker/v1")
                      .put("operationId", id.toString())
                      .put("hostId", "candidate")
                      .put("planDigest", payload.digest())
                      .put("contextHash", payload.plan().contextHash())
                      .put("payloadVerified", true)
                      .put("rootMutationSupported", true);
              proof
                  .putArray("supportedActions")
                  .add("PREPARE_ADD_BROKER")
                  .add("FORMAT_ADD_BROKER")
                  .add("START_ADD_BROKER")
                  .add("INSPECT_ADD_BROKER");
              var result =
                  new ExternalBrokerAdditionLedger.Result(
                      (UUID) claim.get("claimToken"), "SUCCESS", proof, null);
              ledger.complete("candidate", task.id(), result);
              ledger.complete("candidate", task.id(), result);
              assertEquals("SUCCESS", ledger.task(task.id()).status());
              assertThrows(
                  org.springframework.web.server.ResponseStatusException.class,
                  () ->
                      ledger.complete(
                          "candidate",
                          task.id(),
                          new ExternalBrokerAdditionLedger.Result(
                              UUID.randomUUID(), "SUCCESS", proof, null)));
              ledger.finalPreview(id, previewId);
              jdbc.update(
                  "update kf_external_add_node_previews set expires_at=now()-interval '1 second'"
                      + " where id=?",
                  previewId);
              assertThrows(
                  org.springframework.web.server.ResponseStatusException.class,
                  () ->
                      ledger.dispatch(id, steps.get(1).getId(), "PREPARE_ADD_BROKER", "PREPARING"));
              jdbc.update(
                  "update kf_external_add_node_previews set expires_at=now()+interval '120 seconds'"
                      + " where id=?",
                  previewId);
              ledger.dispatch(id, steps.get(1).getId(), "PREPARE_ADD_BROKER", "PREPARING");
              assertThrows(
                  org.springframework.web.server.ResponseStatusException.class,
                  () -> ledger.cancelSafe(clusterId, id));
              ledger.recovery(id);
              assertEquals("RECOVERY_REQUIRED", ledger.get(id).state());
              assertTrue(guard.reservedAgent("candidate"));
              assertTrue(ledger.claim("candidate").isEmpty());
              assertEquals(
                  0,
                  jdbc.queryForObject(
                      "select count(*) from kf_external_cluster_nodes", Integer.class));
              assertFalse(ledger.view(clusterId, id).inventoryPublished());
            });
  }

  private static void blocked(JdbcTemplate jdbc, Runnable action) {
    var connection = DataSourceUtils.getConnection(jdbc.getDataSource());
    try {
      var savepoint = connection.setSavepoint();
      assertThrows(org.springframework.dao.DataAccessException.class, action::run);
      connection.rollback(savepoint);
      connection.releaseSavepoint(savepoint);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
