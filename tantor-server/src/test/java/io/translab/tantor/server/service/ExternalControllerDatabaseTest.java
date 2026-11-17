package io.translab.tantor.server.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;

@EnabledIfEnvironmentVariable(
    named = "TANTOR_ADD_NODE_CHECK_URL",
    matches = "jdbc:postgresql://127\\.0\\.0\\.1:65440/.*")
class ExternalControllerDatabaseTest {
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void sharedReservationsLeasesAndAtomicControllerPublication(boolean separate) throws Exception {
    var source =
        new DriverManagerDataSource(System.getenv("TANTOR_ADD_NODE_CHECK_URL"), "postgres", "");
    var jdbc = new JdbcTemplate(source);
    var mapper = new ObjectMapper().findAndRegisterModules();
    var nativePlan =
        separate ? ExternalSeparateProcessTest.frozen() : ExternalControllerRecoveryTest.frozen();
    var p = nativePlan.plan();
    var common =
        new ExternalAddNodeReadinessService.Request(
            p.candidateHostId(),
            p.candidateIp(),
            p.parcelDir(),
            p.artifactId().toString(),
            9092,
            "",
            p.deploymentSettings());
    new TransactionTemplate(new DataSourceTransactionManager(source))
        .executeWithoutResult(
            tx -> {
              tx.setRollbackOnly();
              try {
                String schema =
                    "external_controller_check_" + UUID.randomUUID().toString().replace("-", "");
                jdbc.execute("create schema " + schema);
                jdbc.execute("set local search_path to " + schema);
                jdbc.execute(
                    "create table kf_external_clusters(id uuid primary key,broker_count"
                        + " integer,is_running boolean,status text);create table"
                        + " kf_discovery_agents(id varchar(255) primary key,cluster_id uuid);create"
                        + " table kf_jobs(id varchar(36) primary key,type varchar(64),status"
                        + " varchar(32),resource_key varchar(255));create table kf_job_steps(id"
                        + " uuid primary key,job_id varchar(36));create table"
                        + " kf_external_add_node_previews(id uuid primary key,cluster_id"
                        + " uuid,agent_id varchar(255),context_hash varchar(64),expires_at"
                        + " timestamptz);create table kf_external_add_node_tasks(id uuid primary"
                        + " key,preview_id uuid,status varchar(16),deadline timestamptz);create"
                        + " table kf_external_cluster_nodes(id uuid primary key,cluster_id"
                        + " uuid,agent_id varchar(255),node_id integer,host"
                        + " varchar(255),config_file text,systemd_service text,is_broker"
                        + " boolean,is_controller boolean,jmx_exporter_port integer,port"
                        + " integer,install_dir text,data_dirs text,log_dirs text,can_manage_kafka"
                        + " boolean,can_write_config boolean,can_restart boolean,last_seen"
                        + " timestamptz);create table kf_external_service_tasks(id uuid primary"
                        + " key,agent_id varchar(255),command varchar(40),status"
                        + " varchar(32));create table kf_external_kafka_tasks(id uuid primary"
                        + " key,agent_id varchar(255),command varchar(40),status varchar(32));");
                for (String migration :
                    List.of(
                        "V109__external_broker_addition.sql",
                        "V110__external_broker_verification_recovery.sql",
                        "V111__external_dynamic_controller_addition.sql",
                        "V112__external_separate_process_addition.sql"))
                  try (var stream = getClass().getResourceAsStream("/db/migration/" + migration)) {
                    assertNotNull(stream);
                    jdbc.execute(
                        new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
                  }
                UUID id = p.operationId(), clusterId = p.clusterId(), jobId = UUID.randomUUID();
                jdbc.update(
                    "insert into kf_external_clusters values(?,1,true,'CONNECTED')", clusterId);
                jdbc.update("insert into kf_discovery_agents(id) values(?)", p.candidateHostId());
                for (var member : p.participants()) {
                  jdbc.update(
                      "insert into kf_discovery_agents values(?,?) on conflict do nothing",
                      member.hostId(),
                      clusterId);
                  jdbc.update(
                      "insert into"
                          + " kf_external_cluster_nodes(id,cluster_id,agent_id,node_id,host,config_file,systemd_service,is_broker,is_controller)"
                          + " values(?,?,?,?,?,?,?,?,?)",
                      member.serviceIds().getFirst(),
                      clusterId,
                      member.hostId(),
                      member.nodeId(),
                      member.hostIp(),
                      member.configPath(),
                      member.unit(),
                      member.role().contains("broker"),
                      member.role().contains("controller"));
                }
                jdbc.update(
                    "insert into kf_jobs values(?,'EXTERNAL_CONTROLLER_ADDITION','IN_PROGRESS',?)",
                    jobId.toString(),
                    "cluster:" + clusterId);
                var job = new Job();
                job.setId(jobId);
                job.setType(JobType.EXTERNAL_CONTROLLER_ADDITION);
                job.setStatus(JobStatus.IN_PROGRESS);
                job.setRetryCount(0);
                var steps = new ArrayList<JobStep>();
                for (int i = 0; i < 6; i++) {
                  var step = new JobStep();
                  step.setId(UUID.randomUUID());
                  step.setJob(job);
                  step.setStepOrder(i + 1);
                  steps.add(step);
                  jdbc.update(
                      "insert into kf_job_steps values(?,?)", step.getId(), jobId.toString());
                }
                jdbc.update(
                    "insert into"
                        + " kf_external_controller_previews(id,cluster_id,agent_id,request,context_hash,binding_hash,common_request,plan,plan_digest,expires_at)"
                        + " values(?,?,?,'{}',?,?,?,?,?,clock_timestamp()+interval '120 seconds')",
                    id,
                    clusterId,
                    p.candidateHostId(),
                    p.contextHash(),
                    "b".repeat(64),
                    mapper.writeValueAsString(common),
                    nativePlan.raw(),
                    nativePlan.digest());
                jdbc.update(
                    "insert into"
                        + " kf_external_controller_operations(id,cluster_id,agent_id,node_id,job_id,request_key,request_digest,state,broker_node_id)"
                        + " values(?,?,?,?,?,'fixture-controller-request',?,'VERIFYING',?)",
                    id,
                    clusterId,
                    p.candidateHostId(),
                    p.process("controller").nodeId(),
                    jobId.toString(),
                    "c".repeat(64),
                    separate ? p.process("broker").nodeId() : null);
                var planning = mock(ExternalControllerPlanning.class);
                var release = mock(ExternalControllerRelease.class);
                var jobs = mock(JobService.class);
                var clusters = mock(ExternalClusterRepository.class);
                var readiness = mock(ExternalAddNodeReadinessService.class);
                var artifacts = mock(AddNodeArtifactCatalog.class);
                var evidence = new KafkaExpansionNativeEvidence(mapper);
                var guard = new ExternalBrokerAdditionGuard(jdbc);
                var tasks =
                    new ExternalControllerTasks(
                        jdbc,
                        mapper,
                        readiness,
                        guard,
                        jobs,
                        release,
                        mock(KafkaExpansionRuntimeInspection.class),
                        evidence,
                        artifacts);
                var ledger =
                    new ExternalControllerLedger(
                        planning, tasks, release, jobs, jdbc, evidence, mapper, clusters);
                when(planning.owned(id))
                    .thenReturn(
                        new ExternalControllerPlanning.Frozen(
                            nativePlan, common, "b".repeat(64), Instant.now().plusSeconds(120)));
                when(jobs.getSteps(jobId)).thenReturn(steps);
                when(jobs.getJob(jobId)).thenReturn(job);
                when(readiness.executionBinding(clusterId, common)).thenReturn("b".repeat(64));
                when(artifacts.resolve(p.kafkaVersion(), p.artifactId().toString()))
                    .thenReturn(
                        new AddNodeArtifactCatalog.Artifact(
                            p.artifactId().toString(), p.kafkaVersion(), p.artifactSha256()));
                var cluster = new ExternalCluster();
                cluster.setId(clusterId);
                cluster.setKafkaClusterId(p.kafkaClusterId());
                cluster.setKafkaVersion(p.kafkaVersion());
                when(clusters.findForServiceUpdate(clusterId)).thenReturn(Optional.of(cluster));
                assertTrue(guard.reservedAgent(p.candidateHostId()));
                assertThrows(
                    IllegalStateException.class, () -> guard.assertMutable(clusterId, null));
                assertDoesNotThrow(() -> guard.assertMutable(clusterId, jobId));
                blocked(
                    jdbc,
                    () ->
                        jdbc.update(
                            "update kf_external_controller_previews set plan='{}' where id=?", id));
                blocked(
                    jdbc,
                    () ->
                        jdbc.update(
                            "update kf_external_controller_operations set node_id=node_id+1 where"
                                + " id=?",
                            id));
                blocked(
                    jdbc,
                    () ->
                        jdbc.update(
                            "insert into kf_external_kafka_tasks"
                                + " values(?,'existing-controller','RESTART_SERVICE','PENDING')",
                            UUID.randomUUID()));
                UUID otherJob = UUID.randomUUID();
                jdbc.update("insert into kf_jobs(id) values(?)", otherJob.toString());
                blocked(
                    jdbc,
                    () ->
                        jdbc.update(
                            "insert into"
                                + " kf_external_broker_additions(id,cluster_id,agent_id,job_id,request_key,request_digest,preview_id,node_id,plan)"
                                + " values(?,?,?,?,'other-request-key',?,?,2,'{}')",
                            UUID.randomUUID(),
                            clusterId,
                            p.candidateHostId(),
                            otherJob.toString(),
                            "d".repeat(64),
                            UUID.randomUUID()));
                ledger.failure(id, 0);
                job.setStatus(JobStatus.PARTIAL_SUCCESS);
                jdbc.update(
                    "update kf_jobs set status='PARTIAL_SUCCESS' where id=?", jobId.toString());
                UUID old = UUID.randomUUID(), token = UUID.randomUUID();
                jdbc.update(
                    "insert into"
                        + " kf_external_controller_tasks(id,preview_id,operation_id,step_id,agent_id,command,parameters,status,claim_token,claimed_at,deadline)"
                        + " values(?,?,?,?,?,'ADMIT','{}','RUNNING',?,clock_timestamp(),clock_timestamp()+interval"
                        + " '120 seconds')",
                    old,
                    id,
                    id,
                    steps.get(4).getId(),
                    p.candidateHostId(),
                    token);
                assertThrows(IllegalStateException.class, () -> ledger.resume(clusterId, id));
                assertEquals(0, ledger.get(id).generation());
                jdbc.update(
                    "update kf_external_controller_tasks set deadline=clock_timestamp()-interval"
                        + " '200 seconds' where id=?",
                    old);
                when(jobs.retryExternalBrokerJob(jobId))
                    .thenAnswer(
                        call -> {
                          job.setStatus(JobStatus.PENDING);
                          job.setRetryCount(1);
                          jdbc.update(
                              "update kf_jobs set status='PENDING' where id=?", jobId.toString());
                          return job;
                        });
                ledger.resume(clusterId, id);
                assertEquals(1, ledger.get(id).generation());
                assertThrows(
                    org.springframework.dao.EmptyResultDataAccessException.class,
                    () ->
                        tasks.complete(
                            p.candidateHostId(),
                            old,
                            new ExternalControllerTasks.Result(token, "FAILED", null, "late")));
                assertThrows(
                    IllegalStateException.class,
                    () ->
                        tasks.enqueue(
                            id,
                            id,
                            steps.get(5).getId(),
                            p.candidateHostId(),
                            "INSPECT",
                            tasks.parameters(nativePlan, "INSPECT", null),
                            0));
                job.setStatus(JobStatus.IN_PROGRESS);
                jdbc.update("update kf_jobs set status='IN_PROGRESS' where id=?", jobId.toString());
                ledger.state(id, 1, "VERIFYING");
                var samples = new ArrayList<ExternalControllerLedger.Sample>();
                Instant now = Instant.now();
                for (int offset : new int[] {12, 6, 0}) {
                  UUID task =
                      tasks.enqueue(
                          id,
                          id,
                          steps.get(5).getId(),
                          p.candidateHostId(),
                          "INSPECT",
                          tasks.parameters(nativePlan, "INSPECT", null),
                          1);
                  var claim = tasks.claim(p.candidateHostId(), true, separate);
                  assertEquals(task, claim.get("id"));
                  var report = ExternalControllerRecoveryTest.report(nativePlan);
                  Instant at = now.minusSeconds(offset);
                  report.put("observedAt", at.toString());
                  jdbc.update(
                      "update kf_external_controller_tasks set claimed_at=? where id=?",
                      Timestamp.from(at.minusSeconds(1)),
                      task);
                  var result =
                      new ExternalControllerTasks.Result(
                          (UUID) claim.get("claimToken"), "SUCCESS", report, null);
                  tasks.complete(p.candidateHostId(), task, result);
                  tasks.complete(p.candidateHostId(), task, result);
                  String directory =
                      report.path("observation").path("metadataDirectoryId").asText();
                  var proof =
                      new KafkaExpansionLiveVerifier.ControllerProof(
                          UUID.randomUUID(),
                          id,
                          nativePlan.digest(),
                          p.process("controller").nodeId(),
                          directory,
                          p.voterDigest(),
                          at,
                          false,
                          true);
                  UUID brokerTask = null;
                  if (separate) {
                    brokerTask =
                        tasks.enqueue(
                            id,
                            id,
                            steps.get(5).getId(),
                            p.candidateHostId(),
                            "INSPECT",
                            tasks.parameters(nativePlan, "INSPECT", null, "broker"),
                            1);
                    var brokerClaim = tasks.claim(p.candidateHostId(), false, true);
                    assertEquals(brokerTask, brokerClaim.get("id"));
                    var brokerReport = ExternalSeparateProcessTest.report(nativePlan, "broker");
                    brokerReport.put("observedAt", at.toString());
                    jdbc.update(
                        "update kf_external_controller_tasks set claimed_at=? where id=?",
                        Timestamp.from(at.minusSeconds(1)),
                        brokerTask);
                    tasks.complete(
                        p.candidateHostId(),
                        brokerTask,
                        new ExternalControllerTasks.Result(
                            (UUID) brokerClaim.get("claimToken"), "SUCCESS", brokerReport, null));
                  }
                  samples.add(
                      new ExternalControllerLedger.Sample(
                          task, proof, "full cluster and voter health verified", at, brokerTask));
                }
                var verified =
                    new ExternalControllerLedger.Verified(
                        nativePlan.digest(), samples, Instant.now());
                assertThrows(
                    IllegalStateException.class,
                    () ->
                        ledger.verified(
                            id,
                            1,
                            new ExternalControllerLedger.Verified(
                                nativePlan.digest(), samples.subList(0, 2), Instant.now())));
                ledger.verified(id, 1, verified);
                var connection = DataSourceUtils.getConnection(source);
                var before = connection.setSavepoint();
                doThrow(new IllegalStateException("simulated job callback loss"))
                    .when(jobs)
                    .completeStep(eq(steps.get(5).getId()), anyString());
                assertThrows(
                    IllegalStateException.class, () -> ledger.publish(id, 1, steps.get(5).getId()));
                connection.rollback(before);
                connection.releaseSavepoint(before);
                assertEquals(
                    2,
                    jdbc.queryForObject(
                        "select count(*) from kf_external_cluster_nodes", Integer.class));
                assertNull(
                    jdbc.queryForObject(
                        "select cluster_id from kf_discovery_agents where id=?",
                        UUID.class,
                        p.candidateHostId()));
                assertEquals("PUBLISHING", ledger.get(id).state());
                doNothing().when(jobs).completeStep(eq(steps.get(5).getId()), anyString());
                when(jobs.updateJobStatus(jobId, JobStatus.SUCCESS))
                    .thenAnswer(
                        call -> {
                          jdbc.update(
                              "update kf_jobs set status='SUCCESS' where id=?", jobId.toString());
                          return job;
                        });
                var published = ledger.publish(id, 1, steps.get(5).getId());
                assertEquals("SUCCEEDED", published.state());
                assertEquals(published, ledger.publish(id, 1, steps.get(5).getId()));
                assertEquals(
                    separate ? 4 : 3,
                    jdbc.queryForObject(
                        "select count(*) from kf_external_cluster_nodes", Integer.class));
                assertEquals(
                    separate ? 2 : 1,
                    jdbc.queryForObject(
                        "select broker_count from kf_external_clusters", Integer.class));
                assertEquals(
                    "controller",
                    jdbc.queryForObject(
                        "select systemd_service from kf_external_cluster_nodes where agent_id=? and"
                            + " is_controller=true",
                        String.class,
                        p.candidateHostId()));
                assertFalse(
                    jdbc.queryForObject(
                        "select can_write_config or can_restart from kf_external_cluster_nodes"
                            + " where agent_id=? and is_controller=true",
                        Boolean.class,
                        p.candidateHostId()));
                assertEquals(
                    "SUCCESS",
                    jdbc.queryForObject(
                        "select status from kf_jobs where id=?", String.class, jobId.toString()));
                assertFalse(guard.reservedAgent(p.candidateHostId()));
                assertEquals(
                    separate ? 2 : 1,
                    jdbc.queryForObject(
                        "select count(*) from kf_external_add_node_resources where permanent=true",
                        Integer.class));
              } catch (Exception failure) {
                throw new RuntimeException(failure);
              }
            });
  }

  static void blocked(JdbcTemplate jdbc, Runnable action) {
    var connection = DataSourceUtils.getConnection(jdbc.getDataSource());
    try {
      var point = connection.setSavepoint();
      assertThrows(org.springframework.dao.DataAccessException.class, action::run);
      connection.rollback(point);
      connection.releaseSavepoint(point);
    } catch (SQLException failure) {
      throw new RuntimeException(failure);
    }
  }
}
