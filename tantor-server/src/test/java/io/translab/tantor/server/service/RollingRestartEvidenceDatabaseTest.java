package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import io.translab.tantor.server.repository.*;

/** Isolated committed fixtures exercise immutable evidence without touching a live cluster. */
@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class RollingRestartEvidenceDatabaseTest {
    final JdbcTemplate jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD")));
    final TransactionTemplate tx=new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
    final RollingRestartEvidenceService evidence=new RollingRestartEvidenceService(jdbc,new ObjectMapper());
    final Map<String,Object> settings=Map.of("stableSamples",2,"maxConsumerLag",1000000L,"maxDiskUsedPercent",85.0);
    Job job() {
        Job job=new Job();job.setId(UUID.randomUUID());job.setType(JobType.ROLLING_RESTART);job.setRetryCount(0);
        jdbc.update("INSERT INTO kf_jobs(id,type,status,payload) VALUES (?,'ROLLING_RESTART','SUCCESS','{}')",job.getId().toString());return job;
    }
    UUID step(Job job) {UUID id=UUID.randomUUID();jdbc.update("INSERT INTO kf_job_steps(id,job_id,step_order,name,status) VALUES (?,?,1,'fixture','SUCCESS')",id,job.getId().toString());return id;}
    @Test void capturesActualPolicyOnceAndRejectsChangingItWithinTheSameAttempt() {
        Job job=job();var first=evidence.begin(job,settings);
        assertThat(evidence.begin(job,settings)).isEqualTo(first);
        assertThatThrownBy(()->evidence.begin(job,Map.of("stableSamples",4))).hasMessageContaining("changed within an attempt");
        job.setRetryCount(1);var retry=evidence.begin(job,Map.of("stableSamples",4));
        assertThat(retry.policyId()).isNotEqualTo(first.policyId());assertThat(retry.attempt()).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_policy WHERE job_id=?",Integer.class,job.getId().toString())).isEqualTo(2);
    }
    @Test void retainsEveryFailedAndPassingSampleWithDefinitionAndStepLinks() {
        Job job=job();UUID step=step(job);var context=evidence.begin(job,settings);
        for(String outcome:List.of("FAIL","PASS","ERROR","UNKNOWN","SKIPPED"))
            evidence.check(context,step,"POST_RESTART","quorum",outcome,Map.of("voterCount",3),"observed "+outcome);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_check_result r JOIN kf_rolling_restart_check_definition d USING(definition_version,check_key) WHERE policy_id=? AND step_id=? AND observed_at IS NOT NULL",Integer.class,context.policyId(),step)).isEqualTo(5);
        assertThat(jdbc.queryForList("SELECT DISTINCT outcome FROM kf_rolling_restart_check_result WHERE policy_id=?",String.class,context.policyId())).containsExactlyInAnyOrder("PASS","FAIL","ERROR","UNKNOWN","SKIPPED");
        assertThat(jdbc.queryForObject("SELECT count(distinct sample_id) FROM kf_rolling_restart_check_result WHERE policy_id=?",Integer.class,context.policyId())).isEqualTo(5);
    }
    @Test void preflightIsClusterScopedAndEvidenceSurvivesAFailedJobOutcome() {
        Job job=job();var context=evidence.begin(job,settings);
        evidence.check(context,null,"PREFLIGHT","offlinePartitions","FAIL",Map.of("count",2),"no leader");
        jdbc.update("UPDATE kf_jobs SET status='FAILED' WHERE id=?",job.getId().toString());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_check_result WHERE policy_id=? AND step_id IS NULL AND outcome='FAIL'",Integer.class,context.policyId())).isEqualTo(1);
    }
    @Test void rejectsCrossJobTargetsUnknownDefinitionsAndNonRestartPolicyOwners() {
        Job job=job();var context=evidence.begin(job,settings);UUID wrong=step(job());
        assertThatThrownBy(()->evidence.check(context,wrong,"POST_RESTART","quorum","PASS",Map.of(),null)).hasStackTraceContaining("different job");
        assertThatThrownBy(()->evidence.check(context,null,"PREFLIGHT","invented","PASS",Map.of(),null)).hasStackTraceContaining("foreign key");
        jdbc.update("UPDATE kf_jobs SET type='DEPLOYMENT' WHERE id=?",job.getId().toString());job.setRetryCount(1);
        assertThatThrownBy(()->evidence.begin(job,settings)).hasStackTraceContaining("standalone rolling restart");
    }
    @Test void policyDefinitionsAndResultsAreImmutableAndMissingHistoricalEvidenceIsNotInvented() {
        Job job=job();assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_policy WHERE job_id=?",Integer.class,job.getId().toString())).isZero();
        var context=evidence.begin(job,settings);evidence.check(context,null,"PREFLIGHT","controller","PASS",Map.of("controllerId",1),null);
        for(String statement:List.of("UPDATE kf_rolling_restart_policy SET policy_version='changed' WHERE policy_id=?","DELETE FROM kf_rolling_restart_check_result WHERE policy_id=?"))
            assertThatThrownBy(()->jdbc.update(statement,context.policyId())).hasStackTraceContaining("immutable");
        assertThatThrownBy(()->jdbc.update("UPDATE kf_rolling_restart_check_definition SET rule='invented' WHERE check_key='controller'")).hasStackTraceContaining("immutable");
    }
    @Test void aFailedSampleTransactionDoesNotLeaveAPartialSetOfObservations() {
        var context=evidence.begin(job(),settings);
        var checks=new LinkedHashMap<String,RollingRestartEvidenceService.Check>();
        checks.put("controller",new RollingRestartEvidenceService.Check("PASS",Map.of(),null));
        checks.put("invalid",new RollingRestartEvidenceService.Check("PASS",Map.of(),null));
        assertThatThrownBy(()->tx.executeWithoutResult(ignored->evidence.sample(context,null,"PREFLIGHT",checks))).isInstanceOf(RuntimeException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_check_result WHERE policy_id=?",Integer.class,context.policyId())).isZero();
    }
    @Test void handlerPersistsRecoveryChecksAgainstTheRealDatabaseWithoutRestartingAKafkaProcess() throws Exception {
        Job job=job();UUID stepId=step(job);UUID clusterId=UUID.randomUUID();
        var mapper=new ObjectMapper();job.setPayload(mapper.writeValueAsString(Map.of("clusterId",clusterId)));
        Cluster cluster=new Cluster();cluster.setId(clusterId);cluster.setMode("kraft");
        var services=new ArrayList<ClusterServiceAssignment>();
        for(int node=1;node<=2;node++){var service=new ClusterServiceAssignment();service.setHostId("host-"+node);service.setRole("broker");service.setNodeId(node);services.add(service);}
        cluster.setServices(services);
        var jobs=mock(JobService.class);var deployment=mock(DeploymentService.class);var awaiter=mock(AgentTaskAwaiter.class);
        var clusters=mock(ClusterRepository.class);var hosts=mock(HostRepository.class);var kafka=mock(KafkaAdminService.class);var health=mock(RollingRestartHealthService.class);
        when(clusters.findWithServicesById(clusterId)).thenReturn(Optional.of(cluster));
        Host host=new Host();host.setAgentVersion("test-rollingrestart.1");when(hosts.findById("host-1")).thenReturn(Optional.of(host));
        JobStep step=new JobStep();step.setId(stepId);step.setJob(job);step.setStatus(JobStepStatus.PENDING);step.setAgentTaskId(UUID.randomUUID());
        step.setPayload(mapper.writeValueAsString(Map.of("hostId","host-1","role","broker","nodeId",1)));
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));when(awaiter.status(step.getAgentTaskId())).thenReturn("SUCCESS");when(awaiter.await(step.getAgentTaskId())).thenReturn(new Task());
        UUID probe=UUID.randomUUID();when(deployment.verifyKafkaService(clusterId,"host-1","broker")).thenReturn(probe);
        Task observed=new Task();observed.setLogOutput("{\"state\":\"active\",\"pid\":\"123\"}");when(awaiter.await(probe)).thenReturn(observed);when(awaiter.await(eq(probe),anyLong())).thenReturn(observed);
        when(health.inspect(eq(clusterId),eq(2),anySet())).thenReturn(new RollingRestartHealthService.HealthSnapshot(true,2,101,0,0,0,0,Map.of(),
                Map.of("offlinePartitions",new RollingRestartEvidenceService.Check("PASS",Map.of("count",0),null),"disk",new RollingRestartEvidenceService.Check("UNKNOWN",Map.of(),"unmeasured"))));
        var handler=new RollingRestartJobHandler(jobs,deployment,awaiter,clusters,hosts,kafka,health,evidence,mapper,new RollingRestartTaskDispatcher(deployment,jobs),mock(RestartNodeDisplayService.class));
        org.springframework.test.util.ReflectionTestUtils.setField(handler,"stableSamples",1);
        org.springframework.test.util.ReflectionTestUtils.setField(handler,"healthTimeoutSeconds",2L);
        handler.execute(job);
        verify(deployment,never()).restartService(any(),anyString(),anyString());verify(jobs).completeStep(eq(stepId),anyString());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_check_result r JOIN kf_rolling_restart_policy p USING(policy_id) WHERE p.job_id=?",Integer.class,job.getId().toString())).isEqualTo(6);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_check_result r JOIN kf_rolling_restart_policy p USING(policy_id) WHERE p.job_id=? AND r.step_id=? AND r.check_key='offlinePartitions' AND r.outcome='PASS'",Integer.class,job.getId().toString(),stepId)).isEqualTo(1);
    }

}
