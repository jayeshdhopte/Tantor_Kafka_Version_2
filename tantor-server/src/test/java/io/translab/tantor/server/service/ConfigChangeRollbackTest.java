package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfigChangeRollbackTest {
    final JobService jobs = mock(JobService.class);
    final KafkaAdminService kafka = mock(KafkaAdminService.class);
    final RollingRestartHealthService health = mock(RollingRestartHealthService.class);
    final DeploymentService deployment = mock(DeploymentService.class);
    final AgentTaskAwaiter awaiter = mock(AgentTaskAwaiter.class);
    final ObjectMapper mapper = new ObjectMapper();
    final TargetedConfigChangeJobHandler handler = new TargetedConfigChangeJobHandler(jobs, kafka, health, deployment, awaiter, mapper);
    final UUID cluster = UUID.randomUUID(), read = UUID.randomUUID(), patch = UUID.randomUUID(), verify = UUID.randomUUID();
    final Job job = new Job();

    @BeforeEach void setup() throws Exception {
        job.setId(UUID.randomUUID()); job.setType(JobType.TARGETED_CONFIG_CHANGE);
        job.setPayload(mapper.writeValueAsString(Map.of("clusterId", cluster.toString())));
        when(health.inspect(eq(cluster),eq(3),anySet())).thenReturn(new RollingRestartHealthService.HealthSnapshot(true,3,101,0,0,0,0,Map.of()));
        when(jobs.queueConfigMutation(any(),eq(true),any())).thenAnswer(i -> ((java.util.function.Supplier<?>)i.getArgument(2)).get());
        when(deployment.readKafkaProperties(any(),anyString(),anyString(),anyString(),anyString(),anyString())).thenReturn(read);
        when(deployment.verifyKafkaService(any(),anyString(),anyString())).thenReturn(verify);
        when(awaiter.await(verify,90)).thenReturn(task("{\"state\":\"inactive\"}"));
        when(deployment.rollbackKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),anyMap(),anyMap(),anyString())).thenReturn(patch);
        when(awaiter.await(patch)).thenReturn(task("restored"));
    }
    Task task(String log) { Task t = new Task(); t.setLogOutput(log); return t; }
    JobStep step(Map<String,Object> extra) throws Exception {
        var p = new LinkedHashMap<String,Object>();
        p.put("operation","STATIC");p.put("nodeId",1);p.put("hostId","host-1");p.put("role","broker");
        p.put("expectedBrokerCount",3);p.put("configJson","{}");p.put("configPath","/opt/kafka/config/broker.properties");
        p.put("changes",Map.of("num.io.threads","12"));p.put("configMutationStarted",true);
        p.put("recoveryCheckpoint",Map.of("file",Map.of("num.io.threads","8"),"dynamic",Map.of()));p.putAll(extra);
        JobStep s = new JobStep();s.setId(UUID.randomUUID());s.setJob(job);s.setStepOrder(1);s.setStatus(JobStepStatus.FAILED);s.setPayload(mapper.writeValueAsString(p));
        when(jobs.getSteps(job.getId())).thenReturn(List.of(s));return s;
    }
    @Test void stoppedFailedNodeCanRecoverBeforeFullClusterHealthReturns() throws Exception {
        var s=step(Map.of());
        when(awaiter.await(read,90)).thenReturn(task("{\"properties\":{\"num.io.threads\":\"12\"}}"),task("{\"properties\":{\"num.io.threads\":\"8\"}}"));
        handler.rollback(job);
        verify(deployment).rollbackKafkaConfig(eq(cluster),eq("host-1"),eq("broker"),eq("1"),eq("{}"),anyString(),eq(Map.of("num.io.threads","8")),eq(Map.of("num.io.threads","12")),anyString());
        var order=inOrder(deployment,health);order.verify(deployment).rollbackKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),anyMap(),anyMap(),anyString());order.verify(health,times(2)).inspect(eq(cluster),eq(3),anySet());
        verify(jobs).rolledBackStep(eq(s.getId()),contains("health verified"));
    }
    @Test void restoresAbsentPropertyWithNullRatherThanEmptyString() throws Exception {
        var absent=new LinkedHashMap<String,Object>();absent.put("num.io.threads",null);
        step(Map.of("recoveryCheckpoint",Map.of("file",absent,"dynamic",Map.of())));
        when(awaiter.await(read,90)).thenReturn(task("{\"properties\":{\"num.io.threads\":\"12\"}}"),task("{\"properties\":{}}"));
        handler.rollback(job);
        verify(deployment).rollbackKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),eq(absent),anyMap(),anyString());
    }
    @Test void refusesConcurrentEditWithoutWritingOrRestarting() throws Exception {
        var s=step(Map.of());when(awaiter.await(read,90)).thenReturn(task("{\"properties\":{\"num.io.threads\":\"16\"}}"));
        assertThatThrownBy(()->handler.rollback(job)).hasMessageContaining("outside this job");
        verify(deployment,never()).rollbackKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),anyMap(),anyMap(),anyString());
        verify(jobs).rollbackFailedStep(eq(s.getId()),contains("outside this job"));
    }
    @Test void refusesInflightForwardTask() throws Exception {
        UUID pending=UUID.randomUUID();step(Map.of("configMutationTaskId",pending.toString()));when(awaiter.status(pending)).thenReturn("IN_PROGRESS");
        assertThatThrownBy(()->handler.rollback(job)).hasMessageContaining("still running");
        verifyNoInteractions(deployment);
    }
    @Test void unhealthyClusterCannotRestartAnActiveNodeDuringRollback() throws Exception {
        step(Map.of());when(awaiter.await(read,90)).thenReturn(task("{\"properties\":{\"num.io.threads\":\"12\"}}"));
        when(awaiter.await(verify,90)).thenReturn(task("{\"state\":\"active\"}"));
        when(health.inspect(eq(cluster),eq(3),anySet())).thenReturn(new RollingRestartHealthService.HealthSnapshot(false,2,101,0,82,0,0,Map.of("replicas","82 under-replicated partitions")));
        assertThatThrownBy(()->handler.rollback(job)).hasMessageContaining("under-replicated");
        verify(deployment,never()).rollbackKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),anyMap(),anyMap(),anyString());
    }
    @Test void untouchedNodeDoesNotGetRestarted() throws Exception {
        var s=step(Map.of("configMutationStarted",false));handler.rollback(job);
        verifyNoInteractions(deployment,kafka,health);verify(jobs).rolledBackStep(eq(s.getId()),contains("unchanged"));
    }
    @Test void successfulRecoveryTaskIsVerifiedWithoutRepeatingRestart() throws Exception {
        UUID completed=UUID.randomUUID();step(Map.of("recoveryTaskId",completed.toString()));when(awaiter.status(completed)).thenReturn("SUCCESS");
        when(awaiter.await(read,90)).thenReturn(task("{\"properties\":{\"num.io.threads\":\"8\"}}"));
        handler.rollback(job);
        verify(deployment,never()).rollbackKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),anyMap(),anyMap(),anyString());
    }
    @Test void restoresInheritedDynamicValueByDeletingOverride() throws Exception {
        var original=Map.<String,Object>of("value","8","source","STATIC_BROKER_CONFIG");
        step(Map.of("operation","DYNAMIC","brokerId",1,"recoveryCheckpoint",Map.of("dynamic",Map.of("num.io.threads",original))));
        var changed=Map.<String,Object>of("value","12","source","DYNAMIC_BROKER_CONFIG");
        when(kafka.getBrokerConfigs(cluster)).thenReturn(Map.of(1,Map.of("num.io.threads",changed)),Map.of(1,Map.of("num.io.threads",changed)),Map.of(1,Map.of("num.io.threads",original)));
        when(deployment.backupKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),anyString(),anyMap(),anyString())).thenReturn(patch);
        handler.rollback(job);
        verify(kafka).removeBrokerConfigs(cluster,1,Set.of("num.io.threads"),true);
        verify(kafka).removeBrokerConfigs(cluster,1,Set.of("num.io.threads"),false);
        verify(kafka,never()).alterBrokerConfigs(any(),anyInt(),anyMap());
        verify(deployment,never()).rollbackKafkaConfig(any(),anyString(),anyString(),anyString(),anyString(),anyString(),anyMap(),anyMap(),anyString());
    }
    @Test void recoversFailedNodeBeforeCompletedNodes() throws Exception {
        var completed=step(Map.of("configMutationStarted",false));completed.setStatus(JobStepStatus.SUCCESS);completed.setStepOrder(1);
        var failed=step(Map.of("configMutationStarted",false));failed.setStepOrder(2);
        when(jobs.getSteps(job.getId())).thenReturn(List.of(completed,failed));
        handler.rollback(job);var order=inOrder(jobs);
        order.verify(jobs).rolledBackStep(eq(failed.getId()),anyString());order.verify(jobs).rolledBackStep(eq(completed.getId()),anyString());
    }
}
