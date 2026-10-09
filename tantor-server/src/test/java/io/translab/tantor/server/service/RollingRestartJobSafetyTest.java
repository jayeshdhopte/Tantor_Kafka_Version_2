package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RollingRestartJobSafetyTest {
    @Test void retryPreservesCompletedStepsAndRestartEvidenceAndRejectsRollback() {
        var jobs = mock(JobRepository.class); var steps = mock(JobStepRepository.class); var clusters = mock(ClusterRepository.class);
        var service = new JobService(jobs, steps, new ObjectMapper(), clusters);
        Job job = new Job(); job.setId(UUID.randomUUID()); job.setType(JobType.ROLLING_RESTART); job.setStatus(JobStatus.PARTIAL_SUCCESS);
        job.setResourceKey("cluster:" + UUID.randomUUID()); job.setRollbackSupported(true);
        JobStep done = new JobStep(); done.setStatus(JobStepStatus.SUCCESS);
        JobStep failed = new JobStep(); failed.setStatus(JobStepStatus.FAILED); failed.setAgentTaskId(UUID.randomUUID());
        UUID task = failed.getAgentTaskId();
        when(jobs.findById(job.getId())).thenReturn(Optional.of(job));
        when(jobs.findForUpdateById(job.getId())).thenReturn(Optional.of(job));
        when(jobs.findByResourceKeyAndStatusIn(anyString(), anyList())).thenReturn(List.of());
        when(steps.findByJobIdOrderByStepOrderAsc(job.getId())).thenReturn(List.of(done, failed));
        service.retryJob(job.getId());
        assertThat(done.getStatus()).isEqualTo(JobStepStatus.SUCCESS);
        assertThat(failed.getAgentTaskId()).isEqualTo(task);
        assertThat(failed.getStatus()).isEqualTo(JobStepStatus.PENDING);
        job.setStatus(JobStatus.FAILED);
        assertThatThrownBy(() -> service.requestRollback(job.getId())).hasMessageContaining("not supported");
    }
    @Test void agentBaselineIsStoredInPayloadWithoutUsingExternalTaskForeignKey() throws Exception {
        var jobs = mock(JobRepository.class); var steps = mock(JobStepRepository.class);
        ObjectMapper mapper = new ObjectMapper();
        var service = new JobService(jobs, steps, mapper, mock(ClusterRepository.class));
        Job job = new Job(); job.setType(JobType.ROLLING_RESTART);
        JobStep step = new JobStep(); step.setId(UUID.randomUUID()); step.setJob(job);
        step.setPayload(mapper.writeValueAsString(Map.of("hostId", "one", "role", "broker", "nodeId", 1)));
        when(steps.findById(step.getId())).thenReturn(Optional.of(step));
        UUID baselineTask = UUID.randomUUID();
        var snapshot = Map.<String, Object>of("state", "active", "pid", "123", "started", "456");
        service.recordRollingRestartBaseline(step.getId(), baselineTask, snapshot);
        var stored = mapper.readTree(step.getPayload());
        assertThat(stored.get("hostId").asText()).isEqualTo("one");
        assertThat(stored.get("baselineAgentTaskId").asText()).isEqualTo(baselineTask.toString());
        assertThat(stored.get("baselineServiceState").get("pid").asText()).isEqualTo("123");
        assertThat(step.getExternalServiceTaskId()).isNull();
        assertThat(step.getAgentTaskId()).isNull();
        verify(steps).save(step);
    }

    @Test void queuedRestartLocksSameClusterButNotAnotherCluster() {
        var jobs = mock(JobRepository.class); var clusters = mock(ClusterRepository.class);
        var service = new JobService(jobs, mock(JobStepRepository.class), new ObjectMapper(), clusters);
        UUID cluster = UUID.randomUUID(); Job active = new Job(); active.setId(UUID.randomUUID()); active.setType(JobType.ROLLING_RESTART);
        when(jobs.findByResourceKeyAndStatusIn(eq("cluster:" + cluster), anyList())).thenReturn(List.of(active));
        assertThatThrownBy(() -> service.assertClusterMutable(cluster)).isInstanceOf(ClusterOperationConflictException.class);
        service.assertClusterMutable(UUID.randomUUID());
    }
    @Test void resumeSuccessfulRestartOnlyChecksHealthAndDoesNotMarkClusterFailed() throws Exception {
        var jobs = mock(JobService.class); var deploy = mock(DeploymentService.class); var awaiter = mock(AgentTaskAwaiter.class);
        var clusters = mock(ClusterRepository.class); var hosts = mock(HostRepository.class); var kafka = mock(KafkaAdminService.class);
        var health = mock(RollingRestartHealthService.class); var mapper = new ObjectMapper();
        var handler = new RollingRestartJobHandler(jobs, deploy, awaiter, clusters, hosts, kafka, health, mapper, new RollingRestartTaskDispatcher(deploy, jobs));
        ReflectionTestUtils.setField(handler, "healthTimeoutSeconds", 1L);
        ReflectionTestUtils.setField(handler, "stableSamples", 1);
        Cluster cluster = new Cluster(); cluster.setId(UUID.randomUUID()); cluster.setMode("kraft"); cluster.setStatus("SUCCESS");
        var first = new ClusterServiceAssignment(); first.setHostId("one"); first.setRole("broker"); first.setNodeId(1);
        var second = new ClusterServiceAssignment(); second.setHostId("two"); second.setRole("broker"); second.setNodeId(2);
        cluster.setServices(List.of(first, second));
        Job job = new Job(); job.setId(UUID.randomUUID()); job.setPayload(mapper.writeValueAsString(Map.of("clusterId", cluster.getId())));
        JobStep step = new JobStep(); step.setId(UUID.randomUUID()); step.setStatus(JobStepStatus.PENDING); step.setAgentTaskId(UUID.randomUUID());
        step.setPayload(mapper.writeValueAsString(Map.of("hostId", "one", "role", "broker", "nodeId", 1)));
        when(clusters.findWithServicesById(cluster.getId())).thenReturn(Optional.of(cluster)); when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        Host host = new Host(); host.setAgentVersion("1.0-rollingrestart.1"); when(hosts.findById("one")).thenReturn(Optional.of(host));
        when(awaiter.status(step.getAgentTaskId())).thenReturn("SUCCESS"); when(awaiter.await(step.getAgentTaskId())).thenReturn(new Task());
        UUID probe = UUID.randomUUID(); when(deploy.verifyKafkaService(cluster.getId(), "one", "broker")).thenReturn(probe);
        Task verified = new Task(); verified.setLogOutput("{\"state\":\"active\",\"pid\":\"2\",\"started\":\"200\"}"); when(awaiter.await(probe)).thenReturn(verified);
        when(health.inspect(eq(cluster.getId()), eq(2), anySet())).thenReturn(new RollingRestartHealthService.HealthSnapshot(true, 2, 101, 0, 0, 0, 0, Map.of()));
        handler.execute(job);
        verify(deploy, never()).restartService(any(), anyString(), anyString());
        verify(jobs).completeStep(eq(step.getId()), anyString()); verify(clusters, never()).save(any());
        // An ambiguous failure with a different process identity also resumes without another restart.
        Map<String, Object> baseline = new HashMap<>(Map.of("state", "active", "pid", "1", "started", "100"));
        step.setPayload(mapper.writeValueAsString(Map.of("hostId", "one", "role", "broker", "nodeId", 1, "baselineServiceState", baseline)));
        when(awaiter.status(step.getAgentTaskId())).thenReturn("FAILED");
        handler.execute(job);
        verify(deploy, never()).restartService(any(), anyString(), anyString());
        // A failed restart that left the original process alive must actually retry the restart.
        baseline.put("pid", "2"); baseline.put("started", "200");
        step.setPayload(mapper.writeValueAsString(Map.of("hostId", "one", "role", "broker", "nodeId", 1, "baselineServiceState", baseline)));
        UUID retriedTask = UUID.randomUUID();
        when(deploy.restartService(cluster.getId(), "one", "broker")).thenReturn(retriedTask);
        when(awaiter.await(retriedTask)).thenReturn(new Task());
        handler.execute(job);
        verify(deploy).restartService(cluster.getId(), "one", "broker");
        verify(health).assertBrokerCanStop(cluster.getId(), 1);
        verify(jobs).recordRollingRestartBaseline(eq(step.getId()), eq(probe), anyMap());
        verify(jobs, never()).attachExternalServiceTask(any(), any());
        verify(clusters, never()).save(any());
    }
}
