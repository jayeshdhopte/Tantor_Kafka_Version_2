package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TargetedConfigChangeJobHandlerTest {
    private final JobService jobs = mock(JobService.class);
    private final KafkaAdminService kafka = mock(KafkaAdminService.class);
    private final RollingRestartHealthService health = mock(RollingRestartHealthService.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final DeploymentService deployment = mock(DeploymentService.class);
    private final AgentTaskAwaiter awaiter = mock(AgentTaskAwaiter.class);
    private final UUID backupId = UUID.randomUUID();
    private final TargetedConfigChangeJobHandler handler = new TargetedConfigChangeJobHandler(
            jobs, kafka, health, deployment, awaiter, mapper);
    private final UUID clusterId = UUID.randomUUID();
    private final Job job = new Job();
    private final JobStep step = new JobStep();

    @BeforeEach
    void setup() throws Exception {
        job.setId(UUID.randomUUID());
        job.setPayload(mapper.writeValueAsString(Map.of("clusterId", clusterId.toString())));
        step.setId(UUID.randomUUID());
        step.setStatus(JobStepStatus.PENDING);
        step.setPayload(mapper.writeValueAsString(Map.of("operation", "DYNAMIC", "brokerId", 1,
                "key", "log.segment.bytes", "value", "1073741829", "expectedOldValue", "1073741824",
                "expectedBrokerCount", 1, "hostId", "host-1", "role", "broker", "configPath", "/opt/kafka/config/broker.properties", "configJson", "{}")));
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        when(health.inspect(clusterId, 1, Set.of())).thenReturn(
                new RollingRestartHealthService.HealthSnapshot(true, 1, 101, 0, 0, 0, 0, Map.of()));
        when(deployment.backupKafkaConfig(eq(clusterId), eq("host-1"), eq("broker"), eq("1"), eq("{}"), eq("/opt/kafka/config/broker.properties"), eq("log.segment.bytes"), anyMap(), anyString())).thenReturn(backupId);
        Task backup = new Task(); backup.setStatus("SUCCESS"); backup.setLogOutput("Checkpoint /opt/kafka/config/.tantor-backups/broker.properties/example.bak");
        when(awaiter.await(backupId)).thenReturn(backup);
        ReflectionTestUtils.setField(handler, "dynamicVerificationTimeoutMs", 1000L);
        ReflectionTestUtils.setField(handler, "dynamicVerificationPollMs", 1L);
    }

    @Test
    void waitsForBrokerReadbackWithoutApplyingTheChangeAgain() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"),
                config("1073741824"), config("1073741824"), config("1073741829"));

        handler.execute(job);

        verify(kafka, times(1)).alterBrokerConfig(clusterId, 1, "log.segment.bytes", "1073741829");
        verify(kafka, times(4)).getBrokerConfigs(clusterId);
        verify(jobs).completeStep(eq(step.getId()), startsWith("Verified log.segment.bytes on broker 1."));
        verify(jobs, never()).failStep(any(), anyString());
    }

    @Test
    void verifiesAnImmediatelyVisibleChange() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"), config("1073741824"), config("1073741829"));
        handler.execute(job);
        verify(jobs).completeStep(eq(step.getId()), anyString());
    }

    @Test
    void alreadyAppliedChangeIsVerifiedWithoutAnotherWrite() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741829"));
        handler.execute(job);
        verify(kafka, never()).alterBrokerConfig(any(), anyInt(), anyString(), anyString());
        verify(jobs).completeStep(eq(step.getId()), anyString());
    }

    @Test
    void retriesATransientReadFailureAfterTheWrite() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"),config("1073741824"))
                .thenThrow(new IllegalStateException("Temporary read failure"))
                .thenReturn(config("1073741829"));
        handler.execute(job);
        verify(kafka, times(1)).alterBrokerConfig(clusterId, 1, "log.segment.bytes", "1073741829");
        verify(jobs).completeStep(eq(step.getId()), anyString());
    }

    @Test
    void timesOutWithTheDesiredAndObservedValuesAndStopsLaterTargets() throws Exception {
        ReflectionTestUtils.setField(handler, "dynamicVerificationTimeoutMs", 0L);
        JobStep next = new JobStep();
        next.setId(UUID.randomUUID());
        next.setPayload(mapper.writeValueAsString(Map.of("brokerId", 2, "key", "log.segment.bytes",
                "value", "1073741829", "expectedOldValue", "1073741824", "expectedBrokerCount", 1)));
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step, next));
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"));

        assertThatThrownBy(() -> handler.execute(job)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expected=1073741829")
                .hasMessageContaining("last observed value=1073741824")
                .hasMessageContaining("change may already be applied");
        verify(jobs).failStep(eq(step.getId()), contains("last observed value=1073741824"));
        verify(jobs, never()).startStep(next.getId());
        verify(jobs, never()).completeStep(any(), anyString());
    }

    @Test
    void rejectsConcurrentValueChangesBeforeWriting() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741800"));
        assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("changed since preview");
        verify(kafka, never()).alterBrokerConfig(any(), anyInt(), anyString(), anyString());
    }

    @Test
    void stopsVerificationOnInterruption() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741829"));
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("Interrupted");
            verify(jobs, never()).completeStep(any(), anyString());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void backupFailurePreventsAnyKafkaWrite() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"));
        when(awaiter.await(backupId)).thenThrow(new IllegalStateException("Backup disk full"));
        assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("Backup disk full");
        verify(kafka, never()).alterBrokerConfig(any(), anyInt(), anyString(), anyString());
    }

    @Test
    void waitsForBackupBeforeMutationAndRejectsAnInterveningChange() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"), config("1073741800"));
        assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("changed while backing up");
        verify(awaiter).await(backupId);
        verify(kafka, never()).alterBrokerConfig(any(), anyInt(), anyString(), anyString());
    }

    @Test
    void successfulWriteHappensAfterBackupCompletion() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"), config("1073741824"), config("1073741829"));
        handler.execute(job);
        var order = inOrder(deployment, awaiter, kafka);
        order.verify(deployment).backupKafkaConfig(any(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyMap(), anyString());
        order.verify(awaiter).await(backupId);
        order.verify(kafka).alterBrokerConfig(clusterId, 1, "log.segment.bytes", "1073741829");
        verify(jobs).completeStep(eq(step.getId()), contains(".bak"));
    }

    private void batchSetup(String operation) throws Exception {
        var payload = new java.util.LinkedHashMap<String, Object>();
        payload.put("operation", operation); payload.put("brokerId", 1); payload.put("nodeId", 1);
        payload.put("changes", Map.of("log.segment.bytes", "200", "log.retention.hours", "72"));
        payload.put("expectedValues", Map.of("log.segment.bytes", "100", "log.retention.hours", "168"));
        payload.put("expectedBrokerCount", 1); payload.put("hostId", "host-1"); payload.put("role", "broker");
        payload.put("configPath", "/opt/kafka/config/broker.properties"); payload.put("configJson", "{}");
        step.setPayload(mapper.writeValueAsString(payload));
        when(health.inspect(clusterId, 1, Set.of("host-1"))).thenReturn(new RollingRestartHealthService.HealthSnapshot(true, 1, 101, 0, 0, 0, 0, Map.of()));
        when(deployment.backupKafkaConfig(eq(clusterId), eq("host-1"), eq("broker"), eq("1"), eq("{}"), anyString(), eq("batch"), anyMap(), anyString())).thenReturn(backupId);
    }
    private Map<Integer, Map<String, Object>> batchConfig(String segment, String retention) {
        return Map.of(1, Map.of("log.segment.bytes", Map.of("value", segment), "log.retention.hours", Map.of("value", retention)));
    }
    @Test void batchIsBackedUpBeforeOneKafkaWriteAndVerifiesAllProperties() throws Exception {
        batchSetup("DYNAMIC");
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(batchConfig("100", "168"), batchConfig("100", "168"), batchConfig("200", "72"));
        handler.execute(job);
        var order = inOrder(awaiter, kafka);
        order.verify(awaiter).await(backupId);
        order.verify(kafka).alterBrokerConfigs(clusterId, 1, Map.of("log.segment.bytes", "200", "log.retention.hours", "72"));
        verify(deployment).backupKafkaConfig(any(), anyString(), anyString(), anyString(), anyString(), anyString(), eq("batch"),
                argThat(snapshot -> "batch".equals(snapshot.get("key")) && "DYNAMIC".equals(snapshot.get("mode"))), anyString());
        verify(jobs).completeStep(eq(step.getId()), contains("2 properties"));
    }
    @Test void batchSnapshotsSatisfyExistingAgentContractAndChunkUtf8ValuesWithoutLoss() throws Exception {
        var requested = new java.util.LinkedHashMap<String, String>();
        var previous = new java.util.LinkedHashMap<String, Object>();
        for (int i = 0; i < 10; i++) {
            String key = "test.property." + i;
            requested.put(key, "\u0915".repeat(2000)); previous.put(key, Map.of("value", "a".repeat(4096)));
        }
        var snapshots = handler.dynamicSnapshots(clusterId, 1, previous, requested);
        org.assertj.core.api.Assertions.assertThat(snapshots.size()).isGreaterThan(1);
        var restored = new java.util.LinkedHashMap<String, Object>();
        for (var snapshot : snapshots) {
            org.assertj.core.api.Assertions.assertThat(snapshot.get("key")).isEqualTo("batch");
            org.assertj.core.api.Assertions.assertThat(snapshot.get("mode")).isEqualTo("DYNAMIC");
            org.assertj.core.api.Assertions.assertThat(mapper.writeValueAsBytes(snapshot).length).isLessThanOrEqualTo(16384);
            ((Map<?, ?>) snapshot.get("changes")).forEach((key, value) -> restored.put(String.valueOf(key), value));
        }
        org.assertj.core.api.Assertions.assertThat(restored).isEqualTo(requested);
        previous.put("test.property.0", Map.of("value", "x".repeat(17000)));
        assertThatThrownBy(() -> handler.dynamicSnapshots(clusterId, 1, previous, requested)).hasMessageContaining("16 KiB");
    }

    @Test void batchBackupFailureAndConcurrentEditPreventAllWrites() throws Exception {
        batchSetup("DYNAMIC"); when(kafka.getBrokerConfigs(clusterId)).thenReturn(batchConfig("100", "168"));
        when(awaiter.await(backupId)).thenThrow(new IllegalStateException("Backup disk full"));
        assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("Backup disk full");
        verify(kafka, never()).alterBrokerConfigs(any(), anyInt(), anyMap());
        Task backup = new Task(); backup.setLogOutput("saved"); doReturn(backup).when(awaiter).await(backupId);
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(batchConfig("100", "168"), batchConfig("100", "96"));
        assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("changed while backing up");
        verify(kafka, never()).alterBrokerConfigs(any(), anyInt(), anyMap());
    }
    @Test void staticBatchPatchesAndRestartsOnceWithBothProperties() throws Exception {
        batchSetup("STATIC");
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(batchConfig("200", "72"));
        UUID patch = UUID.randomUUID(); Task complete = new Task(); complete.setLogOutput("patched and restarted");
        when(deployment.patchKafkaConfig(any(), anyString(), anyString(), anyString(), anyString(), anyString(), anyMap(), anyMap(), eq(true), anyString())).thenReturn(patch);
        when(awaiter.await(patch)).thenReturn(complete);
        handler.execute(job);
        verify(deployment, times(1)).patchKafkaConfig(eq(clusterId), eq("host-1"), eq("broker"), eq("1"), eq("{}"), anyString(),
                eq(Map.of("log.segment.bytes", "200", "log.retention.hours", "72")), eq(Map.of("log.segment.bytes", "100", "log.retention.hours", "168")), eq(true), anyString());
        verify(jobs).completeStep(eq(step.getId()), anyString());
    }

    private Map<Integer, Map<String, Object>> config(String value) {
        return Map.of(1, Map.of("log.segment.bytes", Map.of("value", value)));
    }
}
