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
    private final TargetedConfigChangeJobHandler handler = new TargetedConfigChangeJobHandler(
            jobs, kafka, health, mock(DeploymentService.class), mock(AgentTaskAwaiter.class), mapper);
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
                "expectedBrokerCount", 1)));
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        when(health.inspect(clusterId, 1, Set.of())).thenReturn(
                new RollingRestartHealthService.HealthSnapshot(true, 1, 101, 0, 0, 0, 0, Map.of()));
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
        verify(jobs).completeStep(step.getId(), "Verified log.segment.bytes on broker 1.");
        verify(jobs, never()).failStep(any(), anyString());
    }

    @Test
    void verifiesAnImmediatelyVisibleChange() {
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"), config("1073741829"));
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
        when(kafka.getBrokerConfigs(clusterId)).thenReturn(config("1073741824"))
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

    private Map<Integer, Map<String, Object>> config(String value) {
        return Map.of(1, Map.of("log.segment.bytes", Map.of("value", value)));
    }
}
