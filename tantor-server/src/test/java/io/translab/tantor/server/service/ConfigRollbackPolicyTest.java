package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfigRollbackPolicyTest {
    final JobRepository jobs = mock(JobRepository.class);
    final JobStepRepository steps = mock(JobStepRepository.class);
    final ObjectMapper mapper = new ObjectMapper();
    final ConfigRollbackPolicy policy = new ConfigRollbackPolicy(jobs, steps, mapper);
    final String resource = "cluster:" + UUID.randomUUID();
    final Job original = job(JobType.TARGETED_CONFIG_CHANGE, JobStatus.ROLLBACK_FAILED, "2026-11-01T10:00:00Z");
    final Job newer = job(JobType.CONFIG_CHANGE, JobStatus.SUCCESS, "2026-11-01T11:00:00Z");

    Job job(JobType type, JobStatus status, String time) {
        Job j = new Job(); j.setId(UUID.randomUUID()); j.setResourceKey(resource);
        j.setType(type); j.setStatus(status); j.setCreatedAt(Instant.parse(time)); return j;
    }
    Map<String, Object> payload() {
        return new HashMap<>(Map.of("hostId", "host-1", "nodeId", 1, "role", "BROKER",
                "operation", "STATIC", "configPath", "/kafka/broker.properties",
                "changes", Map.of("num.io.threads", "12"), "configMutationStarted", true));
    }
    JobStep step(Job job, Map<String, Object> payload) throws Exception {
        JobStep s = new JobStep(); s.setId(UUID.randomUUID()); s.setJob(job);
        s.setStatus(JobStepStatus.SUCCESS); s.setPayload(mapper.writeValueAsString(payload)); return s;
    }
    void history(JobStep old, JobStep next) {
        when(steps.findByJobIdOrderByStepOrderAsc(original.getId())).thenReturn(List.of(old));
        when(jobs.findConfigRollbackCandidates(eq(resource), eq(original.getId()), anyList(), eq(original.getCreatedAt())))
                .thenReturn(List.of(newer));
        when(steps.findByJob_IdIn(List.of(newer.getId()))).thenReturn(List.of(next));
    }
    @Test void newerSuccessfulChangeBlocksOldRollbackEvenWhenValueIsTheSame() throws Exception {
        history(step(original, payload()), step(newer, payload()));
        var result = policy.evaluate(original);
        assertThat(result.allowed()).isFalse();
        assertThat(result.conflictingJobId()).isEqualTo(newer.getId());
        assertThat(result.reason()).contains("newer configuration change", "Retry rollback is unavailable");
    }
    @Test void unrelatedTargetsRemainRecoverable() throws Exception {
        for (var difference : List.of(Map.entry("hostId", (Object)"host-2"), Map.entry("nodeId", (Object)2),
                Map.entry("role", (Object)"CONTROLLER"), Map.entry("configPath", (Object)"/other.properties"),
                Map.entry("changes", (Object)Map.of("log.retention.hours", "48")))) {
            var next = payload(); next.put(difference.getKey(), difference.getValue());
            if (difference.getKey().equals("hostId")) next.remove("nodeId");
            history(step(original, payload()), step(newer, next));
            assertThat(policy.evaluate(original).allowed()).as(difference.getKey()).isTrue();
        }
    }
    @Test void partiallyWrittenNewerFailedChangeAlsoBlocksRecovery() throws Exception {
        newer.setStatus(JobStatus.FAILED);
        var next = step(newer, payload()); next.setStatus(JobStepStatus.FAILED);
        history(step(original, payload()), next);
        assertThat(policy.evaluate(original).allowed()).isFalse();
    }
    @Test void unmutatedAndRolledBackChangesDoNotInvalidateRecovery() throws Exception {
        var p = payload(); p.put("configMutationStarted", false);
        history(step(original, payload()), step(newer, p));
        assertThat(policy.evaluate(original).allowed()).isTrue();
        newer.setStatus(JobStatus.ROLLED_BACK);
        history(step(original, payload()), step(newer, payload()));
        assertThat(policy.evaluate(original).allowed()).isTrue();
    }
    @Test void alreadyRecoveredNodeIsExcluded() throws Exception {
        var old = step(original, payload()); old.setStatus(JobStepStatus.ROLLED_BACK);
        history(old, step(newer, payload()));
        assertThat(policy.evaluate(original).allowed()).isTrue();
    }
    @Test void missingOrMalformedHistoryFailsClosed() throws Exception {
        var next = step(newer, payload()); next.setPayload("invalid-json");
        history(step(original, payload()), next);
        assertThat(policy.evaluate(original).allowed()).isFalse();
        assertThat(policy.evaluate(original).reason()).contains("Cannot verify");
        when(steps.findByJob_IdIn(List.of(newer.getId()))).thenReturn(List.of());
        assertThat(policy.evaluate(original).allowed()).isFalse();
    }
    @Test void dynamicCheckpointConflictsAcrossFilePaths() throws Exception {
        var p = payload(); p.put("recoveryCheckpoint", Map.of("dynamic", Map.of("num.io.threads", "8")));
        var next = payload(); next.put("configPath", "/different.properties");
        history(step(original, p), step(newer, next));
        assertThat(policy.evaluate(original).allowed()).isFalse();
    }
    @Test void numericLegacyTargetDoesNotBecomeADifferentHost() throws Exception {
        var p = payload(); p.remove("hostId"); var old = step(original, p); old.setTargetId("1");
        history(old, step(newer, payload()));
        assertThat(policy.evaluate(original).allowed()).isFalse();
    }
    @Test void sameNodeWithLegacyHostnameAndInventoryIdStillConflicts() throws Exception {
        var next = payload(); next.put("hostId", "broker.example.com");
        history(step(original, payload()), step(newer, next));
        assertThat(policy.evaluate(original).allowed()).isFalse();
    }
    @Test void legacyRollingWritesUseJobScopeAndIgnoreValidationSteps() throws Exception {
        newer.setType(JobType.ROLLING_CONFIG_UPDATE);
        newer.setPayload(mapper.writeValueAsString(Map.of("role", "BROKER", "changes", List.of(Map.of(
                "host", "host-1", "nodeId", 1, "configFilePath", "/kafka/broker.properties",
                "properties", List.of(Map.of("key", "num.io.threads", "newValue", "16")))))));
        var write = step(newer, payload()); write.setPayload(null); write.setName("WRITE_CONFIG: host-1"); write.setTargetId("host-1");
        history(step(original, payload()), write);
        var validate = step(newer, payload()); validate.setName("FINAL_VERIFY"); validate.setPayload(null);
        when(steps.findByJob_IdIn(List.of(newer.getId()))).thenReturn(List.of(validate, write));
        assertThat(policy.evaluate(original).allowed()).isFalse();
    }
    @Test void staleRetryRejectedBeforeStateChangesOrTaskScheduling() throws Exception {
        history(step(original, payload()), step(newer, payload()));
        when(jobs.findForUpdateById(original.getId())).thenReturn(Optional.of(original));
        var service = new JobService(jobs, steps, mapper, mock(ClusterRepository.class), mock(ExternalClusterRepository.class));
        assertThatThrownBy(() -> service.retryJob(original.getId())).isInstanceOf(ClusterOperationConflictException.class);
        assertThat(original.getStatus()).isEqualTo(JobStatus.ROLLBACK_FAILED);
        assertThat(original.getRetryCount()).isZero();
        verify(jobs, never()).save(any()); verify(steps, never()).save(any());
    }
    @Test void initialRollbackRequestCannotBypassStaleCheck() throws Exception {
        original.setStatus(JobStatus.FAILED); original.setRollbackSupported(true);
        history(step(original, payload()), step(newer, payload()));
        when(jobs.findForUpdateById(original.getId())).thenReturn(Optional.of(original));
        var service = new JobService(jobs, steps, mapper, mock(ClusterRepository.class), mock(ExternalClusterRepository.class));
        assertThatThrownBy(() -> service.requestRollback(original.getId())).isInstanceOf(ClusterOperationConflictException.class);
        verify(jobs, never()).save(any());
    }
    @Test void currentRollbackRetryPreservesCheckpointAndTaskIdentity() throws Exception {
        var old = step(original, payload()); old.setStatus(JobStepStatus.ROLLBACK_FAILED);
        UUID task = UUID.randomUUID(); old.setAgentTaskId(task); String checkpoint = old.getPayload();
        when(steps.findByJobIdOrderByStepOrderAsc(original.getId())).thenReturn(List.of(old));
        when(jobs.findForUpdateById(original.getId())).thenReturn(Optional.of(original));
        when(jobs.findById(original.getId())).thenReturn(Optional.of(original));
        when(jobs.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        var service = new JobService(jobs, steps, mapper, mock(ClusterRepository.class), mock(ExternalClusterRepository.class));
        assertThat(service.retryJob(original.getId()).getStatus()).isEqualTo(JobStatus.ROLLBACK_PENDING);
        assertThat(original.getRetryCount()).isEqualTo(1);
        assertThat(old.getStatus()).isEqualTo(JobStepStatus.ROLLBACK_FAILED);
        assertThat(old.getPayload()).isEqualTo(checkpoint); assertThat(old.getAgentTaskId()).isEqualTo(task);
        verify(steps, never()).save(any());
    }
    @Test void otherJobTypesKeepExistingBehaviour() {
        original.setType(JobType.DEPLOYMENT);
        assertThat(policy.evaluate(original).allowed()).isTrue(); verifyNoInteractions(jobs, steps);
    }
}
