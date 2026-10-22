package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfigRecoveryCheckpointTest {
    final JobRepository jobs=mock(JobRepository.class);
    final JobStepRepository steps=mock(JobStepRepository.class);
    final ObjectMapper mapper=new ObjectMapper();
    final JobService service=new JobService(jobs,steps,mapper,mock(ClusterRepository.class),mock(ExternalClusterRepository.class));
    JobStep step() {
        Job job=new Job();job.setType(JobType.TARGETED_CONFIG_CHANGE);
        JobStep step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setPayload("{}");
        when(steps.findById(step.getId())).thenReturn(Optional.of(step));return step;
    }
    @Test void retryCannotOverwriteOriginalCheckpoint() throws Exception {
        var step=step();
        service.checkpointConfigChange(step.getId(),Map.of("file",Map.of("num.io.threads","8")));
        service.checkpointConfigChange(step.getId(),Map.of("file",Map.of("num.io.threads","12")));
        assertThat(step.getPayload()).contains("\"num.io.threads\":\"8\"").doesNotContain("\"num.io.threads\":\"12\"");
    }
    @Test void mutationTaskAndRecoveryTaskHaveSeparateDurableIdentities() throws Exception {
        var step=step();service.checkpointConfigChange(step.getId(),Map.of("file",Map.of("num.io.threads","8")));
        UUID forward=UUID.randomUUID(),rollback=UUID.randomUUID();
        service.queueConfigMutation(step.getId(),false,()->forward);
        service.queueConfigMutation(step.getId(),true,()->rollback);
        var payload=mapper.readTree(step.getPayload());
        assertThat(payload.get("configMutationTaskId").asText()).isEqualTo(forward.toString());
        assertThat(payload.get("recoveryTaskId").asText()).isEqualTo(rollback.toString());
        assertThat(payload.get("configMutationStarted").asBoolean()).isTrue();
        assertThat(step.getAgentTaskId()).isEqualTo(rollback);
    }
    @Test void mutationCannotBeQueuedWithoutCheckpoint() {
        var step=step();
        assertThatThrownBy(()->service.queueConfigMutation(step.getId(),false,UUID::randomUUID)).hasMessageContaining("checkpoint is missing");
        verify(steps,never()).save(step);
    }
}
