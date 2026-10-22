package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class VersionedConfigRecoveryTest {
    final ConfigChangeJobHandler handler=mock(ConfigChangeJobHandler.class,CALLS_REAL_METHODS);
    final JobService jobs=mock(JobService.class);
    final ConfigVersionService versions=mock(ConfigVersionService.class);
    final TargetedConfigChangeJobHandler targeted=mock(TargetedConfigChangeJobHandler.class);
    final ObjectMapper mapper=new ObjectMapper();
    final UUID cluster=UUID.randomUUID(),version=UUID.randomUUID();
    Job setup() throws Exception {
        ReflectionTestUtils.setField(handler,"jobService",jobs);
        ReflectionTestUtils.setField(handler,"configVersionService",versions);
        ReflectionTestUtils.setField(handler,"targetedChanges",targeted);
        ReflectionTestUtils.setField(handler,"clusterRepository",mock(ClusterRepository.class));
        ReflectionTestUtils.setField(handler,"objectMapper",mapper);
        Job job=new Job();job.setId(UUID.randomUUID());job.setType(JobType.CONFIG_CHANGE);
        job.setPayload(mapper.writeValueAsString(Map.of("clusterId",cluster.toString(),"configVersionId",version.toString(),"checkpointRecovery",true)));
        return job;
    }
    @Test void versionedFileChangeUsesSameCheckpointedExecution() throws Exception {
        Job job=setup();JobStep step=new JobStep();step.setId(UUID.randomUUID());step.setStatus(JobStepStatus.PENDING);
        step.setPayload("{\"checkpointRecovery\":true,\"operation\":\"patch_service\",\"role\":\"broker\"}");
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        handler.execute(job);
        verify(targeted).executeStatic(eq(cluster),eq(step),anyMap());
        verify(versions).markApplied(eq(version),anyString());
    }
    @Test void versionedRollbackUsesFailedNodeRecoveryAndRetainsVersionHistory() throws Exception {
        Job job=setup();handler.rollback(job);
        var order=inOrder(targeted,versions);order.verify(targeted).rollback(job);order.verify(versions).markJobRolledBack(version);
    }
}
