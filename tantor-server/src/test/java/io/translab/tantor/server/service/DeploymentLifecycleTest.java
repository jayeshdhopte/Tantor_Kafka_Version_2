package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeploymentLifecycleTest {
    @Mock JobRepository jobs;
    @Mock JobStepRepository steps;
    @Mock ClusterRepository clusters;
    @Mock ExternalClusterRepository externalClusters;
    JobService service;
    final UUID clusterId = UUID.randomUUID();

    @BeforeEach void setUp() {
        service = new JobService(jobs, steps, new ObjectMapper(), clusters, externalClusters);
    }

    Job job(JobStatus status, boolean servicesOnly) {
        Job job = new Job();
        job.setId(UUID.randomUUID()); job.setResourceKey("cluster:" + clusterId);
        job.setType(JobType.DEPLOYMENT); job.setStatus(status);
        job.setPayload("{\"servicesOnly\":" + servicesOnly + "}");
        return job;
    }

    @Test void activeDeploymentTakesPrecedenceOverAnOlderJob() {
        Job active = job(JobStatus.IN_PROGRESS, false);
        when(jobs.findByResourceKeyAndStatusIn(eq("cluster:" + clusterId), anyList())).thenReturn(List.of(active));
        assertThat(service.getClusterDeploymentJob(clusterId)).contains(active);
        verify(jobs, never()).findFirstByResourceKeyAndTypeOrderByCreatedAtDesc(anyString(), any());
    }

    @Test void latestCompletedDeploymentRemainsAvailableForFailureReporting() {
        Job failed = job(JobStatus.FAILED, false);
        when(jobs.findFirstByResourceKeyAndTypeOrderByCreatedAtDesc("cluster:" + clusterId, JobType.DEPLOYMENT)).thenReturn(Optional.of(failed));
        assertThat(service.getClusterDeploymentJob(clusterId)).contains(failed);
    }

    @Test void addingAServiceIsNotTreatedAsAnInitialClusterDeployment() {
        Job addon = job(JobStatus.IN_PROGRESS, true);
        when(jobs.findByResourceKeyAndStatusIn(eq("cluster:" + clusterId), anyList())).thenReturn(List.of(addon));
        when(jobs.findFirstByResourceKeyAndTypeOrderByCreatedAtDesc("cluster:" + clusterId, JobType.DEPLOYMENT)).thenReturn(Optional.of(addon));
        assertThat(service.getClusterDeploymentJob(clusterId)).isEmpty();
    }

    @Test void lateCallbacksRemainOwnedByTheirDeploymentEvenAfterItFails() {
        Task task = new Task(); task.setId(UUID.randomUUID()); task.setClusterId(clusterId);
        when(steps.existsByAgentTaskIdAndJob_Type(task.getId(), JobType.DEPLOYMENT)).thenReturn(true);
        assertThat(service.ownsDeploymentTask(task)).isTrue();
        verifyNoInteractions(jobs);
    }

    @Test void earlyCallbackIsGuardedBeforeTheTaskIsAttachedToItsStep() {
        Task task = new Task(); task.setId(UUID.randomUUID()); task.setClusterId(clusterId);
        when(jobs.findByResourceKeyAndStatusIn(eq("cluster:" + clusterId), anyList())).thenReturn(List.of(job(JobStatus.IN_PROGRESS, false)));
        assertThat(service.ownsDeploymentTask(task)).isTrue();
    }

    @Test void unrelatedLegacyTasksRetainTheirExistingLifecycleHandling() {
        Task task = new Task(); task.setId(UUID.randomUUID()); task.setClusterId(clusterId);
        assertThat(service.ownsDeploymentTask(task)).isFalse();
    }

    @Test void backendRejectsClusterMutationsWhileDeploymentIsActive() {
        when(jobs.findByResourceKeyAndStatusIn(eq("cluster:" + clusterId), anyList())).thenReturn(List.of(job(JobStatus.IN_PROGRESS, false)));
        assertThatThrownBy(() -> service.assertClusterMutable(clusterId)).isInstanceOf(ClusterOperationConflictException.class);
    }

    @Test void repositoryPropertyPathsResolveAgainstTheActualEntities() {
        assertThatCode(() -> new PartTree("existsByAgentTaskIdAndJob_Type", JobStep.class)).doesNotThrowAnyException();
        assertThatCode(() -> new PartTree("findFirstByResourceKeyAndTypeOrderByCreatedAtDesc", Job.class)).doesNotThrowAnyException();
    }
}
