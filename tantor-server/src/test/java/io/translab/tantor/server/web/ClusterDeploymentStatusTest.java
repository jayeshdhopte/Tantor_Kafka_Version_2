package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.BrokerSummaryDto;
import io.translab.tantor.server.service.BrokerMetricsCacheService;
import io.translab.tantor.server.service.JobService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClusterDeploymentStatusTest {
    @Mock JobService jobService;
    @Mock BrokerMetricsCacheService brokerMetricsCacheService;
    @InjectMocks ClusterController controller;

    Cluster cluster() { Cluster c = new Cluster(); c.setId(UUID.randomUUID()); c.setStatus("SUCCESS"); return c; }
    Job job(Cluster c, JobStatus status) {
        Job j = new Job(); j.setId(UUID.randomUUID()); j.setType(JobType.DEPLOYMENT); j.setStatus(status);
        when(jobService.getClusterDeploymentJob(c.getId())).thenReturn(Optional.of(j));
        return j;
    }
    Map<String, Object> health(Cluster c) { return ReflectionTestUtils.invokeMethod(controller, "internalRuntimeHealth", c); }

    @ParameterizedTest
    @EnumSource(value = JobStatus.class, names = {"PENDING", "IN_PROGRESS", "ROLLBACK_PENDING", "ROLLING_BACK"})
    void showsDeployingAndRetainsLockEvenIfAnAgentPreviouslyMarkedClusterSuccessful(JobStatus status) {
        Cluster c = cluster(); Job j = job(c, status);
        Map<String, Object> result = health(c);
        assertThat(result).containsEntry("runtimeStatusLabel", "Deploying")
                .containsEntry("deploymentInProgress", true).containsEntry("deploymentFailed", false)
                .containsEntry("deploymentJobId", j.getId());
        verifyNoInteractions(brokerMetricsCacheService);
    }

    @ParameterizedTest
    @EnumSource(value = JobStatus.class, names = {"FAILED", "PARTIAL_SUCCESS", "ROLLBACK_FAILED"})
    void deploymentFailureShowsKafkaCheckFailed(JobStatus status) {
        Cluster c = cluster(); job(c, status);
        assertThat(health(c)).containsEntry("runtimeStatusLabel", "Kafka Check Failed")
                .containsEntry("deploymentInProgress", false).containsEntry("deploymentFailed", true);
        verifyNoInteractions(brokerMetricsCacheService);
    }

    @Test void failedStepIsReportedBeforeTheJobWorkerFinishesUpdatingTheParentJob() {
        Cluster c = cluster(); Job j = job(c, JobStatus.IN_PROGRESS);
        JobStep failed = new JobStep(); failed.setStatus(JobStepStatus.FAILED);
        when(jobService.getSteps(j.getId())).thenReturn(List.of(failed));
        assertThat(health(c)).containsEntry("runtimeStatusLabel", "Kafka Check Failed")
                .containsEntry("deploymentInProgress", true).containsEntry("deploymentFailed", true);
    }

    @Test void finishedDeploymentUnlocksAndUsesRealBrokerHealthAgain() {
        Cluster c = cluster(); job(c, JobStatus.SUCCESS);
        when(brokerMetricsCacheService.getBrokerSummaries(c)).thenReturn(List.of(BrokerSummaryDto.builder().role("broker").brokerHealth("HEALTHY").build()));
        assertThat(health(c)).containsEntry("runtimeStatusLabel", "Active")
                .containsEntry("deploymentInProgress", false).containsEntry("deploymentFailed", false);
        verify(brokerMetricsCacheService).getBrokerSummaries(c);
    }

    @Test void deletionIsNotOverriddenByAnOldFailedDeployment() {
        Cluster c = cluster(); c.setStatus("DELETING"); job(c, JobStatus.FAILED);
        assertThat(health(c)).containsEntry("runtimeStatusLabel", "DELETING");
    }
}
