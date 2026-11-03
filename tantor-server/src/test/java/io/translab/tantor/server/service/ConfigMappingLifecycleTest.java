package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfigMappingLifecycleTest {
    final JobRepository jobs=mock(JobRepository.class);
    final JobStepRepository steps=mock(JobStepRepository.class);
    final ConfigChangeMappingService mapping=mock(ConfigChangeMappingService.class);
    final JobService service=new JobService(jobs,steps,new ObjectMapper(),mock(ClusterRepository.class),mock(ExternalClusterRepository.class),mock(RollingRestartMappingService.class),mapping);
    Job job(JobType type){var job=new Job();job.setId(UUID.randomUUID());job.setType(type);job.setPayload("{}");when(jobs.findById(job.getId())).thenReturn(Optional.of(job));return job;}

    @ParameterizedTest @EnumSource(value=JobType.class,names={"TARGETED_CONFIG_CHANGE","CONFIG_CHANGE","ROLLING_CONFIG_UPDATE"})
    void everyConfigCreationPathRecordsTheSamePersistedJobAndSteps(JobType type){
        var job=job(type);when(jobs.saveAndFlush(job)).thenReturn(job);
        service.createJob(job,List.of());verify(mapping).record(job,List.of());verify(jobs).saveAndFlush(job);
    }
    @ParameterizedTest @EnumSource(value=JobStatus.class,names={"SUCCESS","FAILED","PARTIAL_SUCCESS","ROLLED_BACK"})
    void terminalOutcomesSynchronizeTheConfigSummary(JobStatus status){
        var job=job(JobType.TARGETED_CONFIG_CHANGE);service.updateJobStatus(job.getId(),status);
        verify(mapping).status(job);assertThat(job.getEndTime()).isNotNull();
    }
    @Test void startCheckpointMutationAndCompletionKeepTheOriginalPayloadAndNotifyTheProjection(){
        var job=job(JobType.TARGETED_CONFIG_CHANGE);var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setName("Apply log.retention.hours");step.setPayload("{\"key\":\"log.retention.hours\",\"value\":\"72\"}");
        job.setStatus(JobStatus.IN_PROGRESS);when(jobs.findForUpdateById(job.getId())).thenReturn(Optional.of(job));
        when(steps.findById(step.getId())).thenReturn(Optional.of(step));when(steps.findByJobIdOrderByStepOrderAsc(job.getId())).thenReturn(List.of(step));
        service.startStep(step.getId());service.checkpointConfigChange(step.getId(),Map.of("file",Map.of("log.retention.hours","168")));service.markConfigMutation(step.getId());service.completeStep(step.getId(),"Verified");
        verify(mapping,times(4)).step(step);
        assertThat(step.getPayload()).contains("\"key\":\"log.retention.hours\"","\"value\":\"72\"","recoveryCheckpoint","configMutationStarted");
        assertThat(step.getStatus()).isEqualTo(JobStepStatus.SUCCESS);
    }
    @Test void actualObservedValuesAndHealthSnapshotsAreForwardedWithoutAnotherKafkaRead(){
        var job=job(JobType.TARGETED_CONFIG_CHANGE);var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);when(steps.findById(step.getId())).thenReturn(Optional.of(step));
        service.recordConfigVerification(step.getId(),"log.retention.hours","72","DYNAMIC_BROKER_CONFIG");
        verify(mapping).verified(step,"log.retention.hours","72","DYNAMIC_BROKER_CONFIG");
        var snapshot=new RollingRestartHealthService.HealthSnapshot(true,2,1,0,0,0,0,Map.of());service.recordConfigHealth(step.getId(),"PRECHECK",snapshot);
        verify(mapping).health(eq(job),eq(step),eq("PRECHECK"),argThat(metrics->metrics.get("visibleBrokers").equals(2)),eq(true),isNull());
    }
    @Test void standaloneRestartsContinueUsingTheirOwnMapping(){
        var job=job(JobType.ROLLING_RESTART);service.updateJobStatus(job.getId(),JobStatus.SUCCESS);verifyNoInteractions(mapping);
    }

    @ParameterizedTest
    @EnumSource(value=JobStatus.class,names={"IN_PROGRESS","ROLLING_BACK"})
    void executionHealthUsesThePersistedParentStatus(JobStatus status) {
        var job=job(JobType.TARGETED_CONFIG_CHANGE);job.setStatus(status);
        var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);
        when(steps.findById(step.getId())).thenReturn(Optional.of(step));
        var snapshot=new RollingRestartHealthService.HealthSnapshot(true,3,101,0,0,0,0,Map.of());

        service.recordConfigExecutionHealth(step.getId(),snapshot);

        verify(mapping).health(eq(job),eq(step),eq(status==JobStatus.ROLLING_BACK?"ROLLBACK":"EXECUTION"),
                argThat(metrics->metrics.get("visibleBrokers").equals(3)),eq(true),isNull());
    }
}
