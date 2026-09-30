package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStatus;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.domain.JobStepStatus;
import io.translab.tantor.server.domain.JobType;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import io.translab.tantor.server.repository.HostRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeploymentJobHandlerTest {

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void connectFailurePreservesKafkaAndMarksTheServiceFailed(boolean servicesOnly) throws Exception {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setStatus("SUCCESS");
        var assignment = new io.translab.tantor.server.domain.ClusterServiceAssignment();
        assignment.setRole("kafka_connect");
        var assignments = mock(ClusterServiceAssignmentRepository.class);
        when(assignments.findByClusterIdAndRole(clusterId, "kafka_connect")).thenReturn(Optional.of(assignment));
        var clusters = mock(ClusterRepository.class);
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        var mapper = new ObjectMapper();
        Job job = new Job();
        job.setId(UUID.randomUUID());
        job.setRequestedBy("operator");
        job.setPayload(mapper.writeValueAsString(Map.of("clusterId", clusterId, "servicesOnly", servicesOnly, "kafkaVersion", "3.9.0",
                "kafkaConnect", Map.of("host_id", "host-1", "host_name", "connect.local", "rest_port", 18083,
                        "working_dir", "/data/connect", "plugin_dir", "/opt/plugins"))));
        JobStep step = new JobStep();
        step.setId(UUID.randomUUID());
        step.setStatus(JobStepStatus.PENDING);
        step.setPayload("{\"operation\":\"install_connect\",\"host_id\":\"host-1\"}");
        var jobs = mock(JobService.class);
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        var deployment = mock(DeploymentService.class);
        UUID taskId = UUID.randomUUID();
        when(deployment.dispatchSchemaTask(eq(clusterId), eq("host-1"), eq("INSTALL_CONNECT"), anyString(), anyString(), anyMap()))
                .thenReturn(taskId);
        var awaiter = mock(AgentTaskAwaiter.class);
        when(awaiter.await(taskId)).thenThrow(new IllegalStateException("Connect start failed"));
        var handler = new DeploymentJobHandler(jobs, deployment, awaiter, clusters, mock(HostRepository.class),
                mapper, mock(PrometheusMonitoringService.class), mock(KafkaAdminService.class),
                mock(DataServiceConnectionService.class), assignments);

        assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("Connect start failed");
        assertThat(cluster.getStatus()).isEqualTo(servicesOnly ? "SUCCESS" : "DEGRADED");
        assertThat(assignment.getStatus()).isEqualTo("FAILED");
        assertThat(assignment.getLastError()).contains("Connect start failed");
        verify(deployment).dispatchSchemaTask(eq(clusterId), eq("host-1"), eq("INSTALL_CONNECT"), anyString(), anyString(),
                argThat(parameters -> Integer.valueOf(18083).equals(parameters.get("rest_port"))
                        && "/data/connect".equals(parameters.get("working_dir"))
                        && "/opt/plugins".equals(parameters.get("plugin_dir"))));
    }

    @Test
    void preservesRequestingUsernameOnAsynchronousClusterStatusUpdates() {
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setUpdatedBy("system");

        Job job = new Job();
        job.setId(UUID.randomUUID());
        job.setType(JobType.DEPLOYMENT);
        job.setStatus(JobStatus.PENDING);
        job.setRequestedBy("admin");
        job.setPayload("{\"clusterId\":\"" + clusterId + "\"}");

        JobStep completedStep = new JobStep();
        completedStep.setStatus(JobStepStatus.SUCCESS);

        JobService jobService = mock(JobService.class);
        ClusterRepository clusterRepository = mock(ClusterRepository.class);
        when(clusterRepository.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(jobService.getSteps(job.getId())).thenReturn(List.of(completedStep));

        DeploymentJobHandler handler = new DeploymentJobHandler(
                jobService,
                mock(DeploymentService.class),
                mock(AgentTaskAwaiter.class),
                clusterRepository,
                mock(HostRepository.class),
                new ObjectMapper(),
                mock(PrometheusMonitoringService.class),
                mock(KafkaAdminService.class),
                mock(DataServiceConnectionService.class),
                mock(ClusterServiceAssignmentRepository.class)
        );

        handler.execute(job);

        ArgumentCaptor<Cluster> savedClusters = ArgumentCaptor.forClass(Cluster.class);
        verify(clusterRepository, times(2)).save(savedClusters.capture());
        assertThat(savedClusters.getAllValues())
                .allSatisfy(saved -> assertThat(saved.getUpdatedBy()).isEqualTo("admin"));
        assertThat(cluster.getStatus()).isEqualTo("SUCCESS");
    }
}
