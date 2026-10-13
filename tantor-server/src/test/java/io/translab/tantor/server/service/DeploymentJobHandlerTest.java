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

    @Test
    void savingAnotherWorkerPreservesTheExistingDefaultConnection() {
        var connections = mock(DataServiceConnectionService.class);
        UUID clusterId = UUID.randomUUID();
        when(connections.getActiveConnection(clusterId, "KAFKA_CONNECT"))
                .thenReturn(Optional.of(new io.translab.tantor.server.domain.DataServiceConnection()));
        var handler = new DeploymentJobHandler(mock(JobService.class), mock(DeploymentService.class), mock(AgentTaskAwaiter.class),
                mock(ClusterRepository.class), mock(HostRepository.class), new ObjectMapper(), mock(PrometheusMonitoringService.class),
                mock(KafkaAdminService.class), connections, mock(ClusterServiceAssignmentRepository.class));
        var config = Map.<String, Object>of("host_id", "host-2", "host_name", "192.168.3.213", "rest_port", 18083, "instance_connection", true);
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(handler, "executeBackendConnectOperation", clusterId,
                Map.of("requestedBy", "operator"), Map.of("operation", "save_connect_connection", "host_id", "host-2", "serviceConfig", config));
        verify(connections).saveConnection(eq(clusterId), eq("KAFKA_CONNECT"),
                argThat(connection -> "Deployed Kafka Connect on host-2".equals(connection.getConnectionName())
                        && Boolean.FALSE.equals(connection.getIsDefault()) && connection.getPort() == 18083), eq("operator"));
    }

    @Test
    void rollbackRemovesOnlyTheTargetServiceAndKeepsKafka() throws Exception {
        UUID clusterId = UUID.randomUUID(), taskId = UUID.randomUUID();
        var jobs = mock(JobService.class); var deployment = mock(DeploymentService.class); var awaiter = mock(AgentTaskAwaiter.class);
        var mapper = new ObjectMapper(); var job = new Job(); job.setId(UUID.randomUUID());
        job.setPayload(mapper.writeValueAsString(Map.of("clusterId", clusterId, "kafkaVersion", "4.3.0")));
        var step = new JobStep(); step.setId(UUID.randomUUID()); step.setStatus(JobStepStatus.SUCCESS);
        step.setPayload(mapper.writeValueAsString(Map.of("operation", "install_connect", "host_id", "host-2", "serviceConfig",
                Map.of("host_id", "host-2", "install_dir", "/opt/kafka", "config_dir", "/opt/kafka/config", "role", "kafka_connect"))));
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        when(deployment.deleteClusterFromHost(eq(clusterId), eq("host-2"), eq("4.3.0"), eq("{}"), anyList(), eq(false))).thenReturn(taskId);
        when(awaiter.await(taskId)).thenReturn(new io.translab.tantor.server.domain.Task());
        var handler = new DeploymentJobHandler(jobs, deployment, awaiter, mock(ClusterRepository.class), mock(HostRepository.class),
                mapper, mock(PrometheusMonitoringService.class), mock(KafkaAdminService.class), mock(DataServiceConnectionService.class), mock(ClusterServiceAssignmentRepository.class));
        handler.rollback(job);
        verify(deployment).deleteClusterFromHost(eq(clusterId), eq("host-2"), eq("4.3.0"), eq("{}"),
                argThat(services -> services.size() == 1 && "kafka_connect".equals(services.get(0).get("kind"))
                        && "/opt/kafka".equals(services.get(0).get("install_dir"))), eq(false));
        verify(jobs).rolledBackStep(eq(step.getId()), anyString());
    }

    @Test
    void multipleWorkersUseTheirOwnParametersAndFailureDoesNotOverwriteOnlinePeer() throws Exception {
        UUID clusterId = UUID.randomUUID();
        var cluster = new Cluster(); cluster.setId(clusterId); cluster.setStatus("SUCCESS");
        var clusters = mock(ClusterRepository.class); when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        var assignments = mock(ClusterServiceAssignmentRepository.class);
        var primary = new io.translab.tantor.server.domain.ClusterServiceAssignment();
        var secondary = new io.translab.tantor.server.domain.ClusterServiceAssignment();
        when(assignments.findByClusterIdAndRoleAndHostId(clusterId, "kafka_connect", "host-1")).thenReturn(Optional.of(primary));
        when(assignments.findByClusterIdAndRoleAndHostId(clusterId, "kafka_connect", "host-2")).thenReturn(Optional.of(secondary));
        var mapper = new ObjectMapper();
        var job = new Job(); job.setId(UUID.randomUUID());
        job.setPayload(mapper.writeValueAsString(Map.of("clusterId", clusterId, "servicesOnly", true, "kafkaVersion", "4.3.0")));
        var firstConfig = Map.<String, Object>of("host_id", "host-1", "host_name", "192.168.3.191", "rest_port", 8083, "instance_connection", true);
        var secondConfig = Map.<String, Object>of("host_id", "host-2", "host_name", "192.168.3.213", "rest_port", 18083, "instance_connection", true);
        List<JobStep> steps = new java.util.ArrayList<>();
        for (var operation : List.of("install_connect", "save_connect_connection", "install_connect")) {
            var step = new JobStep(); step.setId(UUID.randomUUID()); step.setStatus(JobStepStatus.PENDING);
            var config = steps.size() == 2 ? secondConfig : firstConfig;
            step.setPayload(mapper.writeValueAsString(Map.of("operation", operation, "host_id", config.get("host_id"), "serviceConfig", config)));
            steps.add(step);
        }
        var jobs = mock(JobService.class); when(jobs.getSteps(job.getId())).thenReturn(steps);
        var deployment = mock(DeploymentService.class); var awaiter = mock(AgentTaskAwaiter.class);
        UUID firstTask = UUID.randomUUID(), secondTask = UUID.randomUUID();
        when(deployment.dispatchSchemaTask(eq(clusterId), eq("host-1"), eq("INSTALL_CONNECT"), anyString(), anyString(), anyMap())).thenReturn(firstTask);
        when(deployment.dispatchSchemaTask(eq(clusterId), eq("host-2"), eq("INSTALL_CONNECT"), anyString(), anyString(), anyMap())).thenReturn(secondTask);
        when(awaiter.await(firstTask)).thenReturn(new io.translab.tantor.server.domain.Task());
        when(awaiter.await(secondTask)).thenThrow(new IllegalStateException("Second worker failed"));
        var connections = mock(DataServiceConnectionService.class);
        var handler = new DeploymentJobHandler(jobs, deployment, awaiter, clusters, mock(HostRepository.class), mapper,
                mock(PrometheusMonitoringService.class), mock(KafkaAdminService.class), connections, assignments);
        assertThatThrownBy(() -> handler.execute(job)).hasMessageContaining("Second worker failed");
        assertThat(primary.getStatus()).isEqualTo("ONLINE");
        assertThat(secondary.getStatus()).isEqualTo("FAILED");
        assertThat(cluster.getStatus()).isEqualTo("SUCCESS");
        verify(deployment).dispatchSchemaTask(eq(clusterId), eq("host-2"), eq("INSTALL_CONNECT"), anyString(), anyString(),
                argThat(config -> Integer.valueOf(18083).equals(config.get("rest_port")) && "192.168.3.213".equals(config.get("host_name"))));
        verify(connections).saveConnection(eq(clusterId), eq("KAFKA_CONNECT"),
                argThat(connection -> "Deployed Kafka Connect on host-1".equals(connection.getConnectionName()) && Boolean.TRUE.equals(connection.getIsDefault())), any());
    }

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
        when(assignments.findByClusterIdAndRoleAndHostId(clusterId, "kafka_connect", "host-1")).thenReturn(Optional.of(assignment));
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
