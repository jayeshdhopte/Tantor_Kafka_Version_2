package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class JavaUpgradeJobHandlerTest {
    private final JobService jobs = mock(JobService.class);
    private final ClusterRepository clusters = mock(ClusterRepository.class);
    private final HostRepository hosts = mock(HostRepository.class);
    private final ClusterServiceAssignmentRepository assignments = mock(ClusterServiceAssignmentRepository.class);
    private final TaskRepository tasks = mock(TaskRepository.class);
    private final HostParcelRepository parcels = mock(HostParcelRepository.class);
    private final AddNodeArtifactCatalog catalog = mock(AddNodeArtifactCatalog.class);
    private final HostStatusService hostStatus = mock(HostStatusService.class);
    private final AgentTaskAwaiter awaiter = mock(AgentTaskAwaiter.class);
    private final ExternalJavaUpgradeService externalJava = mock(ExternalJavaUpgradeService.class);
    private final ObjectMapper json = new ObjectMapper();
    private final TransactionTemplate transactions = new TransactionTemplate() {
        @Override
        public <T> T execute(TransactionCallback<T> callback) {
            return callback.doInTransaction(new SimpleTransactionStatus());
        }
    };
    private final JavaUpgradeJobHandler handler = new JavaUpgradeJobHandler(
            jobs, clusters, hosts, assignments, tasks, parcels, catalog, hostStatus, json, transactions, awaiter, externalJava);

    @Test
    void submitsCatalogBoundTaskAndStopsAfterAgentFailure() {
        UUID clusterId = UUID.randomUUID();
        UUID artifactId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        String version = "21.0.13";
        String checksum = "b".repeat(64);

        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setMode("kraft");
        cluster.setStatus("SUCCESS");
        cluster.setKafkaVersion("4.1.0");
        cluster.setBootstrapServers("broker.test:9092");
        cluster.setConfigPath("/opt/kafka/config/server.properties");
        var broker = bindHost(cluster, "broker-1", artifactId, version, checksum);
        var second = bindHost(cluster, "broker-2", artifactId, version, checksum);
        cluster.setServices(List.of(broker, second));
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(catalog.resolveType("JAVA", version, artifactId.toString()))
                .thenReturn(new AddNodeArtifactCatalog.Artifact(artifactId.toString(), version, checksum));
        when(jobs.createJob(any(Job.class), anyList())).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            List<JobStep> steps = invocation.getArgument(1);
            job.setId(jobId);
            for (JobStep step : steps) step.setId(UUID.randomUUID());
            when(jobs.getSteps(jobId)).thenReturn(steps);
            return job;
        });
        when(tasks.saveAndFlush(any(Task.class))).thenAnswer(invocation -> {
            Task task = invocation.getArgument(0);
            task.setId(taskId);
            return task;
        });
        when(awaiter.await(taskId)).thenThrow(new IllegalStateException("Kafka verification failed"));

        assertThat(handler.schedule(clusterId, artifactId, List.of("broker-1", "broker-2"), version)).isEqualTo(jobId);
        Job job = new Job();
        job.setId(jobId);
        assertThatThrownBy(() -> handler.execute(job)).hasMessage("Kafka verification failed");

        var sent = org.mockito.ArgumentCaptor.forClass(Task.class);
        verify(tasks).saveAndFlush(sent.capture());
        assertThat(sent.getValue().getHostId()).isEqualTo("broker-1");
        assertThat(sent.getValue().getCommand()).isEqualTo("JAVA_UPGRADE");
        assertThat(sent.getValue().getChecksum()).isEqualTo(checksum);
        UUID failedStep = jobs.getSteps(jobId).getFirst().getId();
        verify(jobs).failStep(failedStep, "Kafka verification failed");
    }

    @Test
    void externalUpgradeUsesExternalTaskAndStopsOnFailure() {
        UUID clusterId=UUID.randomUUID(),artifact=UUID.randomUUID(),jobId=UUID.randomUUID(),taskId=UUID.randomUUID();
        String target="external:"+UUID.randomUUID();
        Cluster cluster=new Cluster();cluster.setId(clusterId);cluster.setKafkaVersion("4.1.0");
        when(externalJava.external(clusterId)).thenReturn(true);
        when(externalJava.cluster(clusterId)).thenReturn(cluster);
        when(externalJava.prepare(clusterId,target,artifact,"21.0.13")).thenReturn(java.util.Map.of("artifact_id",artifact.toString()));
        when(externalJava.enqueue(clusterId,target,artifact,"21.0.13")).thenReturn(taskId);
        when(jobs.createJob(any(),anyList())).thenAnswer(call->{Job j=call.getArgument(0);j.setId(jobId);List<JobStep> steps=call.getArgument(1);steps.forEach(s->s.setId(UUID.randomUUID()));when(jobs.getSteps(jobId)).thenReturn(steps);return j;});
        assertThat(handler.schedule(clusterId,artifact,List.of(target),"21.0.13")).isEqualTo(jobId);
        when(awaiter.await(taskId)).thenThrow(new IllegalStateException("External JVM verification failed"));
        Job job=new Job();job.setId(jobId);
        assertThatThrownBy(()->handler.execute(job)).hasMessage("External JVM verification failed");
        verify(externalJava).validateTargets(clusterId,List.of(target));
        verify(jobs).attachAgentTask(any(),eq(taskId));
        verifyNoInteractions(tasks);
    }

    private ClusterServiceAssignment bindHost(Cluster cluster, String hostId, UUID artifactId, String version, String checksum) {
        Host host = new Host();
        host.setId(hostId);
        host.setClusterId(cluster.getId());
        host.setAgentVersion("addnode.9.java-upgrade.1");
        when(hosts.findById(hostId)).thenReturn(Optional.of(host));
        when(hostStatus.agentConnectivityStatus(host)).thenReturn("ONLINE");

        var service = new ClusterServiceAssignment();
        service.setCluster(cluster);
        service.setHostId(hostId);
        service.setRole("broker");
        when(assignments.findLiveAssignmentsForHost(hostId)).thenReturn(List.of(service));

        var parcel = new HostParcel();
        parcel.setStatus("DISTRIBUTED");
        parcel.setServiceType("JAVA");
        parcel.setVersion(version);
        parcel.setChecksum(checksum);
        parcel.setParcelDir("/srv/apps/tantor/parcels");
        when(parcels.findFirstByHostIdAndArtifactIdOrderByCreatedAtDescIdDesc(hostId, artifactId)).thenReturn(Optional.of(parcel));
        return service;
    }
}
