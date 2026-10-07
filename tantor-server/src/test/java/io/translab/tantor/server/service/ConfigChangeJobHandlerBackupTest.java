package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfigChangeJobHandlerBackupTest {
    @Test
    void legacyDynamicWritesAndRollbacksCannotBypassTheBackup() {
        var handler = mock(ConfigChangeJobHandler.class, CALLS_REAL_METHODS);
        var clusters = mock(ClusterRepository.class);
        var kafka = mock(KafkaAdminService.class);
        var deployment = mock(DeploymentService.class);
        var awaiter = mock(AgentTaskAwaiter.class);
        var jobs = mock(JobService.class);
        ReflectionTestUtils.setField(handler,"clusterRepository",clusters);
        ReflectionTestUtils.setField(handler,"kafkaAdminService",kafka);
        ReflectionTestUtils.setField(handler,"deploymentService",deployment);
        ReflectionTestUtils.setField(handler,"taskAwaiter",awaiter);
        ReflectionTestUtils.setField(handler,"jobService",jobs);
        ReflectionTestUtils.setField(handler,"objectMapper",new ObjectMapper());
        UUID id = UUID.randomUUID(), taskId = UUID.randomUUID();
        var cluster = new Cluster(); cluster.setId(id); cluster.setMode("kraft"); cluster.setKafkaVersion("4.3.0");
        var broker = new ClusterServiceAssignment(); broker.setRole("broker"); broker.setNodeId(1); broker.setHostId("host-1");
        cluster.setServices(List.of(broker));
        when(clusters.findWithServicesById(id)).thenReturn(Optional.of(cluster));
        when(kafka.getBrokerConfigs(id)).thenReturn(Map.of(1,Map.of("log.retention.hours",Map.of("value","168","source","DYNAMIC_BROKER_CONFIG"))));
        when(deployment.backupKafkaConfig(eq(id),eq("host-1"),eq("broker"),eq("1"),anyString(),eq("/opt/kafka/config/broker.properties"),eq("log.retention.hours"),anyMap(),anyString())).thenReturn(taskId);
        var step = new JobStep(); step.setId(UUID.randomUUID());
        when(awaiter.await(taskId)).thenThrow(new IllegalStateException("Backup failed"));
        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(handler,"alterDynamicWithBackup",id,step,1,"log.retention.hours","72")).hasMessageContaining("Backup failed");
        verify(kafka,never()).alterBrokerConfig(any(),anyInt(),anyString(),anyString());
        clearInvocations(awaiter,kafka);
        var backup = new Task(); backup.setLogOutput("Checkpoint saved"); doReturn(backup).when(awaiter).await(taskId);
        ReflectionTestUtils.invokeMethod(handler,"alterDynamicWithBackup",id,step,1,"log.retention.hours","72");
        var order = inOrder(awaiter,kafka);
        order.verify(awaiter).await(taskId);
        order.verify(kafka).alterBrokerConfig(id,1,"log.retention.hours","72");
    }
}
