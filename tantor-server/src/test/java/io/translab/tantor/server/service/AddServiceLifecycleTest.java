package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AddServiceLifecycleTest {
    @ParameterizedTest
    @CsvSource({"schema_registry,DEPLOYING", "schema_registry,ONLINE", "schema_registry,FAILED", "schema_registry,ROLLED_BACK",
            "kafka_connect,DEPLOYING", "kafka_connect,ONLINE", "kafka_connect,FAILED", "kafka_connect,ROLLED_BACK"})
    void managedStatusPreservesExistingAssignmentAndUpdatesTheSameMapping(String role, String status) {
        var services = mock(ClusterServiceAssignmentRepository.class);
        var mappings = mock(AddServiceMappingService.class);
        var handler = new DeploymentJobHandler(mock(JobService.class), mock(DeploymentService.class), mock(AgentTaskAwaiter.class),
                mock(ClusterRepository.class), mock(HostRepository.class), new ObjectMapper(), mock(PrometheusMonitoringService.class),
                mock(KafkaAdminService.class), mock(DataServiceConnectionService.class), services, mappings);
        UUID clusterId = UUID.randomUUID();
        var service = new ClusterServiceAssignment(); service.setId(UUID.randomUUID()); service.setRole(role);
        when(services.findByClusterIdAndRoleAndHostId(clusterId, role, "host-a")).thenReturn(Optional.of(service));
        String method = role.equals("schema_registry") ? "markSchemaService" : "markConnectService";
        ReflectionTestUtils.invokeMethod(handler, method, clusterId, Map.of("host_id", "host-a"), status, "saved error", "operator");
        assertThat(service.getStatus()).isEqualTo(status);
        assertThat(service.getLastError()).isEqualTo("saved error");
        verify(services).save(service);
        verify(mappings).updateStatus(service.getId(), status, "operator");
        verifyNoMoreInteractions(mappings);
    }

    @Test
    void externalFailureStillFailsTheOriginalStepAndUpdatesTheMapping() {
        var jobs = mock(JobService.class); var manager = mock(ExternalDataServiceManager.class);
        var services = mock(ExternalDataServiceRepository.class); var mappings = mock(AddServiceMappingService.class);
        var handler = new ExternalServicesJobHandler(jobs, manager, services, mock(ExternalServiceTaskRepository.class),
                mock(KafkaAdminService.class), mock(DataServiceConnectionService.class), mappings);
        var job = new Job(); job.setId(UUID.randomUUID()); job.setRequestedBy("operator");
        var step = new JobStep(); step.setId(UUID.randomUUID()); step.setPayload("step");
        var service = new ExternalDataService(); service.setId(UUID.randomUUID()); service.setClusterId(UUID.randomUUID());
        when(jobs.getSteps(job.getId())).thenReturn(List.of(step));
        when(manager.read("step")).thenReturn(Map.of("serviceId", service.getId().toString()));
        when(services.findById(service.getId())).thenReturn(Optional.of(service));
        when(manager.requireCluster(service.getClusterId())).thenThrow(new IllegalArgumentException("agent unavailable"));
        assertThatThrownBy(() -> handler.execute(job)).isInstanceOf(IllegalStateException.class).hasMessageContaining("agent unavailable");
        verify(jobs).failStep(step.getId(), "agent unavailable");
        assertThat(service.getStatus()).isEqualTo("FAILED");
        verify(mappings).updateStatus(service.getId(), "FAILED", "operator");
    }
}
