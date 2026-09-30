package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.HostParcel;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.HostParcelRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParcelServiceTest {

    @Test
    void distributesToOccupiedHostWhenAgentIsConnected() {
        Fixture fixture = new Fixture("OCCUPIED", "ONLINE");

        List<HostParcel> scheduled = fixture.service.distribute(fixture.artifactId, fixture.request());

        assertThat(scheduled).hasSize(1);
        assertThat(scheduled.get(0).getStatus()).isEqualTo("DISTRIBUTING");
        var taskCaptor = org.mockito.ArgumentCaptor.forClass(Task.class);
        verify(fixture.taskRepository).save(taskCaptor.capture());
        assertThat(taskCaptor.getValue().getArtifactUrl())
                .contains("https://artifacts.example/api/v1/artifacts/")
                .doesNotContain("artifact-repository:8081");
    }

    @Test
    void rejectsDistributionWhenAgentIsOffline() {
        Fixture fixture = new Fixture("OCCUPIED", "OFFLINE");

        assertThatThrownBy(() -> fixture.service.distribute(fixture.artifactId, fixture.request()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("agent is not online")
                .hasMessageContaining("OFFLINE");
    }

    private static final class Fixture {
        private final UUID artifactId = UUID.randomUUID();
        private final String hostId = "occupied-host";
        private final HostParcelRepository hostParcelRepository = mock(HostParcelRepository.class);
        private final HostRepository hostRepository = mock(HostRepository.class);
        private final TaskRepository taskRepository = mock(TaskRepository.class);
        private final HostStatusService hostStatusService = mock(HostStatusService.class);
        private final AuditService auditService = mock(AuditService.class);
        private final ParcelService service;

        private Fixture(String lifecycleStatus, String connectivityStatus) {
            Host host = new Host();
            host.setId(hostId);
            host.setHostname(hostId);
            host.setHostIp("192.168.3.191");
            host.setStatus(lifecycleStatus);

            when(hostRepository.findById(hostId)).thenReturn(Optional.of(host));
            when(hostStatusService.agentConnectivityStatus(host)).thenReturn(connectivityStatus);
            when(hostParcelRepository.findFirstByHostIdAndArtifactIdOrderByCreatedAtDescIdDesc(hostId, artifactId))
                    .thenReturn(Optional.empty());
            when(taskRepository.save(any(Task.class))).thenAnswer(invocation -> {
                Task task = invocation.getArgument(0);
                task.setId(UUID.randomUUID());
                return task;
            });
            when(hostParcelRepository.save(any(HostParcel.class))).thenAnswer(invocation -> {
                HostParcel parcel = invocation.getArgument(0);
                parcel.setId(UUID.randomUUID());
                return parcel;
            });
            when(auditService.currentActor()).thenReturn("admin");

            service = new ParcelService(hostParcelRepository, hostRepository, taskRepository,
                    new ObjectMapper(), hostStatusService, auditService, artifactRepositoryProperties());
        }

        private io.translab.tantor.server.config.ArtifactRepositoryProperties artifactRepositoryProperties() {
            var properties = new io.translab.tantor.server.config.ArtifactRepositoryProperties();
            properties.setInternalUrl(java.net.URI.create("http://artifact-repository:8081"));
            properties.setPublicUrl(java.net.URI.create("https://artifacts.example"));
            return properties;
        }

        private ParcelService.ParcelActionRequest request() {
            ParcelService.ParcelActionRequest request = new ParcelService.ParcelActionRequest();
            request.setHostIds(List.of(hostId));
            request.setChecksum("abc123");
            request.setServiceType("KAFKA");
            request.setVersion("3.9.2");
            request.setFileName("kafka.tgz");
            request.setParcelDir("/srv/apps/tantor/parcels");
            return request;
        }
    }
}
