package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.service.HostStatusService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class HostControllerDataServicePrecheckTest {
    @Test
    void occupiedClusterHostWithLiveAgentCanRunConnectPrecheck() {
        var controller = mock(HostController.class, CALLS_REAL_METHODS);
        var hosts = mock(HostRepository.class);
        var tasks = mock(TaskRepository.class);
        var status = mock(HostStatusService.class);
        var roles = mock(RoleAuthenticationUtil.class);
        var host = mock(Host.class);
        ReflectionTestUtils.setField(controller, "hostRepository", hosts);
        ReflectionTestUtils.setField(controller, "taskRepository", tasks);
        ReflectionTestUtils.setField(controller, "hostStatusService", status);
        ReflectionTestUtils.setField(controller, "roleAuthenticationUtil", roles);
        ReflectionTestUtils.setField(controller, "objectMapper", new ObjectMapper());

        when(roles.canAccess("token", RoleAuthenticationUtil.HOST_PREREQUISITES)).thenReturn(true);
        when(hosts.findById("node174.translab.io")).thenReturn(Optional.of(host));
        when(host.getAgentVersion()).thenReturn("1.0.0-dataservices.1");
        when(status.effectiveStatus(host)).thenReturn("OCCUPIED");
        when(status.agentConnectivityStatus(host)).thenReturn("ONLINE");
        when(tasks.save(any(Task.class))).thenAnswer(invocation -> {
            Task task = invocation.getArgument(0);
            task.setId(UUID.randomUUID());
            return task;
        });

        var response = controller.precheckConnect("token", "node174.translab.io", Map.of("rest_port", 8083));

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        verify(tasks).save(argThat(task -> "PRECHECK_CONNECT".equals(task.getCommand())));
    }
}
