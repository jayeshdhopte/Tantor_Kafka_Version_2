package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.dto.HostRegistrationDto;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class AgentRegistrationIdentityTest {
    @Test void distinguishesLegacyDefaultAndPreservesConfiguredAgentNamesAndHostIds() {
        var hosts = mock(HostRepository.class);
        var service = new AgentService(hosts, mock(TaskRepository.class), mock(ClusterRepository.class),
                new ObjectMapper(), mock(ParcelService.class), mock(ActivityAlertService.class),
                mock(AuditService.class), mock(JobService.class));
        var dto = new HostRegistrationDto();
        dto.setHostId("stable-agent-id"); dto.setHostname("broker3.translab.io");
        dto.setIpAddresses(List.of("192.168.3.191")); dto.setAgentName(dto.getHostname());
        assertThat(service.registerHost(dto, "192.168.3.191")).isTrue();
        var saved = org.mockito.ArgumentCaptor.forClass(Host.class);
        verify(hosts).save(saved.capture());
        assertThat(saved.getValue().getAgentName()).isEqualTo("tantor-agent");
        assertThat(saved.getValue().getHostname()).isEqualTo("broker3.translab.io");
        assertThat(saved.getValue().getId()).isEqualTo("stable-agent-id");
        assertThat(saved.getValue().getStatus()).isEqualTo("PENDING");
        clearInvocations(hosts);
        dto.setAgentName("Production Internal Agent 191");
        service.registerHost(dto, "192.168.3.191");
        verify(hosts).save(saved.capture());
        assertThat(saved.getValue().getAgentName()).isEqualTo("Production Internal Agent 191");
    }
}
