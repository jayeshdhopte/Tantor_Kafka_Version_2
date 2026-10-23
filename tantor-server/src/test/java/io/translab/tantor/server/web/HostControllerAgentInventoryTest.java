package io.translab.tantor.server.web;

import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.service.HostStatusService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.time.OffsetDateTime;
import java.util.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class HostControllerAgentInventoryTest {
    @Test void hostInventoryExcludesExternalEnrollmentAndKeepsTheActualHostAgentDetails() {
        var controller = mock(HostController.class, CALLS_REAL_METHODS);
        var hosts = mock(HostRepository.class);
        var nodes = mock(ExternalClusterNodeRepository.class);
        var clusters = mock(ExternalClusterRepository.class);
        var discovery = mock(DiscoveryAgentRepository.class);
        ReflectionTestUtils.setField(controller, "hostRepository", hosts);
        ReflectionTestUtils.setField(controller, "hostStatusService", new HostStatusService());
        ReflectionTestUtils.setField(controller, "externalClusterNodeRepository", nodes);
        ReflectionTestUtils.setField(controller, "externalClusterRepository", clusters);
        ReflectionTestUtils.setField(controller, "discoveryAgentRepository", discovery);
        var host = new Host();
        host.setId("node191"); host.setHostname("node191"); host.setAgentName("Internal 191");
        host.setAgentVersion("1.0.0-prod.16"); host.setAgentPath("/opt/tantor-agent/bin/tantor-agent");
        host.setStatus("ONLINE"); host.setLastHeartbeat(OffsetDateTime.now());
        var external = new Host(); external.setId("external-node229"); external.setStatus("PENDING");
        var node = new ExternalClusterNode(); node.setHost("node191"); node.setClusterId(UUID.randomUUID());
        when(hosts.findAll()).thenReturn(List.of(external, host));
        when(nodes.findAll()).thenReturn(List.of(node));
        when(clusters.findById(node.getClusterId())).thenReturn(Optional.empty());

        var inventory = controller.getAllHosts().getBody();
        assertThat(inventory).hasSize(1);
        assertThat(inventory.getFirst()).containsEntry("id", "node191")
                .containsEntry("agentName", "Internal 191").containsEntry("agentType", "HOST")
                .containsEntry("agentVersion", "1.0.0-prod.16")
                .containsEntry("agentPath", "/opt/tantor-agent/bin/tantor-agent")
                .containsEntry("status", "OCCUPIED_EXTERNAL").containsEntry("available", false);
        host.setStatus("OCCUPIED");
        host.setLastHeartbeat(OffsetDateTime.now().minusMinutes(5));
        var offline = controller.getAllHosts().getBody().getFirst();
        assertThat(offline).containsEntry("status", "OFFLINE").containsEntry("agentStatus", "OFFLINE")
                .containsEntry("available", false).containsEntry("clusterId", node.getClusterId().toString());
        // Even an occupied internal cluster cannot turn a stale internal agent into an available host.
        when(nodes.findAll()).thenReturn(List.of());
        assertThat(controller.getAllHosts().getBody().getFirst()).containsEntry("status", "OFFLINE");
        verifyNoInteractions(discovery);
    }

    @Test void externalAgentCannotBeApprovedAsAnInternalHostEvenWithADirectRequest() {
        var controller = mock(HostController.class, CALLS_REAL_METHODS);
        var hosts = mock(HostRepository.class);
        var roles = mock(RoleAuthenticationUtil.class);
        var jobs = mock(io.translab.tantor.server.service.JobService.class);
        ReflectionTestUtils.setField(controller, "hostRepository", hosts);
        ReflectionTestUtils.setField(controller, "hostStatusService", new HostStatusService());
        ReflectionTestUtils.setField(controller, "roleAuthenticationUtil", roles);
        ReflectionTestUtils.setField(controller, "jobService", jobs);
        var host = new Host(); host.setId("external-node229"); host.setStatus("PENDING");
        when(hosts.findById(host.getId())).thenReturn(Optional.of(host));
        when(roles.canAccess("token", RoleAuthenticationUtil.HOST_ONBOARDING)).thenReturn(true);
        assertThat(controller.approveHost("token", host.getId()).getStatusCode().value()).isEqualTo(409);
        verifyNoInteractions(jobs);
    }
}
