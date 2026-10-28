package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.ExternalDataService;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AddServiceMappingServiceTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final org.springframework.context.ApplicationEventPublisher events = mock(org.springframework.context.ApplicationEventPublisher.class);
    private final AddServiceMappingService mapping = new AddServiceMappingService(jdbc, new ObjectMapper(), events);

    @Test
    void managedMappingUsesAssignmentIdentityAndApprovedFieldMeaningsWithoutCopyingSecrets() {
        var cluster = new Cluster(); cluster.setId(UUID.randomUUID());
        var service = new ClusterServiceAssignment(); service.setId(UUID.randomUUID());
        service.setCluster(cluster); service.setHostId("host-a"); service.setRole("schema_registry");
        service.setStatus("PENDING");
        service.setConfigJson("""
                {"bootstrap_servers":"PLAINTEXT://broker-a:9092,PLAINTEXT://broker-b:9092",
                 "service_user":"root", "password_security":"do-not-copy"}
                """);
        mapping.recordManaged(service, "alice");
        var invocation = mockingDetails(jdbc).getInvocations().iterator().next();
        String sql = (String) invocation.getRawArguments()[0];
        Object[] args = (Object[]) invocation.getRawArguments()[1];
        assertThat(args[2]).isInstanceOf(OffsetDateTime.class);
        assertThat(args).containsExactly(service.getId(), cluster.getId(), args[2], "PENDING", "host-a",
                "PLAINTEXT://broker-a:9092,PLAINTEXT://broker-b:9092", "root", "alice", "alice", "schema_registry");
        assertThat(sql).contains("ON CONFLICT (add_service_id) DO NOTHING");
        assertThat(sql).doesNotContain("password_security", "security_protocol", "trust_store_location");
        assertThat(args).doesNotContain("do-not-copy");
    }

    @Test
    void externalMappingUsesAgentIdentityAndLeavesUnavailableValuesNull() {
        var service = external("kafka_connect");
        service.setConfigJson("{}");
        mapping.recordExternal(service, "bob");
        var invocation = mockingDetails(jdbc).getInvocations().iterator().next();
        Object[] args = (Object[]) invocation.getRawArguments()[1];
        assertThat(args[0]).isEqualTo(service.getId());
        assertThat(args[1]).isEqualTo(service.getClusterId());
        assertThat(args[4]).isEqualTo("discovery-agent-a");
        assertThat(args[5]).isNull();
        assertThat(args[6]).isNull();
        assertThat(args[9]).isEqualTo("kafka_connect");
    }

    @Test
    void brokerAssignmentsAreNotRecordedAsAddServices() {
        mapping.recordExternal(external("broker"), "alice");
        verifyNoInteractions(jdbc);
    }

    @Test
    void invalidPersistedConfigurationCannotCreateAnIncompleteMapping() {
        var service = external("schema_registry"); service.setConfigJson("invalid");
        assertThatThrownBy(() -> mapping.recordExternal(service, "alice"))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(jdbc);
    }

    @Test
    void assignmentMustHaveAnExistingId() {
        var service = external("schema_registry"); service.setId(null);
        assertThatThrownBy(() -> mapping.recordExternal(service, "alice"))
                .isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(jdbc);
    }

    @Test
    void retryAndCompletionUpdateOnlyStatusAndActorWithoutBackfillingOldAssignments() {
        UUID id = UUID.randomUUID();
        mapping.updateStatus(id, "DEPLOYING", "retry-user");
        mapping.updateStatus(id, "ONLINE", "retry-user");
        for (var invocation : mockingDetails(jdbc).getInvocations()) {
            String sql = (String) invocation.getRawArguments()[0];
            assertThat(sql).contains("UPDATE kf_add_service", "WHERE add_service_id = ?");
            assertThat(sql).doesNotContain("INSERT", "created_time =", "created_by =", "config_json");
        }
        assertThat(mockingDetails(jdbc).getInvocations()).hasSize(2);
    }

    @Test
    void duplicateCreationAndUnmappedLegacyUpdatesDoNotEmitAuditEvents() {
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1, 0, 0);
        var service = external("schema_registry");
        mapping.recordExternal(service, "creator");
        mapping.recordExternal(service, "retry-user");
        mapping.updateStatus(UUID.randomUUID(), "ONLINE", "legacy-job");
        verify(events).publishEvent(new AddServiceAuditListener.Event(service.getId(), "SERVICE_ADDED", "PENDING", "creator"));
        verifyNoMoreInteractions(events);
    }

    private static ExternalDataService external(String role) {
        var service = new ExternalDataService(); service.setId(UUID.randomUUID());
        service.setClusterId(UUID.randomUUID()); service.setAgentId("discovery-agent-a");
        service.setRole(role); service.setStatus("PENDING"); service.setConfigJson("{}");
        return service;
    }
}
