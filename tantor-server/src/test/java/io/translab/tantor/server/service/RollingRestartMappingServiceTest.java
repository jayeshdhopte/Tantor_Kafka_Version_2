package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RollingRestartMappingServiceTest {
    final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    final ObjectMapper mapper = new ObjectMapper();
    final ExternalClusterRepository external = mock(ExternalClusterRepository.class);
    final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
    final AuditService audit = mock(AuditService.class);
    final RollingRestartMappingService mapping = new RollingRestartMappingService(jdbc, mapper, external, events, audit);

    Job job() {
        var job = new Job(); job.setId(UUID.randomUUID()); job.setType(JobType.ROLLING_RESTART);
        job.setRequestedBy("alice"); return job;
    }
    Cluster cluster() {
        var cluster = new Cluster(); cluster.setId(UUID.randomUUID()); cluster.setName("test-cluster");
        cluster.setMode("kraft"); cluster.setEnvironment("DEV"); cluster.setBootstrapServers("a:9092,b:9092"); return cluster;
    }
    JobStep step(String host, String role, int node) throws Exception {
        var step = new JobStep(); step.setId(UUID.randomUUID()); step.setTargetId(host);
        step.setPayload(mapper.writeValueAsString(Map.of("hostId",host,"role",role,"nodeId",node,
                "serviceName","kafka.service","password","not-for-summary"))); return step;
    }

    @Test void oneSummaryPreservesTheHostRoleAssociationForAllSelectedProcesses() throws Exception {
        var job = job(); var cluster = cluster();
        mapping.record(job,cluster,List.of(step("a","broker",1),step("a","controller",2),step("b","broker",3)));
        var invocation = mockingDetails(jdbc).getInvocations().iterator().next();
        String sql = (String) invocation.getRawArguments()[0];
        Object[] args = (Object[]) invocation.getRawArguments()[1];
        assertThat(args[0]).isEqualTo(job.getId());
        assertThat(args[7]).isEqualTo("a:9092,b:9092");
        assertThat(args[8]).isEqualTo("alice");
        assertThat(mapper.readTree((String)args[13])).isEqualTo(mapper.readTree("[\"a\",\"b\"]"));
        var roles = mapper.readTree((String)args[12]);
        assertThat(roles.size()).isEqualTo(3);
        assertThat(roles.get(1).path("hostId").asText()).isEqualTo("a");
        assertThat(roles.get(1).path("role").asText()).isEqualTo("controller");
        assertThat((String)args[12]).doesNotContain("password","not-for-summary");
        assertThat(sql).contains("ON CONFLICT (unique_id) DO NOTHING").doesNotContain("log_path","old_config_path");
    }

    @Test void externalMetadataComesFromExternalInventoryWithoutCopyingSecuritySettings() throws Exception {
        var cluster = cluster(); var discovered = new ExternalCluster(); discovered.setId(cluster.getId());
        discovered.setName("discovered"); discovered.setKafkaMode("zookeeper"); discovered.setEnvironment("QA");
        discovered.setBootstrapServers("external:9092"); discovered.setSaslPasswordEncrypted("secret");
        when(external.findById(cluster.getId())).thenReturn(Optional.of(discovered));
        mapping.record(job(),cluster,List.of(step("external:node","broker",1)));
        Object[] args = (Object[]) mockingDetails(jdbc).getInvocations().iterator().next().getRawArguments()[1];
        assertThat(args).contains("discovered","Existing","zookeeper","QA","external:9092").doesNotContain("secret");
    }

    @Test void duplicateCreationDoesNotOverwriteOriginalDataOrProduceAnotherAuditEvent() throws Exception {
        when(jdbc.update(anyString(),any(Object[].class))).thenReturn(1,0);
        var job = job(); var cluster = cluster(); var steps = List.of(step("a","broker",1));
        mapping.record(job,cluster,steps); mapping.record(job,cluster,steps);
        var event = ArgumentCaptor.forClass(RollingRestartMappingService.AuditEvent.class);
        verify(events).publishEvent(event.capture());
        assertThat(event.getValue().jobId()).isEqualTo(job.getId());
        assertThat(event.getValue().action()).isEqualTo("ROLLING_RESTART_REQUESTED");
    }

    @Test void statusUpdatesUseSavedActorAndDoNotOverwriteCreationOrTargetSnapshots() {
        var job = job(); job.setStatus(JobStatus.SUCCESS);
        when(jdbc.queryForList(anyString(),any(Object[].class))).thenReturn(List.of(Map.of("cluster_id",UUID.randomUUID(),
                "cluster_name","snapshot","updated_by","retry-user","host_id","[\"a\"]","role_per_node","[]")));
        mapping.synchronizeStatus(job,null,"ROLLING_RESTART_COMPLETED");
        String sql = (String) mockingDetails(jdbc).getInvocations().iterator().next().getRawArguments()[0];
        assertThat(sql).contains("status IS DISTINCT FROM", "COALESCE(?, updated_by)").doesNotContain("created_at =","role_per_node =","INSERT");
        var event = ArgumentCaptor.forClass(RollingRestartMappingService.AuditEvent.class);
        verify(events).publishEvent(event.capture());
        assertThat(event.getValue().actor()).isEqualTo("retry-user");
        assertThat(event.getValue().status()).isEqualTo("SUCCESS");
    }

    @Test void oldUnmappedJobsDoNotGetBackfilledOrAuditedOnStatusChange() {
        mapping.synchronizeStatus(job(),"bob","ROLLING_RESTART_RETRY_REQUESTED");
        verifyNoInteractions(events);
    }

    @Test void otherJobTypesNeverWriteOrAuditRestartSummaries() {
        var job = job(); job.setType(JobType.TARGETED_CONFIG_CHANGE);
        mapping.record(job,cluster(),List.of()); mapping.synchronizeStatus(job,"alice","ignored");
        verifyNoInteractions(jdbc,external,events,audit);
    }
}
