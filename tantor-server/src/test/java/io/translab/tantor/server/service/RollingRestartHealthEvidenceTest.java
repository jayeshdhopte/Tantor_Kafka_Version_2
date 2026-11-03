package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.HostRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.time.OffsetDateTime;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RollingRestartHealthEvidenceTest {
    final KafkaAdminService kafka=mock(KafkaAdminService.class);
    final HostRepository hosts=mock(HostRepository.class);
    final RollingRestartHealthService health=new RollingRestartHealthService(kafka,hosts);
    RollingRestartHealthEvidenceTest() {
        ReflectionTestUtils.setField(health,"maxDiskUsedPercent",85.0);
        ReflectionTestUtils.setField(health,"maxConsumerLag",1000000L);
        ReflectionTestUtils.setField(health,"heartbeatTimeoutSeconds",90L);
    }
    void onlineHost(boolean disk) {
        Host host=new Host();host.setStatus("ONLINE");host.setLastHeartbeat(OffsetDateTime.now());
        if(disk){host.setDiskTotalGb(100L);host.setDiskUsedGb(40L);}
        when(hosts.findById("one")).thenReturn(Optional.of(host));
    }
    void healthyKafka(UUID cluster) {
        AdminClient client=mock(AdminClient.class,RETURNS_DEEP_STUBS);when(kafka.getAdminClient(cluster)).thenReturn(client);
        when(client.describeCluster().nodes()).thenReturn(KafkaFuture.completedFuture(List.of(new Node(1,"one",9092),new Node(2,"two",9092))));
        when(client.describeCluster().controller()).thenReturn(KafkaFuture.completedFuture(new Node(101,"controller",9093)));
        when(client.listTopics(any(ListTopicsOptions.class)).names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(client.listConsumerGroups().all()).thenReturn(KafkaFuture.completedFuture(List.of()));
    }
    @Test void savesRealIndividualOutcomesWithoutTreatingMissingDiskMetricsAsPass() {
        UUID cluster=UUID.randomUUID();healthyKafka(cluster);onlineHost(false);
        var snapshot=health.inspect(cluster,2,Set.of("one"));
        assertThat(snapshot.healthy()).isTrue();
        assertThat(snapshot.checks().get("disk").outcome()).isEqualTo("UNKNOWN");
        for(String key:List.of("brokerReachability","controller","offlinePartitions","underReplicatedPartitions","minimumIsr","consumerLag","kafkaInspection","hostReachability"))
            assertThat(snapshot.checks().get(key).outcome()).as(key).isEqualTo("PASS");
    }
    @Test void kafkaInspectionErrorsLeaveUnobservedChecksUnknown() {
        UUID cluster=UUID.randomUUID();onlineHost(true);when(kafka.getAdminClient(cluster)).thenThrow(new IllegalStateException("unreachable"));
        var snapshot=health.inspect(cluster,2,Set.of("one"));
        assertThat(snapshot.healthy()).isFalse();assertThat(snapshot.checks().get("kafkaInspection").outcome()).isEqualTo("ERROR");
        assertThat(snapshot.checks().get("offlinePartitions").outcome()).isEqualTo("UNKNOWN");
        assertThat(snapshot.checks().get("disk").outcome()).isEqualTo("PASS");
    }
    @Test void unhealthyBrokerCountAndHostHeartbeatAreSeparateFailures() {
        UUID cluster=UUID.randomUUID();healthyKafka(cluster);when(hosts.findById("one")).thenReturn(Optional.empty());
        var snapshot=health.inspect(cluster,3,Set.of("one"));
        assertThat(snapshot.healthy()).isFalse();
        assertThat(snapshot.checks().get("brokerReachability").outcome()).isEqualTo("FAIL");
        assertThat(snapshot.checks().get("hostReachability").outcome()).isEqualTo("FAIL");
        assertThat(snapshot.checks().get("disk").outcome()).isEqualTo("UNKNOWN");
    }
    @Test void externalDiscoveryAgentFailuresAreMappedToTheSameCheckKeys() {
        UUID cluster=UUID.randomUUID();healthyKafka(cluster);
        var external=mock(ExternalKafkaManagement.class);ReflectionTestUtils.setField(health,"externalKafka",external);
        when(external.target("external:one")).thenReturn(true);
        when(external.diskMetrics("external:one")).thenReturn(Map.of("totalGb",100L,"usedGb",95L));
        doAnswer(call->{Map<String,String> failures=call.getArgument(1);failures.put("disk:external:one","Disk pressure");return null;})
                .when(external).inspectHosts(anySet(),anyMap(),anyDouble(),anyLong());
        var snapshot=health.inspect(cluster,2,Set.of("external:one"));
        assertThat(snapshot.healthy()).isFalse();
        assertThat(snapshot.checks().get("disk").outcome()).isEqualTo("FAIL");
        assertThat(snapshot.checks().get("hostReachability").outcome()).isEqualTo("PASS");
    }

}
