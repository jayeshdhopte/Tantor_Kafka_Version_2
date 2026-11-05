package io.translab.tantor.server.service;

import io.translab.tantor.server.config.AddNodeReadinessProperties;
import io.translab.tantor.server.domain.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

class AddNodeKafkaReadinessTest {
    final KafkaAdminService kafka=mock(KafkaAdminService.class);
    final AdminClient admin=mock(AdminClient.class);
    final Cluster cluster=new Cluster();
    final AddNodeKafkaReadiness service=new AddNodeKafkaReadiness(kafka,new AddNodeReadinessProperties());
    final ListTopicsResult topics=mock(ListTopicsResult.class);
    final DescribeLogDirsResult logs=mock(DescribeLogDirsResult.class);
    @BeforeEach void setup() {
        cluster.setId(UUID.randomUUID()); cluster.setKafkaClusterId("live-id"); cluster.setKafkaVersion("4.1.2");
        var broker=new ClusterServiceAssignment();broker.setRole("broker");broker.setNodeId(1);
        var controller=new ClusterServiceAssignment();controller.setRole("controller");controller.setNodeId(101);
        cluster.setServices(List.of(broker,controller));
        when(kafka.getAdminClient(cluster.getId())).thenReturn(admin);
        var desc=mock(DescribeClusterResult.class);when(admin.describeCluster()).thenReturn(desc);
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(desc);
        when(desc.clusterId()).thenReturn(KafkaFuture.completedFuture("live-id"));
        when(desc.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(new Node(1,"192.0.2.1",9092))));
        var rackConfig=mock(DescribeConfigsResult.class);when(admin.describeConfigs(anyCollection())).thenReturn(rackConfig);
        when(rackConfig.all()).thenReturn(KafkaFuture.completedFuture(Map.of(new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.BROKER,"1"),new Config(List.of(new ConfigEntry("broker.rack",null))))));
        var featureResult=mock(DescribeFeaturesResult.class);var features=mock(FeatureMetadata.class);
        when(admin.describeFeatures()).thenReturn(featureResult);when(featureResult.featureMetadata()).thenReturn(KafkaFuture.completedFuture(features));
        when(features.finalizedFeatures()).thenReturn(Map.of("metadata.version",new FinalizedVersionRange((short)25,(short)25),"kraft.version",new FinalizedVersionRange((short)0,(short)0)));
        var quorumResult=mock(DescribeMetadataQuorumResult.class);var quorum=mock(QuorumInfo.class);var voter=mock(QuorumInfo.ReplicaState.class);
        when(admin.describeMetadataQuorum()).thenReturn(quorumResult);when(quorumResult.quorumInfo()).thenReturn(KafkaFuture.completedFuture(quorum));
        when(quorum.leaderId()).thenReturn(101);when(quorum.voters()).thenReturn(List.of(voter));when(voter.replicaId()).thenReturn(101);
        when(quorum.observers()).thenReturn(List.of());when(quorum.nodes()).thenReturn(Map.of());
        when(admin.listTopics(any(ListTopicsOptions.class))).thenReturn(topics);when(topics.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(admin.describeLogDirs(anyCollection())).thenReturn(logs);
        when(logs.allDescriptions()).thenReturn(KafkaFuture.completedFuture(Map.of(1,Map.of("/data",new LogDirDescription(null,Map.of(),100L<<30,50L<<30)))));
    }
    @ParameterizedTest @ValueSource(strings={"4.1.2","4.3.0"})
    void healthySnapshotUsesReadOnlyKafkaOperations(String version) {
        cluster.setKafkaVersion(version);
        var result=service.observe(cluster,Map.of("quorum_voters","101@192.0.2.1:9093"));
        assertThat(result.checks()).allMatch(c->c.status().equals("PASS"));
        assertThat(result.nodeIds()).containsExactlyInAnyOrder(1,101);
        assertThat(result.controllerEndpoints()).containsExactly("192.0.2.1:9093");
        assertThat(mockingDetails(admin).getInvocations()).allMatch(call->Set.of("describeCluster","describeFeatures","describeMetadataQuorum","listTopics","describeLogDirs","describeConfigs").contains(call.getMethod().getName()));
        assertThat(result.rackObserved()).isTrue();
    }
    @Test void fourThreeDynamicQuorumUsesLiveFeatureState() {
        cluster.setKafkaVersion("4.3.0");
        var features=mock(FeatureMetadata.class);var description=mock(DescribeFeaturesResult.class);
        when(admin.describeFeatures()).thenReturn(description);when(description.featureMetadata()).thenReturn(KafkaFuture.completedFuture(features));
        when(features.finalizedFeatures()).thenReturn(Map.of("metadata.version",new FinalizedVersionRange((short)30,(short)30),"kraft.version",new FinalizedVersionRange((short)1,(short)1)));
        var result=service.observe(cluster,Map.of("quorum_voters","101@192.0.2.1:9093"));
        assertThat(result.quorumMode()).isEqualTo("DYNAMIC");assertThat(result.checks()).allMatch(c->"PASS".equals(c.status()));
    }
    @Test void unvalidatedVersionStillFailsAdmission() {
        cluster.setKafkaVersion("4.4.0");
        assertThat(service.observe(cluster,Map.of("quorum_voters","101@192.0.2.1:9093")).checks()).anyMatch(c->"C03".equals(c.code())&&"FAIL".equals(c.status()));
    }
    @Test void absentDiskMeasurementIsUnknownInsteadOfPass() {
        when(logs.allDescriptions()).thenReturn(KafkaFuture.completedFuture(Map.of(1,Map.of("/data",new LogDirDescription(null,Map.of())))));
        assertThat(service.observe(cluster,Map.of("quorum_voters","101@192.0.2.1:9093")).checks())
                .anyMatch(c->c.code().equals("C08") && c.status().equals("UNKNOWN"));
    }
    @Test void identityMismatchBlocksAdmission() {
        cluster.setKafkaClusterId("different-id");
        assertThat(service.observe(cluster,Map.of("quorum_voters","101@192.0.2.1:9093")).checks())
                .anyMatch(c->c.code().equals("C01") && c.status().equals("FAIL"));
    }
    @Test void untrackedFencedBrokerStillOwnsItsNodeIdentity() {
        var registered=mock(DescribeClusterResult.class);
        when(admin.describeCluster(argThat(options->options.includeFencedBrokers()))).thenReturn(registered);
        when(registered.nodes()).thenReturn(KafkaFuture.completedFuture(List.of(new Node(1,"192.0.2.1",9092),new Node(2,"192.0.2.2",9092))));
        var result=service.observe(cluster,Map.of("quorum_voters","101@192.0.2.1:9093"));
        assertThat(result.nodeIds()).contains(2);
        assertThat(result.checks()).anyMatch(c->"C02".equals(c.code())&&"FAIL".equals(c.status()));
    }
}
