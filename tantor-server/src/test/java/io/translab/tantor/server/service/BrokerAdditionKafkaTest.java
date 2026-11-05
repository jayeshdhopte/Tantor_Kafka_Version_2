package io.translab.tantor.server.service;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.*;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class BrokerAdditionKafkaTest {
 final KafkaAdminService clients=mock(KafkaAdminService.class);final AddNodeKafkaReadiness health=mock(AddNodeKafkaReadiness.class);
 final AdminClient admin=mock(AdminClient.class);final Cluster cluster=new Cluster();final BrokerAdditionKafka service=new BrokerAdditionKafka(clients,health);
 final BrokerAdditionPlan plan=mock(BrokerAdditionPlan.class);String baseline;
 @BeforeEach void setup(){cluster.setId(UUID.randomUUID());cluster.setKafkaClusterId("abcdefghijklmnopqrstuv");var old=new ClusterServiceAssignment();old.setRole("broker");old.setNodeId(1);cluster.setServices(List.of(old));when(clients.getAdminClient(cluster.getId())).thenReturn(admin);when(plan.nodeId()).thenReturn(4);when(plan.kafkaClusterId()).thenReturn(cluster.getKafkaClusterId());when(plan.hostIp()).thenReturn("192.0.2.20");when(plan.brokerPort()).thenReturn(9092);when(plan.brokerRack()).thenReturn("");
  var quorum=mock(QuorumInfo.class);var voter=mock(QuorumInfo.ReplicaState.class);when(voter.replicaId()).thenReturn(101);when(voter.replicaDirectoryId()).thenReturn(Uuid.randomUuid());when(quorum.voters()).thenReturn(List.of(voter));var qr=mock(DescribeMetadataQuorumResult.class);when(admin.describeMetadataQuorum()).thenReturn(qr);when(qr.quorumInfo()).thenReturn(KafkaFuture.completedFuture(quorum));baseline=service.voterBaseline(cluster);
  var checks=List.of("C01","C03","C06","C07","C08","B09").stream().map(code->new Check(code,"health","COMMON","PASS","expected","observed","",Instant.now(),"KAFKA")).toList();when(health.observe(eq(cluster),anyMap())).thenReturn(new AddNodeKafkaReadiness.Observation(checks,Set.of(),List.of(),List.of(),"STATIC"));
 }
 BrokerAdditionLedger.Payload payload(String voters){return new BrokerAdditionLedger.Payload(plan,"","digest",voters,new BrokerAdditionLedger.RuntimeOptions("","",0,100000));}
 void brokers(List<Node> nodes){var description=mock(DescribeClusterResult.class);when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(description);when(description.clusterId()).thenReturn(KafkaFuture.completedFuture(cluster.getKafkaClusterId()));when(description.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));}
 @Test void fencedOrMissingCandidateCannotPublish(){brokers(List.of(new Node(1,"192.0.2.10",9092)));assertThrows(BrokerAdditionKafka.RegistrationPending.class,()->service.verify(cluster,payload(baseline),Map.of()));verify(admin).describeCluster(argThat(options->!options.includeFencedBrokers()));verifyNoInteractions(health);}
 @Test void foreignOrMissingExistingBrokersAreNotNormalRegistrationDelay(){
  for(var nodes:List.of(List.of(new Node(4,"192.0.2.20",9092)),List.of(new Node(1,"192.0.2.10",9092),new Node(9,"192.0.2.99",9092)))) {
   brokers(nodes);var error=assertThrows(IllegalStateException.class,()->service.verify(cluster,payload(baseline),Map.of()));assertFalse(error instanceof BrokerAdditionKafka.RegistrationPending);
  }
 }
 @Test void candidateEndpointAndVoterIdentityMustMatch(){brokers(List.of(new Node(1,"192.0.2.10",9092),new Node(4,"192.0.2.99",9092)));assertThrows(IllegalStateException.class,()->service.verify(cluster,payload(baseline),Map.of()));brokers(List.of(new Node(1,"192.0.2.10",9092),new Node(4,"192.0.2.20",9092)));assertThrows(IllegalStateException.class,()->service.verify(cluster,payload("changed-voters"),Map.of()));}
 @Test void registeredRackMustMatchCandidatePlan(){when(plan.brokerRack()).thenReturn("rack2");brokers(List.of(new Node(1,"192.0.2.10",9092,"rack1"),new Node(4,"192.0.2.20",9092,"rack1")));assertThrows(IllegalStateException.class,()->service.verify(cluster,payload(baseline),Map.of()));}
 @Test void matchingUnfencedRegistrationAndHealthProvideEvidence(){brokers(List.of(new Node(1,"192.0.2.10",9092),new Node(4,"192.0.2.20",9092)));assertTrue(service.verify(cluster,payload(baseline),Map.of()).contains("unfenced broker 4"));verify(health).observe(cluster,Map.of());}
 @Test void candidateRackIsExplicitAndDoesNotCopyAnExistingBrokersRack(){
  var result=mock(DescribeConfigsResult.class);when(admin.describeConfigs(anyCollection())).thenReturn(result);
  var resource=new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.BROKER,"1");
  when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource,new Config(List.of(new ConfigEntry("broker.rack","rack1"))))));
  when(plan.brokerRack()).thenReturn("");assertThrows(IllegalStateException.class,()->service.properties(cluster,plan));
  when(plan.brokerRack()).thenReturn("rack2");when(plan.dataPaths()).thenReturn(List.of("/data/broker"));when(plan.metadataPath()).thenReturn("");when(plan.quorumMode()).thenReturn("STATIC");when(plan.quorumVoters()).thenReturn("101@192.0.2.10:9093");
  String properties=service.properties(cluster,plan);assertTrue(properties.contains("broker.rack=rack2\n"));assertFalse(properties.contains("broker.rack=rack1\n"));
 }
}
