package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
class ExternalKafkaManagementTest {
 final ExternalClusterRepository clusters=mock(ExternalClusterRepository.class);
 final ExternalClusterNodeRepository nodes=mock(ExternalClusterNodeRepository.class);
 final DiscoveryAgentRepository agents=mock(DiscoveryAgentRepository.class);
 final JdbcTemplate jdbc=mock(JdbcTemplate.class);
 final ExternalKafkaManagement management=new ExternalKafkaManagement(clusters,nodes,agents,jdbc,new ObjectMapper());
 final UUID cluster=UUID.randomUUID(), nodeId=UUID.randomUUID();
 ExternalClusterNode node;DiscoveryAgent agent;ExternalCluster external;
 @BeforeEach void prepare(){
  external=new ExternalCluster();external.setId(cluster);external.setName("external");external.setKafkaClusterId("identity-1");external.setKafkaMode("kraft");
  node=new ExternalClusterNode();node.setId(nodeId);node.setClusterId(cluster);node.setAgentId("discovery-1");node.setNodeId(7);node.setHost("broker.example");node.setSystemdService("custom-kafka-broker.service");node.setConfigFile("/etc/company/broker.properties");node.setIsBroker(true);node.setIsController(false);node.setCanManageKafka(true);node.setCanWriteConfig(true);node.setCanRestart(true);
  agent=new DiscoveryAgent();agent.setId("discovery-1");agent.setClusterId(cluster);agent.setVersion("3.4.0-dataservices.2-configchange.8-rollingrestart.2");agent.setStatus("ONLINE");agent.setLastHeartbeat(OffsetDateTime.now());agent.setCanExecuteTasks(true);
  when(clusters.findById(cluster)).thenReturn(Optional.of(external));when(nodes.findByClusterId(cluster)).thenReturn(List.of(node));when(nodes.findById(nodeId)).thenReturn(Optional.of(node));when(agents.findById("discovery-1")).thenReturn(Optional.of(agent));
 }
 @Test void discoveryDefinesActualUnitAndPath(){
  var virtual=management.resolve(cluster);var service=virtual.getServices().getFirst();
  assertThat(RollingRestartPlan.key(service)).isEqualTo("external:"+nodeId+":custom-kafka-broker.service");
  assertThat(service.getConfigJson()).contains("/etc/company/broker.properties");
  assertThat(RollingRestartPlan.select(virtual,"BROKERS",List.of())).hasSize(1);
  assertThat(RollingRestartPlan.select(virtual,"SELECTED",List.of(RollingRestartPlan.key(service)))).hasSize(1);
 }
 @Test void olderAgentsAndDisabledCapabilitiesAreBlocked(){
  agent.setVersion("3.3.0-dataservices.1");assertThatThrownBy(()->management.require("external:"+nodeId,true,true)).hasMessageContaining("3.4.0");
  agent.setVersion("3.4.0-configchange.8");node.setCanWriteConfig(false);assertThatThrownBy(()->management.require("external:"+nodeId,true,false)).hasMessageContaining("configuration writes");
  node.setCanWriteConfig(true);node.setCanRestart(false);assertThatThrownBy(()->management.require("external:"+nodeId,false,true)).hasMessageContaining("restarts");
  agent.setLastHeartbeat(OffsetDateTime.now().minusMinutes(5));assertThatThrownBy(()->management.require("external:"+nodeId,false,false)).hasMessageContaining("3.4.0");
 }
 @Test void crossClusterTasksAndLifecycleCommandsAreRejected(){
  assertThatThrownBy(()->management.enqueue(UUID.randomUUID(),"external:"+nodeId,"READ_KAFKA_PROPERTIES",Map.of())).hasMessageContaining("another cluster");
  assertThatThrownBy(()->management.enqueue(cluster,"external:"+nodeId,"DELETE_CLUSTER",Map.of())).hasMessageContaining("Unsupported");
  verifyNoInteractions(jdbc);
 }
 @Test void topologyDoesNotGuessForLegacyDiscovery(){
  node.setSystemdService(null);assertThatThrownBy(()->management.resolve(cluster)).hasMessageContaining("refresh discovery");
 }
}
