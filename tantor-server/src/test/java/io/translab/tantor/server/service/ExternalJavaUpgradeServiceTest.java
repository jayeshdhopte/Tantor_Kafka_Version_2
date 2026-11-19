package io.translab.tantor.server.service;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExternalJavaUpgradeServiceTest {
 final ExternalKafkaManagement management=mock(ExternalKafkaManagement.class);
 final ExternalClusterRepository clusters=mock(ExternalClusterRepository.class);
 final ExternalClusterNodeRepository nodes=mock(ExternalClusterNodeRepository.class);
 final AddNodeArtifactCatalog catalog=mock(AddNodeArtifactCatalog.class);
 final JdbcTemplate jdbc=mock(JdbcTemplate.class);
 final JobService jobs=mock(JobService.class);
 final ExternalJavaUpgradeService service=new ExternalJavaUpgradeService(management,clusters,nodes,catalog,jdbc,jobs);
 final UUID clusterId=UUID.randomUUID(),artifact=UUID.randomUUID();
 final String target="external:"+UUID.randomUUID();
 ExternalClusterNode node() {
  var n=new ExternalClusterNode();n.setClusterId(clusterId);n.setAgentId("agent-1");n.setHost("external.test");n.setNodeId(7);n.setSystemdService("custom-broker.service");n.setConfigFile("/opt/kafka/config/broker.properties");
  when(management.node(target)).thenReturn(n);when(management.agentVersion(target)).thenReturn("3.4.0-an.5.java-upgrade.1");when(nodes.findByAgentId("agent-1")).thenReturn(List.of(n));return n;
 }
 @Test void refusesLegacyAgentBeforeAnyTaskIsEnqueued() {
  node();when(management.agentVersion(target)).thenReturn("3.4.0-an.5");
  assertThatThrownBy(()->service.requireTarget(clusterId,target,true)).hasMessageContaining("Java-upgrade capable");verifyNoInteractions(jdbc);
 }
 @Test void refusesSharedKafkaHost() {
  var n=node();when(nodes.findByAgentId("agent-1")).thenReturn(List.of(n,n));
  assertThatThrownBy(()->service.requireTarget(clusterId,target,true)).hasMessageContaining("one Kafka process");
 }
 @Test void refusesWrongCluster() {
  node();assertThatThrownBy(()->service.requireTarget(UUID.randomUUID(),target,true)).hasMessageContaining("another cluster");
 }
 @Test void refusesAuthenticatedHealthProbeUntilSupported() {
  ExternalCluster c=new ExternalCluster();c.setId(clusterId);c.setKafkaMode("kraft");c.setKafkaClusterId("kafka-1");c.setSecurityProtocol("SASL_SSL");when(clusters.findById(clusterId)).thenReturn(Optional.of(c));
  assertThatThrownBy(()->service.cluster(clusterId)).hasMessageContaining("PLAINTEXT");
 }
 @Test void distributionPinsCatalogChecksumInExternalTask() {
  var n=node();ExternalCluster c=new ExternalCluster();c.setId(clusterId);c.setKafkaMode("kraft");c.setKafkaClusterId("kafka-1");c.setSecurityProtocol("PLAINTEXT");when(clusters.findById(clusterId)).thenReturn(Optional.of(c));
  when(management.resolve(clusterId)).thenReturn(new Cluster());
  when(catalog.resolveType("JAVA","21.0.13",artifact.toString())).thenReturn(new AddNodeArtifactCatalog.Artifact(artifact.toString(),"21.0.13","a".repeat(64)));
  when(management.enqueue(eq(clusterId),eq(target),eq("STAGE_JAVA"),anyMap())).thenReturn(UUID.randomUUID());
  var request=new ParcelService.ParcelActionRequest();request.setHostIds(List.of(target));request.setServiceType("JAVA");request.setVersion("21.0.13");request.setChecksum("untrusted-client-hash");request.setParcelDir("/srv/tantor/parcels");
  assertThat(service.distribute(artifact,request).getFirst().getStatus()).isEqualTo("DISTRIBUTING");
  var params=org.mockito.ArgumentCaptor.forClass(Map.class);
  verify(management).enqueue(eq(clusterId),eq(target),eq("STAGE_JAVA"),params.capture());
  assertThat(params.getValue()).containsEntry("checksum","a".repeat(64)).containsEntry("target_id",target);
 }
}
