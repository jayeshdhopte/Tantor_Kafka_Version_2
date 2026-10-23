package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RestartNodeDisplayServiceTest {
 final HostRepository hosts=mock(HostRepository.class);
 final ExternalClusterNodeRepository nodes=mock(ExternalClusterNodeRepository.class);
 final DiscoveryAgentRepository agents=mock(DiscoveryAgentRepository.class);
 final RestartNodeDisplayService display=new RestartNodeDisplayService(hosts,nodes,agents,new ObjectMapper());

 @Test void externalUsesRecordedHostnameAndActualBrokerAddress() {
  UUID id=UUID.randomUUID(),cluster=UUID.randomUUID();var node=new ExternalClusterNode();node.setId(id);node.setClusterId(cluster);node.setAgentId("agent");node.setHost("192.168.3.229");
  var agent=new DiscoveryAgent();agent.setClusterId(cluster);agent.setHostname("node229.translab.io");agent.setIpAddresses("[\"127.0.0.1\",\"10.0.0.10\"]");
  when(nodes.findById(id)).thenReturn(Optional.of(node));when(agents.findById("agent")).thenReturn(Optional.of(agent));
  assertThat(display.identity("external:"+id)).containsEntry("hostname","node229.translab.io").containsEntry("ipAddress","192.168.3.229");
  node.setHost("node229.translab.io");assertThat(display.identity("external:"+id)).containsEntry("ipAddress","10.0.0.10");
  verifyNoInteractions(hosts);
 }
 @Test void internalUsesSameIdentityFields() {
  var host=new Host();host.setHostname("internal.example");host.setHostIp("192.168.3.191");when(hosts.findById("host")).thenReturn(Optional.of(host));
  assertThat(display.identity("host")).containsEntry("hostname","internal.example").containsEntry("ipAddress","192.168.3.191");
 }
 @Test void savedHistorySurvivesRemovedDiscoveryAndPreservesExecutionIdentity() {
  var original=new JobStep();original.setId(UUID.randomUUID());original.setTargetId("external:"+UUID.randomUUID());original.setExternalKafkaTaskId(UUID.randomUUID());original.setStatus(JobStepStatus.SUCCESS);
  original.setName("old UUID label");original.setPayload("{\"hostId\":\""+original.getTargetId()+"\",\"role\":\"broker\",\"nodeId\":2,\"hostname\":\"recorded.example\",\"ipAddress\":\"192.168.3.174\"}");
  JobStep view=display.historyView(original);
  assertThat(view).isNotSameAs(original);assertThat(view.getName()).isEqualTo("Restart broker node 2 - recorded.example (192.168.3.174)");
  assertThat(view.getTargetId()).isEqualTo(original.getTargetId());assertThat(view.getAgentTaskId()).isEqualTo(original.getAgentTaskId());assertThat(view.getStatus()).isEqualTo(JobStepStatus.SUCCESS);
  assertThat(original.getName()).isEqualTo("old UUID label");verifyNoInteractions(nodes,hosts,agents);
 }
 @Test void oldHistoryIsEnrichedWithoutUpdatingItsPayload() {
  var original=new JobStep();original.setTargetId("internal-id");original.setPayload("{\"hostId\":\"internal-id\",\"role\":\"controller\",\"nodeId\":101}");
  var host=new Host();host.setHostname("controller.example");host.setHostIp("192.168.3.174");when(hosts.findById("internal-id")).thenReturn(Optional.of(host));
  assertThat(display.historyView(original).getName()).isEqualTo("Restart controller node 101 - controller.example (192.168.3.174)");
  assertThat(original.getPayload()).doesNotContain("hostname");verify(hosts,never()).save(any());
 }
 @Test void missingNodeHasExplicitUnavailableLabels() {
  assertThat(display.identity("external:"+UUID.randomUUID())).containsEntry("hostname","Hostname not reported").containsEntry("ipAddress","IP not reported");
 }
}
