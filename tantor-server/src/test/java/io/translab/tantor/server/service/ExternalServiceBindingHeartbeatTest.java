package io.translab.tantor.server.service;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ExternalServiceBindingHeartbeatTest {
 @Test void startupHeartbeatDoesNotEraseConfirmedUnit(){
  var node=node();var report=new ExternalClusterService.ExternalDiscoveryReport();report.setConfigFile("/opt/kafka/config/broker.properties");report.setSystemdService("");
  ExternalClusterService.updateDiscoveryServiceBinding(node,report,agent("a"));
  assertThat(node.getSystemdService()).isEqualTo("broker.service");
 }
 @Test void changedConfigOrAgentCannotReuseStaleUnit(){
  var node=node();var report=new ExternalClusterService.ExternalDiscoveryReport();report.setConfigFile("/different/broker.properties");
  ExternalClusterService.updateDiscoveryServiceBinding(node,report,agent("a"));assertThat(node.getSystemdService()).isNull();
  node=node();report.setConfigFile(node.getConfigFile());ExternalClusterService.updateDiscoveryServiceBinding(node,report,agent("b"));assertThat(node.getSystemdService()).isNull();
 }
 @Test void newlyReportedUnitUpdatesBinding(){
  var node=node();var report=new ExternalClusterService.ExternalDiscoveryReport();report.setSystemdService("new-broker.service");
  ExternalClusterService.updateDiscoveryServiceBinding(node,report,agent("a"));assertThat(node.getSystemdService()).isEqualTo("new-broker.service");
 }
 private ExternalClusterNode node(){var n=new ExternalClusterNode();n.setConfigFile("/opt/kafka/config/broker.properties");n.setAgentId("a");n.setSystemdService("broker.service");return n;}
 private DiscoveryAgent agent(String id){var a=new DiscoveryAgent();a.setId(id);return a;}
}
