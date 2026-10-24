package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.config.MonitoringHttpClientConfiguration.MonitoringRestTemplate;
import io.translab.tantor.server.config.MonitoringProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
class ExternalMetricsOptionsTest {
 @Test void configuredPortsOverrideDiscoveredPortsAndDisablingRemovesOnlyThatClustersTargets(){
  var clusters=mock(ClusterRepository.class);var externals=mock(ExternalClusterRepository.class);var nodes=mock(ExternalClusterNodeRepository.class);
  var service=new PrometheusMonitoringService(clusters,externals,nodes,mock(HostRepository.class),mock(EncryptionService.class),new ObjectMapper(),mock(MonitoringRestTemplate.class),new MonitoringProperties());
  ReflectionTestUtils.setField(service,"kafkaExporterPortBase",9308);
  var id=UUID.randomUUID();var external=new ExternalCluster();external.setId(id);external.setName("external");external.setConnectionSettings("{\"metrics\":{\"enabled\":true,\"brokerPort\":17071,\"controllerPort\":17072},\"masking\":[]}");
  var broker=new ExternalClusterNode();broker.setNodeId(1);broker.setIsBroker(true);broker.setHost("192.168.1.1");broker.setJmxExporterPort(7071);
  var controller=new ExternalClusterNode();controller.setNodeId(101);controller.setIsController(true);controller.setHost("192.168.1.2");controller.setJmxExporterPort(7072);
  when(externals.findById(id)).thenReturn(Optional.of(external));when(externals.findByStatusNot("DELETED")).thenReturn(List.of(external));when(nodes.findByClusterId(id)).thenReturn(List.of(broker,controller));
  assertThat(service.brokerJmxTargets(id)).containsEntry(1,"192.168.1.1:17071");
  assertThat(service.prometheusTargets()).flatExtracting(PrometheusMonitoringService.SdTargetGroup::getTargets).contains("192.168.1.1:17071","192.168.1.2:17072");
  external.setConnectionSettings("{\"metrics\":{\"enabled\":false},\"masking\":[]}");
  assertThat(service.brokerJmxTargets(id)).isEmpty();assertThat(service.prometheusTargets()).isEmpty();
 }
}
