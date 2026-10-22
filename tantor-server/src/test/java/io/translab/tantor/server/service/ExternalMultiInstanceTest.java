package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
class ExternalMultiInstanceTest {
 @Test void defaultsArePredictableAndAvoidExistingDirectoriesWithoutMovingThem(){
  var manager=new ExternalDataServiceManager(null,null,null,null,null,null,new ObjectMapper());
  assertThat(manager.availablePaths("schema_registry",List.of()))
    .containsEntry("install_dir","/opt/tantor/schema-registry")
    .containsEntry("config_dir","/opt/tantor/schema-registry/etc/schema-registry");
  var existing=new ExternalDataService();existing.setStatus("ACTIVE");
  existing.setConfigJson("{\"install_dir\":\"/opt/tantor/kafka-connect\",\"log_dir\":\"/var/log/tantor/kafka-connect\"}");
  var before=existing.getConfigJson();
  assertThat(manager.availablePaths("kafka_connect",List.of(existing)))
    .containsEntry("install_dir","/opt/tantor/kafka-connect-2")
    .containsEntry("plugin_dir","/opt/tantor/kafka-connect-2/plugins")
    .containsEntry("log_dir","/var/log/tantor/kafka-connect-2");
  assertThat(existing.getConfigJson()).isEqualTo(before);
  existing.setStatus("DELETED");
  assertThat(manager.availablePaths("kafka_connect",List.of(existing))).containsEntry("install_dir","/opt/tantor/kafka-connect");
 }
 @Test void multipleWorkersShareGroupButHaveDistinctUnitsAndPaths(){
  var clusters=mock(ExternalClusterRepository.class);var agents=mock(DiscoveryAgentRepository.class);var services=mock(ExternalDataServiceRepository.class);var tasks=mock(ExternalServiceTaskRepository.class);var jobs=mock(JobService.class);var artifacts=mock(ExternalServiceArtifacts.class);
  var manager=new ExternalDataServiceManager(clusters,agents,services,tasks,jobs,artifacts,new ObjectMapper());
  var cluster=new ExternalCluster();cluster.setId(UUID.randomUUID());cluster.setStatus("SUCCESS");cluster.setSecurityProtocol("PLAINTEXT");cluster.setBootstrapServers("broker:9092");cluster.setBrokerCount(3);
  when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));when(clusters.findForServiceUpdate(cluster.getId())).thenReturn(Optional.of(cluster));
  var agent=new DiscoveryAgent();agent.setId("agent");agent.setClusterId(cluster.getId());agent.setVersion("3.4.0-dataservices.2");agent.setHostname("broker.example");agent.setIpAddresses("[\"192.168.3.174\"]");agent.setStatus("ONLINE");agent.setLastHeartbeat(OffsetDateTime.now());agent.setCanDeployServices(true);when(agents.findById("agent")).thenReturn(Optional.of(agent));
  when(artifacts.verifiedChecksum(any(),any())).thenReturn("a".repeat(64));
  List<ExternalDataService> saved=new ArrayList<>();when(services.saveAndFlush(any())).thenAnswer(i->{ExternalDataService s=i.getArgument(0);s.setId(UUID.randomUUID());saved.add(s);return s;});
  when(jobs.createJob(any(),anyList())).thenAnswer(i->{Job j=i.getArgument(0);j.setId(UUID.randomUUID());return j;});
  Map<String,Object> first=Map.of("host_id","agent","artifact_id",UUID.randomUUID().toString(),"port",8083,"install_dir","/opt/connect-a","log_dir","/var/log/connect-a","working_dir","/var/lib/connect-a");
  Map<String,Object> second=Map.of("host_id","agent","artifact_id",UUID.randomUUID().toString(),"port",8084,"install_dir","/opt/connect-b","log_dir","/var/log/connect-b","working_dir","/var/lib/connect-b");
  manager.create(cluster.getId(),Map.of("kafka_connect",List.of(first,second)),"operator");
  assertThat(saved).hasSize(2);var a=manager.read(saved.get(0).getConfigJson());var b=manager.read(saved.get(1).getConfigJson());
  assertThat(a.get("group_id")).isEqualTo(b.get("group_id"));assertThat(a.get("service_name")).isNotEqualTo(b.get("service_name"));assertThat(a.get("install_dir")).isNotEqualTo(b.get("install_dir"));
  assertThat(a).containsEntry("host_name","192.168.3.174").containsEntry("host_display_name","broker.example");
  assertThat(a).containsEntry("service_name","kafka-connect");
  assertThat(b).containsEntry("service_name","kafka-connect-2");
  assertThat(manager.availableServiceName("schema_registry","agent",saved)).isEqualTo("schema-registry");
  assertThat(manager.availableServiceName("kafka_connect","another-host",saved)).isEqualTo("kafka-connect");
  verify(jobs).createJob(argThat(j->j.getResourceKey().equals("cluster:"+cluster.getId())),argThat(steps->steps.size()==8));
 }
 @Test void serviceAddressPrefersReportedIpWithoutRequiringDisplayHostnameDns(){
  var manager=new ExternalDataServiceManager(null,null,null,null,null,null,new ObjectMapper());
  var agent=new DiscoveryAgent();agent.setHostname("not-in-dns.example");agent.setIpAddresses("[\"127.0.0.1\",\"0.0.0.0\",\"169.254.1.1\",\"999.1.2.3\",\"192.168.3.174\"]");
  assertThat(manager.serviceHost(agent)).isEqualTo("192.168.3.174");
  agent.setIpAddresses("[]");assertThat(manager.serviceHost(agent)).isEqualTo("not-in-dns.example");
  agent.setIpAddresses("invalid");assertThatThrownBy(()->manager.serviceHost(agent)).hasMessageContaining("refresh agent discovery");
 }
 @Test void existingQueuedInstallRefreshesItsAdvertisedAddressBeforeAgentExecution(){
  var clusters=mock(ExternalClusterRepository.class);var agents=mock(DiscoveryAgentRepository.class);var services=mock(ExternalDataServiceRepository.class);var tasks=mock(ExternalServiceTaskRepository.class);
  var manager=new ExternalDataServiceManager(clusters,agents,services,tasks,null,null,new ObjectMapper());
  UUID cluster=UUID.randomUUID(),serviceId=UUID.randomUUID();
  var external=new ExternalCluster();external.setId(cluster);external.setStatus("SUCCESS");when(clusters.findById(cluster)).thenReturn(Optional.of(external));
  var agent=new DiscoveryAgent();agent.setId("agent");agent.setClusterId(cluster);agent.setVersion("3.4.0-dataservices.2");agent.setStatus("ONLINE");agent.setLastHeartbeat(OffsetDateTime.now());agent.setCanDeployServices(true);agent.setHostname("not-in-dns.example");agent.setIpAddresses("[\"192.168.3.174\"]");
  when(agents.findById("agent")).thenReturn(Optional.of(agent));
  var service=new ExternalDataService();service.setId(serviceId);service.setClusterId(cluster);service.setAgentId("agent");service.setRole("kafka_connect");service.setConfigJson("{\"host_name\":\"not-in-dns.example\",\"artifact_id\":\"artifact\",\"checksum\":\"checksum\"}");
  var task=new ExternalServiceTask();task.setId(UUID.randomUUID());task.setServiceId(serviceId);task.setCommand("INSTALL");task.setDeadline(Instant.now().plusSeconds(60));
  when(tasks.findFirstByAgentIdAndStatusInOrderByCreatedAtAsc(eq("agent"),any())).thenReturn(Optional.of(task));when(services.findById(serviceId)).thenReturn(Optional.of(service));
  var claimed=manager.claim("agent");
  assertThat(manager.read(service.getConfigJson())).containsEntry("host_name","192.168.3.174").containsEntry("host_display_name","not-in-dns.example");
  assertThat((Map<?,?>)claimed.get("parameters")).isEqualTo(manager.read(service.getConfigJson()));
  verify(services).save(service);
 }
}
