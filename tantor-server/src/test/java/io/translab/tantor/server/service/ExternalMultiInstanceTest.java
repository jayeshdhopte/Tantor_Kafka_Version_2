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
 @Test void multipleWorkersShareGroupButHaveDistinctUnitsAndPaths(){
  var clusters=mock(ExternalClusterRepository.class);var agents=mock(DiscoveryAgentRepository.class);var services=mock(ExternalDataServiceRepository.class);var tasks=mock(ExternalServiceTaskRepository.class);var jobs=mock(JobService.class);var artifacts=mock(ExternalServiceArtifacts.class);
  var manager=new ExternalDataServiceManager(clusters,agents,services,tasks,jobs,artifacts,new ObjectMapper());
  var cluster=new ExternalCluster();cluster.setId(UUID.randomUUID());cluster.setStatus("SUCCESS");cluster.setSecurityProtocol("PLAINTEXT");cluster.setBootstrapServers("broker:9092");cluster.setBrokerCount(3);
  when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));when(clusters.findForServiceUpdate(cluster.getId())).thenReturn(Optional.of(cluster));
  var agent=new DiscoveryAgent();agent.setId("agent");agent.setClusterId(cluster.getId());agent.setVersion("3.4.0-dataservices.2");agent.setHostname("broker.example");agent.setStatus("ONLINE");agent.setLastHeartbeat(OffsetDateTime.now());agent.setCanDeployServices(true);when(agents.findById("agent")).thenReturn(Optional.of(agent));
  when(artifacts.verifiedChecksum(any(),any())).thenReturn("a".repeat(64));
  List<ExternalDataService> saved=new ArrayList<>();when(services.saveAndFlush(any())).thenAnswer(i->{ExternalDataService s=i.getArgument(0);s.setId(UUID.randomUUID());saved.add(s);return s;});
  when(jobs.createJob(any(),anyList())).thenAnswer(i->{Job j=i.getArgument(0);j.setId(UUID.randomUUID());return j;});
  Map<String,Object> first=Map.of("host_id","agent","artifact_id",UUID.randomUUID().toString(),"port",8083,"install_dir","/opt/connect-a","log_dir","/var/log/connect-a","working_dir","/var/lib/connect-a");
  Map<String,Object> second=Map.of("host_id","agent","artifact_id",UUID.randomUUID().toString(),"port",8084,"install_dir","/opt/connect-b","log_dir","/var/log/connect-b","working_dir","/var/lib/connect-b");
  manager.create(cluster.getId(),Map.of("kafka_connect",List.of(first,second)),"operator");
  assertThat(saved).hasSize(2);var a=manager.read(saved.get(0).getConfigJson());var b=manager.read(saved.get(1).getConfigJson());
  assertThat(a.get("group_id")).isEqualTo(b.get("group_id"));assertThat(a.get("service_name")).isNotEqualTo(b.get("service_name"));assertThat(a.get("install_dir")).isNotEqualTo(b.get("install_dir"));
  verify(jobs).createJob(argThat(j->j.getResourceKey().equals("cluster:"+cluster.getId())),argThat(steps->steps.size()==8));
 }
}
