package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeReadinessProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.*;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ControllerAdditionPlanningServiceTest {
 final ClusterRepository clusters=mock(ClusterRepository.class);final HostRepository hosts=mock(HostRepository.class);final ClusterServiceAssignmentRepository assignments=mock(ClusterServiceAssignmentRepository.class);
 final HostStatusService connectivity=mock(HostStatusService.class);final JobService jobs=mock(JobService.class);final AddNodeKafkaReadiness kafka=mock(AddNodeKafkaReadiness.class);final AddNodeArtifactCatalog artifacts=mock(AddNodeArtifactCatalog.class);
 final ControllerAdditionPlanningService planning=new ControllerAdditionPlanningService(clusters,hosts,assignments,connectivity,jobs,kafka,artifacts,new AddNodeReadinessProperties(),new ObjectMapper().findAndRegisterModules());
 final Cluster cluster=new Cluster();final Host host=new Host();
 @BeforeEach void setup(){
  cluster.setId(UUID.randomUUID());cluster.setMode("kraft");cluster.setStatus("SUCCESS");cluster.setKafkaVersion("4.3.0");cluster.setKafkaClusterId("abcdefghijklmnopqrstuv");cluster.setConfigJson("{}");cluster.setNodeIds(List.of(1,101,102));
  var services=new ArrayList<ClusterServiceAssignment>();for(int id:List.of(1,101,102)){var s=new ClusterServiceAssignment();s.setHostId(id==102?"broker191":"node229");s.setRole(id==101?"controller":"broker");s.setNodeId(id);s.setConfigJson(id==102?"{\"service_name\":\"tantor-kafka-broker-102\",\"config_path\":\"/etc/tantor-kafka/add-broker/old/server.properties\"}":"{}");services.add(s);}cluster.setServices(services);
  host.setId("fresh-controller");host.setHostIp("192.0.2.30");host.setAgentVersion("candidate");when(hosts.findById(host.getId())).thenReturn(Optional.of(host));when(clusters.findWithServicesById(cluster.getId())).thenReturn(Optional.of(cluster));when(connectivity.isInfrastructureHost(host)).thenReturn(true);when(connectivity.agentConnectivityStatus(host)).thenReturn("ONLINE");
  when(artifacts.resolve("4.3.0","")).thenReturn(new AddNodeArtifactCatalog.Artifact(UUID.randomUUID().toString(),"4.3.0","a".repeat(64)));mode("STATIC");
 }
 void mode(String mode){var checks=List.of("C01","C02","C03","C05","C06","C07","C08","B09").stream().map(c->new Check(c,c,"COMMON","PASS","expected","observed","",Instant.now(),"KAFKA")).toList();when(kafka.observe(eq(cluster),anyMap())).thenReturn(new AddNodeKafkaReadiness.Observation(checks,Set.of(1,101,102),List.of("192.0.2.1:9092"),List.of("192.0.2.1:9093"),mode));}
 @Test void staticReviewIncludesEveryBrokerAndDoesNotExecute(){var r=planning.review(cluster.getId(),host.getId());assertEquals(8,r.steps().size());assertTrue(r.maintenanceRequired());assertFalse(r.executionEnabled());assertEquals(103,r.proposedControllerId());assertEquals(3,r.participants().size());assertTrue(r.participants().stream().anyMatch(p->p.serviceName().equals("tantor-kafka-broker-102")&&p.configurationPath().endsWith("server.properties")));assertTrue(r.participants().stream().allMatch(p->p.evidenceStatus().equals("RUNTIME_INSPECTION_REQUIRED")));verify(clusters,never()).save(any());verify(hosts,never()).save(any());verify(assignments,never()).save(any());verify(jobs,never()).createJob(any(),anyList());verify(jobs,never()).assertClusterMutable(any());}
 @Test void dynamicReviewHasObserverAdmissionAndNoExistingNodeStop(){mode("DYNAMIC");var r=planning.review(cluster.getId(),host.getId());assertFalse(r.maintenanceRequired());assertFalse(r.executionEnabled());assertEquals(7,r.steps().size());assertTrue(r.steps().stream().anyMatch(s->s.name().contains("Admit controller")));assertTrue(r.steps().stream().noneMatch(s->s.name().contains("stop brokers")));}
 @Test void unknownModeDoesNotDefaultToOnline(){mode("UNKNOWN");var r=planning.review(cluster.getId(),host.getId());assertTrue(r.steps().isEmpty());assertTrue(r.blockers().stream().anyMatch(s->s.contains("unknown")));assertFalse(r.executionEnabled());}
 @Test void occupiedHostAndUnresolvedOperationAreExplicitBlockers(){host.setClusterId(cluster.getId());when(jobs.getActiveClusterJobs()).thenReturn(List.of(Map.of("clusterId",cluster.getId().toString())));var r=planning.review(cluster.getId(),host.getId());assertTrue(r.blockers().stream().anyMatch(s->s.contains("fresh unassigned")));assertTrue(r.blockers().stream().anyMatch(s->s.contains("retained reservation")));assertFalse(r.executionEnabled());}
 @Test void contextBindsInventoryReferencesAndCandidateVersion(){var first=planning.review(cluster.getId(),host.getId());cluster.getServices().get(2).setConfigJson("{\"config_path\":\"/etc/new/broker.properties\"}");assertNotEquals(first.contextHash(),planning.review(cluster.getId(),host.getId()).contextHash());host.setAgentVersion("changed");assertNotEquals(first.contextHash(),planning.review(cluster.getId(),host.getId()).contextHash());}
 @Test void externalClusterIsRejectedBeforeKafkaObservation(){cluster.setOriginType("EXTERNAL");assertThrows(org.springframework.web.server.ResponseStatusException.class,()->planning.review(cluster.getId(),host.getId()));verifyNoInteractions(kafka);}
}
