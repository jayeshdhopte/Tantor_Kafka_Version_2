package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AddNodeProcessInspectionServiceTest {
    final ClusterRepository clusters=mock(ClusterRepository.class); final HostRepository hosts=mock(HostRepository.class);
    final TaskRepository tasks=mock(TaskRepository.class); final HostStatusService connectivity=mock(HostStatusService.class);
    final JobService jobs=mock(JobService.class); final AuditService audit=mock(AuditService.class);
    final AddNodeInspectionProperties policy=new AddNodeInspectionProperties(); final ObjectMapper mapper=new ObjectMapper().findAndRegisterModules();
    final AddNodeProcessInspectionService service=new AddNodeProcessInspectionService(clusters,hosts,tasks,connectivity,jobs,policy,mapper,audit);
    final Cluster cluster=new Cluster();final Host host=new Host();final ClusterServiceAssignment member=new ClusterServiceAssignment();
    final AtomicReference<Task> saved=new AtomicReference<>();
    @BeforeEach void setup(){
        cluster.setId(UUID.randomUUID());cluster.setMode("kraft");cluster.setStatus("SUCCESS");cluster.setKafkaClusterId("a".repeat(22));cluster.setKafkaVersion("4.3.0");cluster.setConfigJson("{}");
        host.setId("permitted-host");host.setHostIp("192.0.2.20");host.setAgentVersion("1.0.0-addnode.6");host.setClusterId(cluster.getId());
        member.setId(UUID.randomUUID());member.setHostId(host.getId());member.setRole("broker");member.setNodeId(1);member.setConfigJson("{\"service_name\":\"tantor-kafka-broker-1\",\"config_path\":\"/opt/kafka/config/broker.properties\"}");cluster.setServices(List.of(member));
        policy.setAllowedHostIds(List.of(host.getId()));
        when(clusters.findWithServicesById(cluster.getId())).thenReturn(Optional.of(cluster));when(hosts.findById(host.getId())).thenReturn(Optional.of(host));
        when(connectivity.isInfrastructureHost(host)).thenReturn(true);when(connectivity.agentConnectivityStatus(host)).thenReturn("ONLINE");
        when(tasks.saveAndFlush(any())).thenAnswer(inv->{Task t=inv.getArgument(0);t.setId(UUID.randomUUID());saved.set(t);return t;});
        when(tasks.findById(any())).thenAnswer(inv->Optional.ofNullable(saved.get()).filter(t->t.getId().equals(inv.getArgument(0))));
    }
    AddNodeProcessInspectionService.View start(){return service.start(cluster.getId(),member.getId());}
    AddNodeProcessInspectionService.Request request()throws Exception {var params=mapper.readTree(saved.get().getParameters());return mapper.readValue(params.path("process_inspection_request").asText(),AddNodeProcessInspectionService.Request.class);}
    Map<String,Object> evidence()throws Exception {
        var r=request();var e=mapper.convertValue(r,new com.fasterxml.jackson.core.type.TypeReference<Map<String,Object>>(){});
        e.remove("startedAt");e.remove("expiresAt");
        e.put("configSha256","b".repeat(64));e.put("unitSha256","c".repeat(64));e.put("pid",42);e.put("processStartTicks","123456");e.put("serviceActive",true);e.put("metadata",List.of(Map.of("path","/data/kafka/broker","clusterId",r.kafkaClusterId(),"nodeId",1,"directoryId","d".repeat(22))));
        e.put("identityVerified",true);e.put("versionVerified",false);e.put("executionReady",false);e.put("observedAt",Instant.now().toString());e.put("blockers",List.of("VERSION_NOT_VERIFIED"));return e;
    }
    AddNodeProcessInspectionService.View complete(Map<String,Object> result)throws Exception {saved.get().setStatus("SUCCESS");saved.get().setLogOutput(mapper.writeValueAsString(result));return service.get(cluster.getId(),saved.get().getId());}
    @Test void queuesOnlyTheSelectedServiceAsAReadOnlyTask()throws Exception {
        var v=start();var r=request();assertEquals("RUNNING",v.status());assertFalse(v.executionReady());assertEquals("INSPECT_ADD_NODE_PROCESS",saved.get().getCommand());assertEquals(member.getNodeId(),r.nodeId());assertEquals("tantor-kafka-broker-1.service",r.serviceName());assertEquals(host.getId(),r.hostId());
        verify(jobs,never()).createJob(any(),anyList());verify(hosts,never()).save(any());verify(clusters,never()).save(any());verify(jobs,never()).assertClusterMutable(any());
    }
    @Test void observationCannotClaimArtifactQuorumOrExecutionReadiness()throws Exception {start();var v=complete(evidence());assertEquals("OBSERVED",v.status());assertFalse(v.executionReady());assertFalse(v.evidence().versionVerified());assertFalse(v.evidence().executionReady());}
    @Test void excludedNodeCannotBeQueuedEvenWhenExplicitlyAllowed(){
        host.setHostIp("192.168.3.213");policy.setExcludedHostIps(List.of("192.168.3.213"));assertThrows(org.springframework.web.server.ResponseStatusException.class,this::start);verifyNoInteractions(tasks,connectivity);
    }
    @Test void defaultPolicyDeniesEveryHostUntilExplicitlyConfigured(){policy.setAllowedHostIds(List.of());assertThrows(org.springframework.web.server.ResponseStatusException.class,this::start);verifyNoInteractions(tasks);}
    @Test void oldAgentMissingPathsAndWrongServiceIdsCannotDispatch(){
        host.setAgentVersion("1.0.0-addnode.5");assertThrows(org.springframework.web.server.ResponseStatusException.class,this::start);
        host.setAgentVersion("1.0.0-addnode.6");member.setConfigJson("{}");assertThrows(org.springframework.web.server.ResponseStatusException.class,this::start);
        assertThrows(org.springframework.web.server.ResponseStatusException.class,()->service.start(cluster.getId(),UUID.randomUUID()));verifyNoInteractions(tasks);
    }
    @Test void activeClusterOperationPreventsObservationScheduling(){when(jobs.getActiveClusterJobs()).thenReturn(List.of(Map.of("clusterId",cluster.getId().toString())));assertThrows(org.springframework.web.server.ResponseStatusException.class,this::start);verifyNoInteractions(tasks);}
    @Test void changedConfigOrAgentVersionInvalidatesPreviouslyCompletedEvidence()throws Exception {start();saved.get().setStatus("SUCCESS");saved.get().setLogOutput(mapper.writeValueAsString(evidence()));host.setAgentVersion("1.0.0-addnode.7");assertEquals("STALE",service.get(cluster.getId(),saved.get().getId()).status());}
    @Test void unmatchedNonceAndSelfCertifiedExecutionAreRejected()throws Exception {
        start();var e=evidence();e.put("inspectionId",UUID.randomUUID().toString());assertThrows(org.springframework.web.server.ResponseStatusException.class,()->complete(e));
        var invalid=evidence();invalid.put("executionReady",true);assertThrows(org.springframework.web.server.ResponseStatusException.class,()->complete(invalid));
    }
    @Test void foreignMetadataIsRejectedEvenWhenAgentClaimsIdentityVerified()throws Exception {start();var e=evidence();e.put("metadata",List.of(Map.of("path","/data/kafka/broker","clusterId","foreign","nodeId",1,"directoryId","d".repeat(22))));assertThrows(org.springframework.web.server.ResponseStatusException.class,()->complete(e));}
    @Test void failedObservationReturnsNoUnfilteredAgentLog()throws Exception {start();saved.get().setStatus("FAILED");saved.get().setLogOutput("password=do-not-leak");var v=service.get(cluster.getId(),saved.get().getId());assertEquals("BLOCKED",v.status());assertNull(v.evidence());assertFalse(mapper.writeValueAsString(v).contains("do-not-leak"));}
    @Test void externalClusterAndTraversalCannotBeInspected(){cluster.setOriginType("EXTERNAL");assertThrows(org.springframework.web.server.ResponseStatusException.class,this::start);cluster.setOriginType("INTERNAL");member.setConfigJson("{\"service_name\":\"kafka\",\"config_path\":\"/opt/kafka/../server.properties\"}");assertThrows(org.springframework.web.server.ResponseStatusException.class,this::start);verifyNoInteractions(tasks);}
    @Test void genericDeploymentRecoveryCannotConvertInspectionIntoMutation(){var legacy=mock(DeploymentService.class,CALLS_REAL_METHODS);var task=new Task();task.setCommand(AddNodeProcessInspectionService.COMMAND);assertThrows(IllegalArgumentException.class,()->org.springframework.test.util.ReflectionTestUtils.invokeMethod(legacy,"rejectBrokerAdditionTask",task));}
    @Test void existingCombinedRoleAliasIsFrozenAsCanonicalKafkaRoles()throws Exception{member.setRole("broker_controller");start();assertEquals("broker,controller",request().role());}
}
