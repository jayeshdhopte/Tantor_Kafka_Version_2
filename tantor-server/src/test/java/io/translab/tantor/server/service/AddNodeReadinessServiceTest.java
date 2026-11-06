package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.*;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AddNodeReadinessServiceTest {
    final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    final ClusterRepository clusters = mock(ClusterRepository.class);
    final HostRepository hosts = mock(HostRepository.class);
    final TaskRepository tasks = mock(TaskRepository.class);
    final ClusterServiceAssignmentRepository assignments = mock(ClusterServiceAssignmentRepository.class);
    final HostParcelRepository parcels = mock(HostParcelRepository.class);
    final HostStatusService hostStatus = mock(HostStatusService.class);
    final JobService jobs = mock(JobService.class);
    final AddNodeKafkaReadiness kafka = mock(AddNodeKafkaReadiness.class);
    final AddNodeArtifactCatalog catalog = mock(AddNodeArtifactCatalog.class);
    final AddNodeReadinessProperties policy = new AddNodeReadinessProperties();
    final KafkaDeploymentProperties deployment = new KafkaDeploymentProperties();
    final AuditService audit = mock(AuditService.class);
    final AddNodeInspectionProperties inspectionPolicy = new AddNodeInspectionProperties();
    final AddNodeReadinessService service = new AddNodeReadinessService(clusters, hosts, assignments, parcels, tasks, hostStatus, jobs, kafka, catalog, policy, deployment, mapper, audit, inspectionPolicy);
    final UUID id = UUID.randomUUID();
    final String artifactId = UUID.randomUUID().toString();
    final Cluster cluster = new Cluster(); final Host host = new Host();
    Task saved;

    @BeforeEach void setup() {
        cluster.setId(id); cluster.setMode("kraft"); cluster.setStatus("SUCCESS"); cluster.setKafkaClusterId("live-kafka-id"); cluster.setKafkaVersion("4.1.2");
        cluster.setServices(new ArrayList<>()); cluster.setNodeIds(List.of(1, 101));
        var existingBroker=new ClusterServiceAssignment();existingBroker.setRole("broker");existingBroker.setNodeId(1);existingBroker.setHostId("existing-broker");cluster.getServices().add(existingBroker);
        var existingController=new ClusterServiceAssignment();existingController.setRole("controller");existingController.setNodeId(101);existingController.setHostId("existing-controller");cluster.getServices().add(existingController);
        cluster.setConfigJson("{\"listener_port\":9092,\"runtime_user\":\"kafka\"}");
        host.setId("candidate"); host.setHostIp("192.0.2.20"); host.setAgentVersion("test");
        deployment.setSecurityMode("PLAINTEXT");
        when(clusters.findWithServicesById(id)).thenReturn(Optional.of(cluster));
        when(hosts.findById("candidate")).thenReturn(Optional.of(host));
        when(hostStatus.isInfrastructureHost(host)).thenReturn(true); when(hostStatus.isOnline(host)).thenReturn(true);
        when(catalog.resolve(anyString(), anyString())).thenReturn(new AddNodeArtifactCatalog.Artifact(artifactId, "4.1.2", "a".repeat(64)));
        var checks = new ArrayList<Check>();
        for (String code : List.of("C01", "C02", "C03", "C05", "C06", "C07", "C08", "B09")) checks.add(check(code));
        when(kafka.observe(eq(cluster), anyMap())).thenReturn(new AddNodeKafkaReadiness.Observation(checks, Set.of(1, 101), List.of("192.0.2.1:9092"), List.of("192.0.2.1:9093"), "STATIC"));
        when(tasks.saveAndFlush(any())).thenAnswer(call -> { saved = call.getArgument(0); saved.setId(UUID.randomUUID()); return saved; });
        when(tasks.findById(any())).thenAnswer(call -> saved != null && saved.getId().equals(call.getArgument(0)) ? Optional.of(saved) : Optional.empty());
    }
    Check check(String code) { return new Check(code, code, code.startsWith("B") ? "BROKER" : "COMMON", "PASS", "expected", "observed", "", Instant.now(), "AGENT"); }
    void complete() throws Exception {
        Map<String,String> p=mapper.readValue(saved.getParameters(),new TypeReference<>(){});
        var checks = new ArrayList<Check>();
        for (String code : List.of("C09", "C10", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B10", "B11")) checks.add(check(code));
        saved.setStatus("SUCCESS"); saved.setLogOutput(mapper.writeValueAsString(Map.of("schemaVersion", "add-node/v1", "hostId", host.getId(), "contextHash", p.get("readiness_context"), "checks", checks)));
    }
    @Test void persistsOnlyInspectionTaskAndDoesNotAllocateOrDeploy() {
        var result = service.start(id, "candidate");
        assertThat(result.status()).isEqualTo("RUNNING"); assertThat(result.deploymentEnabled()).isFalse();
        assertThat(saved.getCommand()).isEqualTo("INSPECT_ADD_NODE");
        verify(clusters, never()).save(any()); verify(hosts, never()).save(any()); verify(assignments, never()).save(any());
    }
    @Test void fullCorrelatedEvidencePassesAndUnknownEvidenceBlocks() throws Exception {
        service.start(id,"candidate"); complete();
        assertThat(service.get(id,saved.getId()).status()).isEqualTo("PASS");
        saved.setLogOutput(saved.getLogOutput().replaceFirst("\"status\":\"PASS\"", "\"status\":\"UNKNOWN\""));
        assertThat(service.get(id,saved.getId()).status()).isEqualTo("BLOCKED");
    }
    @Test void malformedOldAgentOrWrongContextCannotPass() throws Exception {
        service.start(id,"candidate"); saved.setStatus("FAILED"); saved.setLogOutput("Unknown command: INSPECT_ADD_NODE");
        assertThat(service.get(id,saved.getId()).status()).isEqualTo("BLOCKED");
        complete(); saved.setLogOutput(saved.getLogOutput().replace("add-node/v1","old-contract"));
        assertThat(service.get(id,saved.getId()).checks()).anyMatch(c -> c.code().equals("AGENT_CONTRACT") && c.status().equals("UNKNOWN"));
    }
    @Test void configurationAndHostChangesInvalidateFinishedPreview() throws Exception {
        service.start(id,"candidate"); complete(); cluster.setConfigJson("{\"listener_port\":19092}");
        assertThat(service.get(id,saved.getId()).status()).isEqualTo("STALE");
    }
    @Test void expiryAndClusterBindingAreEnforced() throws Exception {
        service.start(id,"candidate"); complete();
        Map<String,String> p=mapper.readValue(saved.getParameters(),new TypeReference<>(){});
        p.put("readiness_started_at",Instant.now().minusSeconds(300).toString()); saved.setParameters(mapper.writeValueAsString(p));
        assertThat(service.get(id,saved.getId()).status()).isEqualTo("EXPIRED");
        assertThatThrownBy(() -> service.get(UUID.randomUUID(), saved.getId())).isInstanceOf(ResponseStatusException.class);
    }
    @Test void externalClusterAndOfflineHostDoNotDispatch() {
        cluster.setMode("EXTERNAL");
        assertThatThrownBy(() -> service.start(id,"candidate")).isInstanceOf(ResponseStatusException.class);
        cluster.setMode("kraft"); when(hostStatus.isOnline(host)).thenReturn(false);
        assertThatThrownBy(() -> service.start(id,"candidate")).isInstanceOf(ResponseStatusException.class);
        verify(tasks, never()).saveAndFlush(any());
    }
    @Test void approvedArtifactChecksumChangeInvalidatesEvidence() throws Exception {
        service.start(id,"candidate"); complete();
        when(catalog.resolve(anyString(),anyString())).thenReturn(new AddNodeArtifactCatalog.Artifact(artifactId,"4.1.2","b".repeat(64)));
        assertThat(service.get(id,saved.getId()).status()).isEqualTo("STALE");
    }
    @Test void rackAwareClusterRequiresExplicitCandidateRackAndBindsItToContext() throws Exception {
        var original=kafka.observe(cluster,Map.of());
        when(kafka.observe(eq(cluster),anyMap())).thenReturn(new AddNodeKafkaReadiness.Observation(original.checks(),original.nodeIds(),original.brokerEndpoints(),original.controllerEndpoints(),original.quorumMode(),Set.of("rack1"),true));
        var missing=service.start(id,"candidate","");assertThat(missing.checks()).anyMatch(c->c.code().equals("B_RACK")&&c.status().equals("FAIL"));
        var first=service.start(id,"candidate","rack1");assertThat(first.checks()).anyMatch(c->c.code().equals("B_RACK")&&c.status().equals("PASS"));
        var second=service.start(id,"candidate","rack2");assertThat(second.contextHash()).isNotEqualTo(first.contextHash());assertThat(second.brokerPlan()).containsEntry("brokerRack","rack2");
        complete();assertThat(service.get(id,saved.getId()).status()).isEqualTo("PASS");
    }
    @Test void invalidRackCannotScheduleObservationOrMutation() {
        assertThatThrownBy(()->service.start(id,"candidate","rack1\nnode.id=9")).isInstanceOf(ResponseStatusException.class);verify(tasks,never()).saveAndFlush(any());
    }
    @Test void occupiedConnectedBrokerStillReceivesReadOnlyPeerProbe() {
        Host peer=new Host();peer.setId("existing");peer.setHostIp("192.0.2.1");peer.setStatus("OCCUPIED");
        var broker=new ClusterServiceAssignment();broker.setHostId(peer.getId());broker.setRole("broker");cluster.getServices().add(broker);
        when(hosts.findById(peer.getId())).thenReturn(Optional.of(peer));when(hostStatus.isInfrastructureHost(peer)).thenReturn(true);
        when(hostStatus.isOnline(peer)).thenReturn(false);when(hostStatus.agentConnectivityStatus(peer)).thenReturn("ONLINE");
        service.start(id,"candidate");
        verify(tasks).save(argThat(t->"PROBE_ADD_NODE".equals(t.getCommand())&&peer.getId().equals(t.getHostId())));
    }
    @Test void brokerHeapComesFromBrokerServiceAndProfileChangesInvalidateEvidence() throws Exception {
        var broker=new ClusterServiceAssignment();broker.setHostId("existing");broker.setRole("broker");broker.setConfigJson("{\"heap_xmx\":\"1G\"}");cluster.getServices().add(broker);
        var controller=new ClusterServiceAssignment();controller.setHostId("existing");controller.setRole("controller");controller.setConfigJson("{\"heap_xmx\":\"512M\"}");cluster.getServices().add(controller);
        assertThat(service.start(id,"candidate").brokerPlan()).containsEntry("heapXmx","1G");complete();
        broker.setConfigJson("{\"heap_xmx\":\"2G\"}");assertThat(service.get(id,saved.getId()).status()).isEqualTo("STALE");
    }
    @Test void explicitCandidateProfileIsScopedToNewBrokerAndFingerprint() throws Exception {
        policy.setCandidateRuntimeUser("candidate-kafka");policy.setCandidateJavaHome("/usr/lib/jvm/jdk-17");policy.setCandidateLogPath("/data/broker-logs");policy.setCandidateDataPaths(List.of("/opt/broker-storage/data"));
        var preview=service.start(id,"candidate");assertThat(preview.brokerPlan()).containsEntry("runtimeUser","candidate-kafka").containsEntry("javaHome","/usr/lib/jvm/jdk-17").containsEntry("logPath","/data/broker-logs");
        assertThat(preview.brokerPlan()).containsEntry("dataPaths",List.of("/opt/broker-storage/data"));
        assertThat(cluster.getConfigJson()).contains("kafka").doesNotContain("candidate-kafka");verify(clusters,never()).save(any());
        complete();policy.setCandidateLogPath("/data/new-logs");assertThat(service.get(id,saved.getId()).status()).isEqualTo("STALE");
    }
    @Test void ambiguousBrokerHeapsCannotSilentlySelectOne() {
        for(String heap:List.of("1G","2G")){var b=new ClusterServiceAssignment();b.setHostId(heap);b.setRole("broker");b.setConfigJson("{\"heap_xmx\":\""+heap+"\"}");cluster.getServices().add(b);}
        assertThatThrownBy(()->service.start(id,"candidate")).isInstanceOf(ResponseStatusException.class);verify(tasks,never()).saveAndFlush(any());
    }
    @Test void excludedCandidateCannotDispatchOrObserveKafka() {
        inspectionPolicy.setExcludedHostIps(List.of(host.getHostIp()));
        assertThatThrownBy(() -> service.start(id,host.getId())).isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(kafka,tasks);
    }
    @Test void excludedClientProbeBlocksBeforeKafkaOrAnyTask() {
        inspectionPolicy.setExcludedHostIds(List.of("protected-client"));policy.setClientProbeHostIds(List.of("protected-client"));
        assertThatThrownBy(() -> service.start(id,host.getId())).isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(kafka,tasks);
    }}
