package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeReadinessProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** Phase 5 review milestone. No task scheduling, reservation, migration or service mutation. */
@Service @RequiredArgsConstructor
public class ControllerAdditionPlanningService {
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final ClusterServiceAssignmentRepository assignments;
    private final HostStatusService hostStatus;
    private final JobService jobs;
    private final AddNodeKafkaReadiness kafka;
    private final AddNodeArtifactCatalog artifacts;
    private final AddNodeReadinessProperties policy;
    private final ObjectMapper mapper;

    public record Participant(String hostId,String role,Integer nodeId,String serviceName,String configurationPath,
                              String evidenceStatus) {}
    public record Step(String code,String name,String scope,String script) {}
    public record Review(String schemaVersion,UUID clusterId,String hostId,String kafkaClusterId,String kafkaVersion,
                         String quorumMode,Integer proposedControllerId,String artifactId,String artifactChecksum,
                         Instant observedAt,Instant expiresAt,String contextHash,boolean maintenanceRequired,
                         boolean executionEnabled,String status,List<Participant> participants,List<Step> steps,
                         List<Check> checks,List<String> blockers,List<Integer> observedNodeIds,List<String> controllerEndpoints) {}

    public Review review(UUID clusterId,String hostId) {
        var cluster=clusters.findWithServicesById(clusterId).orElseThrow(()->error("Cluster not found."));
        if(!"kraft".equalsIgnoreCase(cluster.getMode())||"EXTERNAL".equalsIgnoreCase(cluster.getOriginType())||!"SUCCESS".equals(cluster.getStatus()))throw error("Controller addition requires an available managed KRaft cluster.");
        var host=hosts.findById(Objects.toString(hostId,"")).orElseThrow(()->error("Select an enrolled candidate host."));
        Map<String,Object> config=readConfig(cluster.getConfigJson());
        var blockers=new ArrayList<String>();
        if(Boolean.TRUE.equals(host.getRemoved())||!hostStatus.isInfrastructureHost(host)||!"ONLINE".equals(hostStatus.agentConnectivityStatus(host)))blockers.add("Candidate must have an online enrolled infrastructure agent.");
        if(host.getClusterId()!=null||assignments.findReservationsForLiveClusters().stream().anyMatch(s->host.getId().equals(s.getHostId())))blockers.add("Controller addition requires a fresh unassigned host. The separate-process flow also requires a fresh host; it does not add a second process to an occupied host.");
        if(jobs.getActiveClusterJobs().stream().anyMatch(j->clusterId.toString().equals(j.get("clusterId"))))blockers.add("Resolve the existing cluster operation or retained reservation before planning maintenance.");
        var observation=kafka.observe(cluster,config);
        String mode=Objects.toString(observation.quorumMode(),"UNKNOWN");
        if(!Set.of("STATIC","DYNAMIC").contains(mode))blockers.add("Quorum mode is unknown; no default controller workflow is selected.");
        Set<String> required=Set.of("C01","C02","C03","C05","C06","C07","C08","B09");
        if(observation.checks().stream().filter(c->required.contains(c.code())&&"PASS".equals(c.status())).map(Check::code).distinct().count()!=required.size())blockers.add("Live cluster identity, registered nodes, package policy, partition health and quorum checks must all pass.");
        var used=new TreeSet<Integer>(observation.nodeIds());
        if(cluster.getNodeIds()!=null)cluster.getNodeIds().stream().filter(Objects::nonNull).forEach(used::add);
        var participants=new ArrayList<Participant>();
        for(var service:cluster.getServices()) {
            if(service.getRole()==null||!(service.getRole().contains("broker")||service.getRole().contains("controller")))continue;
            if(service.getNodeId()!=null)used.add(service.getNodeId());
            var sc=readConfig(service.getConfigJson());
            // Inventory references are not represented as fresh process/file observations.
            participants.add(new Participant(service.getHostId(),service.getRole(),service.getNodeId(),text(sc,"service_name"),text(sc,"config_path"),"RUNTIME_INSPECTION_REQUIRED"));
        }
        participants.sort(Comparator.comparing(Participant::hostId).thenComparing(Participant::role));
        if(participants.stream().noneMatch(p->p.role().contains("controller")))blockers.add("No existing controller assignment is available for a reviewed quorum baseline.");
        int maximum=Math.max(100,used.stream().mapToInt(Integer::intValue).max().orElse(100));
        Integer proposed=maximum==Integer.MAX_VALUE?null:maximum+1;
        if(proposed==null)blockers.add("Controller node ID space is exhausted.");
        String artifactId="",checksum="";
        try{var artifact=artifacts.resolve(cluster.getKafkaVersion(),text(config,"artifact_id"));artifactId=artifact.id();checksum=artifact.checksum();}
        catch(RuntimeException e){blockers.add("A unique approved artifact with the existing Kafka version and SHA-256 is required.");}
        blockers.add("Phase 5 execution is not enabled: obtain fresh candidate/controller readiness and exact runtime service/configuration/metadata observations before accepting an operation.");
        if("STATIC".equals(mode))blockers.add("Static expansion requires a reviewed maintenance window, verified backups and an explicit downtime acknowledgement; it is not the existing rolling-restart operation.");
        else if("DYNAMIC".equals(mode))blockers.add("Dynamic admission requires proven observer catch-up, live directory identity and reconciliation of add-controller outcomes.");
        Instant now=Instant.now();
        var fingerprint=new TreeMap<String,Object>();fingerprint.put("cluster",List.of(clusterId.toString(),Objects.toString(cluster.getKafkaClusterId(),""),Objects.toString(cluster.getKafkaVersion(),""),Objects.toString(cluster.getConfigJson(),"")));
        fingerprint.put("host",List.of(host.getId(),Objects.toString(host.getHostIp(),""),Objects.toString(host.getAgentVersion(),""),Objects.toString(host.getClusterId(),"")));
        fingerprint.put("members",cluster.getServices().stream().map(s->s.getHostId()+":"+s.getRole()+":"+s.getNodeId()+":"+s.getConfigJson()).sorted().toList());
        fingerprint.put("observedNodeIds",used);fingerprint.put("controllerEndpoints",observation.controllerEndpoints());fingerprint.put("quorumMode",mode);fingerprint.put("artifact",artifactId+":"+checksum);fingerprint.put("policy",mapper.valueToTree(policy));
        return new Review("add-controller-review/v1",clusterId,host.getId(),cluster.getKafkaClusterId(),cluster.getKafkaVersion(),mode,proposed,artifactId,checksum,now,now.plusSeconds(policy.getTtlSeconds()),hash(fingerprint),"STATIC".equals(mode),false,"PLANNING_ONLY",List.copyOf(participants),workflow(mode),observation.checks(),List.copyOf(blockers),List.copyOf(used),List.copyOf(observation.controllerEndpoints()));
    }
    static List<Step> workflow(String mode) {
        if("STATIC".equals(mode))return List.of(
            new Step("S0","Prepare candidate, verify backups and stage expanded voters","Candidate and all existing Kafka services","tasks/static_quorum_expand.yml · Phase 0"),
            new Step("S1","Verify leader, zero URP, candidate ports and stop conditions","Entire cluster; abort before downtime on failure","tasks/static_quorum_expand.yml · Phase 1"),
            new Step("S2","Enter maintenance and stop brokers/controllers","All reviewed existing Kafka units, including added brokers","tasks/static_quorum_expand.yml · Phase 2"),
            new Step("S3","Apply and verify identical expanded controller voters","Every affected broker/controller configuration","tasks/static_quorum_expand.yml · Phase 3"),
            new Step("S4","Format only the new controller using the existing cluster ID","Candidate empty controller storage only","tasks/static_quorum_expand.yml · Phase 4"),
            new Step("S5","Start controllers and verify expanded quorum","Existing controllers and candidate controller","tasks/static_quorum_expand.yml · Phase 5"),
            new Step("S6","Start brokers serially and verify URP=0","Every broker stopped by this operation","tasks/static_quorum_expand.yml · Phase 6"),
            new Step("S7","Verify health, run smoke test and close maintenance","Publish membership only after verification","tasks/static_quorum_expand.yml · Phase 7"));
        if("DYNAMIC".equals(mode))return List.of(
            new Step("D0","Verify dynamic quorum and reserve a unique controller ID","Existing quorum read-only; no automatic migration","tasks/common_preview.yml; tasks/role_preview.yml"),
            new Step("D1","Prepare observer bootstrap configuration","Candidate controller.properties; no static voters","tasks/quorum_expand.yml"),
            new Step("D2","Format empty candidate storage with existing cluster ID","Candidate only; --no-initial-controllers","tasks/storage_format.yml"),
            new Step("D3","Start the candidate as observer","Candidate controller service only","tasks/service_deploy.yml"),
            new Step("D4","Verify metadata catch-up and directory identity","Observer must catch up before promotion","tasks/verify_controller.yml"),
            new Step("D5","Admit controller using add-controller","Reconcile actual voters before retrying an uncertain result","tasks/verify_controller.yml"),
            new Step("D6","Verify quorum and publish membership","Existing brokers/controllers are not restarted","tasks/post_health_gate.yml"));
        return List.of();
    }
    private Map<String,Object> readConfig(String value){try{return mapper.readValue(value==null||value.isBlank()?"{}":value,new TypeReference<>(){});}catch(Exception e){throw error("Stored service configuration is unreadable.");}}
    private static String text(Map<String,Object> map,String key){return Objects.toString(map.get(key),"");}
    private String hash(Object value){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsBytes(value)));}catch(Exception e){throw new IllegalStateException("Cannot fingerprint controller review.",e);}}
    private static ResponseStatusException error(String message){return new ResponseStatusException(HttpStatus.CONFLICT,message);}
}
