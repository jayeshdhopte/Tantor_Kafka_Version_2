package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.util.*;

/** Resolves server-owned observation references; never accepts a user-supplied native plan. */
@Service @RequiredArgsConstructor
public class KafkaExpansionPlanCollector {
    private final ControllerAdditionPlanningService controllers;
    private final AddNodeReadinessService readiness;
    private final KafkaExpansionRuntimeInspection runtime;
    private final KafkaExpansionQuorum quorum;
    private final BrokerAdditionKafka brokerKafka;
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final TaskRepository tasks;
    private final AddNodeReadinessProperties commonPolicy;
    private final KafkaExpansionProperties policy;
    private final KafkaDeploymentProperties deployment;
    private final ArtifactRepositoryProperties artifactPolicy;
    private final AddNodeInspectionProperties scopePolicy;
    private final AddNodeArtifactCatalog catalog;
    private final ObjectMapper mapper;
    public record Request(String hostId,KafkaExpansionWorkflow.Layout layout,UUID commonPreviewId,List<UUID> runtimeTaskIds,String brokerRack,boolean maintenanceAcknowledged) {
        public Request {runtimeTaskIds=runtimeTaskIds==null?List.of():List.copyOf(runtimeTaskIds);brokerRack=Objects.toString(brokerRack,"");}
    }
    public record Collected(KafkaExpansionPlan.Frozen frozen,Instant expiresAt) {}
    public Collected collect(UUID clusterId,UUID operationId,Request input){
        if(input.hostId()==null||input.layout()==null||input.commonPreviewId()==null||input.runtimeTaskIds().isEmpty()||input.runtimeTaskIds().size()>256||new HashSet<>(input.runtimeTaskIds()).size()!=input.runtimeTaskIds().size())throw conflict("Common checks and the complete set of approved participant runtime observations are required.");
        var review=controllers.review(clusterId,input.hostId());var cluster=clusters.findWithServicesById(clusterId).orElseThrow();var candidate=hosts.findById(input.hostId()).orElseThrow();
        if(!KafkaExpansionRuntimeInspection.supported(candidate.getAgentVersion()))throw conflict("A compatible addnode.7 or newer candidate agent is required.");
        if(!Set.of("STATIC","DYNAMIC").contains(review.quorumMode())||review.proposedControllerId()==null||review.artifactId().isBlank()||review.artifactChecksum().isBlank())throw conflict("Known live quorum mode, proposed identity and approved artifact are required.");
        if("STATIC".equals(review.quorumMode())&&!input.maintenanceAcknowledged())throw conflict("Review and explicitly acknowledge broker/controller downtime for static expansion.");
        var common=readiness.get(clusterId,input.commonPreviewId());if(!input.hostId().equals(common.hostId())||!common.expiresAt().isAfter(Instant.now())||Set.of("PENDING","RUNNING","STALE","EXPIRED").contains(common.status()))throw conflict("Common host checks are incomplete or stale.");
        var required=new HashSet<String>();for(int i=1;i<=21;i++)required.add(String.format("C%02d",i));var passed=new HashSet<String>();
        for(var check:common.checks())if(required.contains(check.code())){if(!"COMMON".equals(check.phase())||!"PASS".equals(check.status())||!passed.add(check.code()))throw conflict("Common readiness evidence is failed, duplicated or assigned to a different phase.");}
        if(!passed.containsAll(required))throw conflict("All common host, cluster, identity, resource and connectivity checks must pass.");
        if(input.layout()==KafkaExpansionWorkflow.Layout.SEPARATE&&!"PASS".equals(common.status()))throw conflict("Both common and broker-specific checks must pass for the separate-process layout.");
        var commonTask=tasks.findById(input.commonPreviewId()).filter(t->clusterId.equals(t.getClusterId())&&input.hostId().equals(t.getHostId())&&AddNodeReadinessService.COMMAND.equals(t.getCommand())).orElseThrow(()->conflict("Common observation belongs to another scope."));
        var parameters=tree(commonTask.getParameters());var profile=tree(parameters.path("readiness_request").asText());if(!input.brokerRack().equals(profile.path("brokerRack").asText()))throw conflict("Broker rack changed after common checks.");
        AddNodeDeploymentSettings settings=profile.hasNonNull("deploymentSettings")?mapper.convertValue(profile.get("deploymentSettings"),AddNodeDeploymentSettings.class):null;
        if(settings!=null&&!KafkaExpansionRuntimeInspection.supported(candidate.getAgentVersion(),9))throw conflict("Custom deployment layouts require internal agent addnode.9 or newer.");
        boolean systemJava=profile.path("javaHome").asText().startsWith("/usr/lib/jvm/");if(systemJava&&!KafkaExpansionRuntimeInspection.supported(candidate.getAgentVersion(),8))throw conflict("The configured system JDK requires addnode.8 or newer on the candidate and every participant.");
        var byService=new HashMap<UUID,KafkaExpansionRuntimeInspection.View>();Instant expires=common.expiresAt();for(UUID id:input.runtimeTaskIds()){var observation=runtime.get(clusterId,id);if(!"OBSERVED".equals(observation.status())||observation.evidence()==null||!observation.expiresAt().isAfter(Instant.now())||byService.putIfAbsent(observation.serviceId(),observation)!=null)throw conflict("Every participant needs one fresh matching approved runtime observation.");if(observation.expiresAt().isBefore(expires))expires=observation.expiresAt();}
        var expected=review.participants().stream().map(ControllerAdditionPlanningService.Participant::serviceId).collect(java.util.stream.Collectors.toSet());if(!expected.equals(byService.keySet()))throw conflict("Runtime evidence does not cover the complete current service inventory.");
        var grouped=new TreeMap<String,List<KafkaExpansionRuntimeInspection.View>>();for(var view:byService.values()){var e=view.evidence().identity();grouped.computeIfAbsent(e.hostId()+":"+e.serviceName(),ignored->new ArrayList<>()).add(view);}
        var participants=new ArrayList<KafkaExpansionPlan.Participant>();
        for(var group:grouped.values()){
            var first=group.getFirst();var report=first.evidence();var identity=report.identity();for(var other:group){var e=other.evidence().identity();if(e.nodeId()!=identity.nodeId()||e.pid()!=identity.pid()||!e.processStartTicks().equals(identity.processStartTicks())||!e.configPath().equals(identity.configPath())||!e.configSha256().equals(identity.configSha256())||!e.unitSha256().equals(identity.unitSha256())||!other.evidence().runtime().proofSha256().equals(report.runtime().proofSha256())||!e.metadata().equals(identity.metadata()))throw conflict("One service unit has inconsistent process/storage observations.");}
            if(!review.artifactChecksum().equals(report.runtime().archiveSha256())||!review.kafkaVersion().equals(report.runtime().kafkaVersion()))throw conflict("Participant runtime package differs from the approved inherited Kafka version/artifact.");
            var host=hosts.findById(identity.hostId()).orElseThrow();if(settings!=null&&!KafkaExpansionRuntimeInspection.supported(host.getAgentVersion(),9))throw conflict("Custom deployment layouts require addnode.9 or newer on participant "+host.getId()+".");if(systemJava&&!KafkaExpansionRuntimeInspection.supported(host.getAgentVersion(),8))throw conflict("The configured system JDK requires addnode.8 or newer on participant "+host.getId()+".");if(scopePolicy.excludes(host.getId(),host.getHostIp()))throw conflict("A participant is outside the permitted scope.");
            participants.add(new KafkaExpansionPlan.Participant(group.stream().map(KafkaExpansionRuntimeInspection.View::serviceId).sorted().toList(),identity.hostId(),host.getHostIp(),identity.role(),identity.nodeId(),identity.serviceName(),identity.configPath(),identity.configSha256(),identity.unitSha256(),report.runtime().proofSha256(),report.runtime().runtimeHome(),report.javaAgents(),report.resolvedPaths(),identity.metadata().stream().map(m->new KafkaExpansionPlan.Storage(m.path(),m.directoryId())).toList(),identity.pid(),identity.processStartTicks(),report.controllerPort()));
        }
        var live=quorum.observe(review.controllerEndpoints());if(!review.kafkaClusterId().equals(live.clusterId()))throw conflict("Live Kafka identity changed.");boolean dynamic="DYNAMIC".equals(review.quorumMode());if(dynamic!=(live.features().getOrDefault("kraft.version",(short)0)>=1))throw conflict("Finalized quorum mode changed.");
        var voters=new ArrayList<KafkaExpansionPlan.Voter>();for(var voter:live.voters().values()){var member=participants.stream().filter(p->p.nodeId()==voter.id()&&p.role().contains("controller")).findFirst().orElseThrow(()->conflict("A live voter has no matching native controller evidence."));voters.add(new KafkaExpansionPlan.Voter(voter.id(),voter.directoryId(),member.hostIp(),member.controllerPort()));}voters.sort(Comparator.comparingInt(KafkaExpansionPlan.Voter::nodeId));
        var ports=voters.stream().map(KafkaExpansionPlan.Voter::port).collect(java.util.stream.Collectors.toSet());if(ports.size()!=1)throw conflict("An explicit adapter is required for a nonuniform controller listener layout.");int controllerPort=ports.iterator().next();
        var controllerProperties=consistentInherited(byService.values(),"controller");var processes=new ArrayList<KafkaExpansionPlan.Process>();processes.add(new KafkaExpansionPlan.Process("controller",review.proposedControllerId(),controllerPort,settings==null?policy.getControllerDataPaths():settings.controllerDataPaths(),settings==null?policy.getControllerMetadataPath():settings.controllerMetadataPath(),settings==null?policy.getControllerLogPath():settings.controllerLogPath(),settings==null?policy.getControllerHeapXmx():settings.controllerHeapXmx(),"",controllerProperties,jmx(policy.getControllerJmxPort())));
        if(input.layout()==KafkaExpansionWorkflow.Layout.SEPARATE){var allocation=AddNodeIdentityAllocator.broker(cluster,new HashSet<>(review.observedNodeIds()));if(!allocation.available()||allocation.nodeId()==review.proposedControllerId())throw conflict("The sequential broker/controller identity proposals collide or are unavailable.");String originalVoters=voters.stream().map(v->v.nodeId()+"@"+endpoint(v.hostIp(),v.port())).collect(java.util.stream.Collectors.joining(","));var broker=BrokerAdditionPlan.freeze(common,operationId,allocation.nodeId(),originalVoters);var inherited=properties(brokerKafka.properties(cluster,broker));processes.add(new KafkaExpansionPlan.Process("broker",allocation.nodeId(),broker.brokerPort(),broker.dataPaths(),broker.metadataPath(),broker.logPath(),broker.heapXmx(),input.brokerRack(),inherited,jmx(policy.getBrokerJmxPort())));}
        var context=new TreeMap<String,Object>();context.put("controllerContext",review.contextHash());context.put("commonContext",common.contextHash());context.put("runtimeTasks",input.runtimeTaskIds().stream().sorted().toList());context.put("participants",participants);context.put("processes",processes);context.put("voters",voters);context.put("features",new TreeMap<>(live.features()));context.put("profile",mapper.valueToTree(policy));context.put("acknowledged",input.maintenanceAcknowledged());String contextHash=BrokerAdditionLedger.hash(json(context));
        var plan=new KafkaExpansionPlan(settings==null?"add-quorum-expansion/v1":"add-quorum-expansion/v2",operationId,clusterId,contextHash,input.hostId(),candidate.getHostIp(),review.kafkaClusterId(),review.kafkaVersion(),KafkaExpansionWorkflow.Mode.valueOf(review.quorumMode()),input.layout(),live.features(),UUID.fromString(review.artifactId()),review.artifactChecksum(),profile.path("parcelDir").asText(),profile.path("javaHome").asText(),profile.path("runtimeUser").asText(),processes,participants,voters,scopePolicy.getExcludedHostIds(),scopePolicy.getExcludedHostIps(),input.maintenanceAcknowledged(),new KafkaExpansionPlan.ResourcePolicy(commonPolicy.getMinAvailableMemoryMb(),policy.getOsReserveMb(),commonPolicy.getMinDiskGb(),commonPolicy.getMinInodes(),commonPolicy.getAllowedJavaMajors()),settings);
        return new Collected(plan.freeze(mapper),expires);
    }
    private KafkaExpansionPlan.Runtime jmx(int port){if(port==0)return new KafkaExpansionPlan.Runtime(null,"",0,deployment.getLimitNofile());var artifact=catalog.resolveType("JMX_EXPORTER",null,artifactPolicy.getJmxExporterArtifactId());return new KafkaExpansionPlan.Runtime(UUID.fromString(artifact.id()),artifact.checksum(),port,deployment.getLimitNofile());}
    private Map<String,String> consistentInherited(Collection<KafkaExpansionRuntimeInspection.View> views,String role){Map<String,String> result=null;for(var view:views)if(view.evidence().identity().role().contains(role)){var values=view.evidence().inherited();if(result!=null&&!result.equals(values))throw conflict("Existing "+role+" processes have different custom configurations; resolve the inheritance policy first.");result=values;}if(result==null)throw conflict("No inherited "+role+" configuration was observed.");return Map.copyOf(result);}
    private Map<String,String> properties(String text){var result=new TreeMap<String,String>();var replaced=Set.of("broker.id","node.id","process.roles","listeners","advertised.listeners","log.dir","log.dirs","metadata.log.dir","controller.quorum.voters","controller.quorum.bootstrap.servers","controller.quorum.auto.join.enable","broker.rack","listener.security.protocol.map","inter.broker.listener.name","controller.listener.names");for(String line:text.split("\n")){int equal=line.indexOf('=');if(equal>0&&!replaced.contains(line.substring(0,equal)))result.put(line.substring(0,equal),line.substring(equal+1));}return result;}
    private JsonNode tree(String raw){try{return mapper.readTree(raw);}catch(Exception e){throw conflict("Stored readiness inputs are incomplete.");}}
    private String json(Object value){try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("Cannot bind expansion context.",e);}}
    private static String endpoint(String host,int port){return (host.contains(":")?"["+host+"]":host)+":"+port;}
    private static ResponseStatusException conflict(String message){return new ResponseStatusException(HttpStatus.CONFLICT,message);}
}
