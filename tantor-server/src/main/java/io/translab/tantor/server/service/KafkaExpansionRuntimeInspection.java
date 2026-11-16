package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.util.*;

/** Catalog-bound, read-only evidence required before a controller plan can freeze. */
@Service @RequiredArgsConstructor
public class KafkaExpansionRuntimeInspection {
    public static final String COMMAND="INSPECT_QUORUM_RUNTIME";
    private final AddNodeProcessInspectionService identity;
    private final HostParcelRepository parcels;
    private final TaskRepository tasks;
    private final AddNodeArtifactCatalog catalog;
    private final ArtifactRepositoryProperties artifactPolicy;
    private final ObjectMapper mapper;
    public record Request(String schemaVersion,AddNodeProcessInspectionService.Request identity,String parcelDir,String runtimeHome,String artifactSha256,List<String> approvedAgentChecksums) {}
    public record RuntimeProof(String schemaVersion,String archiveSha256,String kafkaVersion,String runtimeHome,String classpathDigest,String proofSha256,int libraryCount) {}
    public record Report(String schemaVersion,AddNodeProcessInspectionService.Evidence identity,RuntimeProof runtime,Map<String,String> javaAgents,Map<String,String> resolvedPaths,@com.fasterxml.jackson.annotation.JsonProperty(access=com.fasterxml.jackson.annotation.JsonProperty.Access.WRITE_ONLY) Map<String,String> inherited,int controllerPort,Instant observedAt) {}
    public record View(UUID taskId,UUID serviceId,String hostId,String status,Instant expiresAt,Report evidence,boolean executionReady,String message) {}

    @Transactional
    public View start(UUID clusterId,UUID serviceId){
        var scope=identity.runtimeScope(clusterId,serviceId);if(!supported(scope.host().getAgentVersion()))throw conflict("Install the tested addnode.7 or newer internal agent before artifact/runtime inspection.");
        Map<String,Object> config=configuration(scope.cluster().getConfigJson());var artifact=catalog.resolve(scope.cluster().getKafkaVersion(),Objects.toString(config.get("artifact_id"),""));
        var parcel=parcels.findFirstByHostIdAndArtifactIdOrderByCreatedAtDescIdDesc(scope.host().getId(),UUID.fromString(artifact.id())).orElseThrow(()->conflict("Distribute the approved matching Kafka parcel to this participant first."));
        if(parcel.getParcelDir()==null||!parcel.getParcelDir().matches("/(opt|var|data|srv|app)/[A-Za-z0-9_./-]+")||parcel.getParcelDir().contains("..")||parcel.getParcelDir().endsWith("/"))throw conflict("Recorded parcel location is incomplete.");
        var agents=new ArrayList<String>();if(scope.service().getJmxExporterPort()!=null&&scope.service().getJmxExporterPort()>0)agents.add(catalog.resolveType("JMX_EXPORTER",null,artifactPolicy.getJmxExporterArtifactId()).checksum());
        var request=new Request("add-node-runtime-inspection/v1",scope.identity(),parcel.getParcelDir(),"",artifact.checksum(),List.copyOf(agents));
        var task=new Task();task.setHostId(scope.host().getId());task.setClusterId(clusterId);task.setCommand(COMMAND);task.setStatus("PENDING");task.setParameters(json(Map.of("runtime_inspection_request",json(request),"service_id",serviceId.toString())));task=tasks.saveAndFlush(task);
        return view(task,serviceId,request,"RUNNING",null,"Inspecting actual JVM libraries against the approved Kafka artifact; no service changes.");
    }
    public View get(UUID clusterId,UUID taskId){
        var task=tasks.findById(taskId).filter(t->clusterId.equals(t.getClusterId())&&COMMAND.equals(t.getCommand())).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Runtime inspection not found for this cluster."));
        try{
            Map<String,String> parameters=mapper.readValue(task.getParameters(),new TypeReference<>(){});UUID serviceId=UUID.fromString(parameters.get("service_id"));var request=mapper.readValue(parameters.get("runtime_inspection_request"),Request.class);var scope=identity.runtimeScope(clusterId,serviceId);
            if(!Objects.equals(task.getHostId(),request.identity().hostId())||!Objects.equals(scope.identity().contextHash(),request.identity().contextHash()))return view(task,serviceId,request,"STALE",null,"Recorded participant context changed. Request fresh evidence.");
            if(!request.identity().expiresAt().isAfter(Instant.now()))return view(task,serviceId,request,"EXPIRED",null,"Runtime evidence expired.");
            if(Set.of("PENDING","IN_PROGRESS","RUNNING").contains(task.getStatus()))return view(task,serviceId,request,"RUNNING",null,"Waiting for the enrolled internal agent.");
            if(!"SUCCESS".equals(task.getStatus())||task.getLogOutput()==null||task.getLogOutput().length()>262144)return view(task,serviceId,request,"BLOCKED",null,"Actual approved runtime could not be verified. Existing services were not changed.");
            var report=mapper.readValue(task.getLogOutput(),Report.class);validate(request,report);return view(task,serviceId,request,"OBSERVED",report,"Process identity and approved Kafka runtime verified. Candidate readiness and live quorum gates remain separate.");
        }catch(ResponseStatusException e){throw e;}catch(Exception e){throw conflict("Invalid or incomplete approved runtime evidence. Request a fresh inspection.");}
    }
    void validate(Request request,Report report){
        identity.validate(report.identity(),request.identity());var proof=report.runtime();Instant now=Instant.now();
        if(!request.schemaVersion().equals(report.schemaVersion())||!report.identity().identityVerified()||proof==null||!"add-node-runtime/v1".equals(proof.schemaVersion())||!request.artifactSha256().equals(proof.archiveSha256())||!runtimeHome(proof.runtimeHome())||!request.runtimeHome().isEmpty()&&!request.runtimeHome().equals(proof.runtimeHome())||!request.identity().kafkaVersion().equals(proof.kafkaVersion())||!hash(proof.classpathDigest())||!hash(proof.proofSha256())||proof.libraryCount()<2||proof.libraryCount()>4096||report.observedAt()==null||report.observedAt().isBefore(request.identity().startedAt().minusSeconds(5))||report.observedAt().isAfter(now.plusSeconds(5))||report.observedAt().isAfter(request.identity().expiresAt())||report.javaAgents()==null||report.javaAgents().size()>8||report.resolvedPaths()==null||report.resolvedPaths().size()>32||report.inherited()==null||report.inherited().size()>4096||report.identity().role().contains("controller")&&(report.controllerPort()<1||report.controllerPort()>65535))throw conflict("Runtime observation does not match its frozen request.");
        for(var agent:report.javaAgents().entrySet())if(!request.approvedAgentChecksums().contains(agent.getValue())||!hash(agent.getValue())||!agent.getKey().matches("/(opt|var|data|srv|app)/[A-Za-z0-9_./-]+")||agent.getKey().contains(".."))throw conflict("Runtime Java agent is not approved.");
        for(var entry:report.inherited().entrySet())if(entry.getKey()==null||!entry.getKey().matches("[A-Za-z][A-Za-z0-9._-]{0,127}")||entry.getValue()==null||entry.getValue().length()>16384||entry.getValue().contains("\n")||entry.getValue().contains("\r")||entry.getValue().contains("\0"))throw conflict("Unsupported inherited runtime property.");
    }
    private View view(Task task,UUID service,Request r,String status,Report report,String message){return new View(task.getId(),service,task.getHostId(),status,r.identity().expiresAt(),report,false,message);}
    static boolean supported(String version){return supported(version,7);}
    static boolean supported(String version,int minimum){if(version==null)return false;var match=java.util.regex.Pattern.compile(".*-addnode\\.([0-9]+)$").matcher(version);try{return match.matches()&&Integer.parseInt(match.group(1))>=minimum;}catch(Exception e){return false;}}
    private Map<String,Object> configuration(String raw){try{return mapper.readValue(Objects.toString(raw,"{}"),new TypeReference<>(){});}catch(Exception e){throw conflict("Stored cluster configuration is unreadable.");}}
    private String json(Object value){try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("Cannot encode approved runtime inspection.",e);}}
    private static boolean hash(String value){return value!=null&&value.matches("[a-f0-9]{64}");}
    private static boolean runtimeHome(String path){return path!=null&&path.matches("/(opt|var|data|srv|app)/[A-Za-z0-9_./-]+")&&!path.endsWith("/")&&java.nio.file.Path.of(path).normalize().toString().replace('\\','/').equals(path);}
    private static ResponseStatusException conflict(String reason){return new ResponseStatusException(HttpStatus.CONFLICT,reason);}
}
