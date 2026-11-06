package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.Task;
import io.translab.tantor.server.repository.TaskRepository;
import io.translab.tantor.server.config.KafkaExpansionProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.util.*;

/** Leased read-only candidate preflight; creates no deployment job or inventory reservation. */
@Service @RequiredArgsConstructor
public class KafkaExpansionPreviewService {
    private final KafkaExpansionPlanCollector collector;
    private final TaskRepository tasks;
    private final KafkaExpansionProperties policy;
    private final ObjectMapper mapper;
    public record Review(String schemaVersion,UUID previewId,UUID clusterId,String hostId,String contextHash,String status,
                         Instant expiresAt,boolean executionEnabled,String message,KafkaExpansionPlan proposedPlan) {}
    public Review start(UUID clusterId,KafkaExpansionPlanCollector.Request request){
        var collected=collector.collect(clusterId,UUID.randomUUID(),request);var frozen=collected.frozen();Instant now=Instant.now();
        if(!collected.expiresAt().isAfter(now.plusSeconds(5)))throw conflict("The readiness observations are about to expire. Refresh them first.");
        var parameters=new LinkedHashMap<String,String>();parameters.put("expansion_plan",frozen.raw());parameters.put("plan_digest",frozen.digest());parameters.put("action","VALIDATE");parameters.put("dispatch_expires_at",minimum(now.plusSeconds(120),collected.expiresAt()).toString());parameters.put("preview_expires_at",collected.expiresAt().toString());parameters.put("preview_started_at",now.toString());parameters.put("preview_inputs",json(request));
        var task=new Task();task.setClusterId(clusterId);task.setHostId(request.hostId());task.setCommand(KafkaExpansionLedger.VALIDATE);task.setStatus("PENDING");task.setParameters(json(parameters));task=tasks.saveAndFlush(task);
        return view(task,frozen,"RUNNING",collected.expiresAt(),"Checking both the selected process layout and the approved candidate runtime; no deployment changes.");
    }
    public Review review(UUID clusterId,UUID previewId){
        var task=task(clusterId,previewId);Map<String,String> params=parameters(task);var frozen=KafkaExpansionPlan.read(mapper,params.get("expansion_plan"),params.get("plan_digest"));Instant expires=parse(params.get("preview_expires_at"));
        if(!task.getHostId().equals(frozen.plan().candidateHostId())||!clusterId.equals(frozen.plan().clusterId()))throw conflict("Preflight belongs to a different expansion scope.");
        if(!expires.isAfter(Instant.now()))return view(task,frozen,"EXPIRED",expires,"Refresh common and participant checks before deployment.");
        if(Set.of("PENDING","IN_PROGRESS","RUNNING").contains(task.getStatus()))return view(task,frozen,"RUNNING",expires,"Waiting for candidate layout and runtime verification.");
        if(!"SUCCESS".equals(task.getStatus()))return view(task,frozen,"BLOCKED",expires,"Candidate preflight failed. No deployment changes were made; inspect its task evidence and rerun checks.");
        validateCapability(task,frozen,params,expires);
        var current=collector.collect(clusterId,frozen.plan().operationId(),input(params));if(!current.frozen().plan().contextHash().equals(frozen.plan().contextHash()))return view(task,frozen,"STALE",expires,"Cluster, host, profile, artifact or participant context changed. Run fresh checks.");
        return view(task,frozen,"PASS",expires,policy.isExecutionEnabled()?"Common checks, approved participant runtimes, live quorum baseline and candidate layout verified.":"Readiness passed. Execution remains held until collective release acceptance passes.");
    }
    public KafkaExpansionPlan.Frozen requireReady(UUID clusterId,UUID previewId,String contextHash){
        var review=review(clusterId,previewId);if(!"PASS".equals(review.status())||!review.executionEnabled()||!Objects.equals(contextHash,review.contextHash()))throw conflict("A fresh successful released expansion review is required.");var params=parameters(task(clusterId,previewId));return KafkaExpansionPlan.read(mapper,params.get("expansion_plan"),params.get("plan_digest"));
    }
    private void validateCapability(Task task,KafkaExpansionPlan.Frozen frozen,Map<String,String> params,Instant expires){
        try{if(task.getLogOutput()==null||task.getLogOutput().length()>32768)throw conflict("Bounded candidate capability evidence required.");var proof=mapper.readTree(task.getLogOutput());Instant observed=parse(proof.path("observedAt").asText());var commands=new HashSet<String>();proof.path("supportedCommands").forEach(c->commands.add(c.asText()));
            if(!"add-quorum-capability/v1".equals(proof.path("schemaVersion").asText())||!frozen.plan().operationId().toString().equals(proof.path("operationId").asText())||!task.getHostId().equals(proof.path("hostId").asText())||!frozen.digest().equals(proof.path("planDigest").asText())||!frozen.plan().contextHash().equals(proof.path("contextHash").asText())||!proof.path("payloadVerified").asBoolean()||!proof.path("candidateReady").asBoolean()||!proof.path("rootMutationSupported").asBoolean()||!commands.equals(Set.of(KafkaExpansionLedger.CANDIDATE,KafkaExpansionLedger.PARTICIPANT,KafkaExpansionLedger.VALIDATE))||observed.isBefore(parse(params.get("preview_started_at")).minusSeconds(5))||observed.isAfter(Instant.now().plusSeconds(5))||observed.isAfter(expires))throw conflict("Candidate capability evidence does not match the reviewed plan.");
        }catch(ResponseStatusException e){throw e;}catch(Exception e){throw conflict("Incomplete candidate capability evidence.");}
    }
    private Task task(UUID clusterId,UUID previewId){return tasks.findById(previewId).filter(t->clusterId.equals(t.getClusterId())&&KafkaExpansionLedger.VALIDATE.equals(t.getCommand())).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Expansion preflight not found for this cluster."));}
    private Review view(Task task,KafkaExpansionPlan.Frozen frozen,String status,Instant expires,String message){return new Review("add-quorum-review/v1",task.getId(),frozen.plan().clusterId(),task.getHostId(),frozen.plan().contextHash(),status,expires,"PASS".equals(status)&&policy.isExecutionEnabled(),message,frozen.plan());}
    private Map<String,String> parameters(Task task){try{return mapper.readValue(task.getParameters(),new TypeReference<>(){});}catch(Exception e){throw conflict("Stored expansion preflight is incomplete.");}}
    private KafkaExpansionPlanCollector.Request input(Map<String,String> params){try{return mapper.readValue(params.get("preview_inputs"),KafkaExpansionPlanCollector.Request.class);}catch(Exception e){throw conflict("Stored expansion inputs are incomplete.");}}
    private String json(Object value){try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("Cannot persist expansion preflight.",e);}}
    private static Instant minimum(Instant a,Instant b){return a.isBefore(b)?a:b;}
    private static Instant parse(String value){try{return Instant.parse(value);}catch(Exception e){throw conflict("Expansion evidence timestamp is invalid.");}}
    private static ResponseStatusException conflict(String message){return new ResponseStatusException(HttpStatus.CONFLICT,message);}
}
