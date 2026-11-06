package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.Task;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.*;
import static io.translab.tantor.server.service.KafkaExpansionWorkflow.*;

/** Validates native ownership/body evidence before a durable checkpoint can advance. */
@Component @RequiredArgsConstructor
public class KafkaExpansionNativeEvidence {
    private final ObjectMapper mapper;
    public record Observation(JsonNode receipt,JsonNode state,Instant observedAt,String target) {
        public String phase(){return receipt.path("phase").asText();}
        public String metadataDirectoryId(){return state.path("metadataDirectoryId").asText();}
    }
    public void capability(KafkaExpansionPlan.Frozen frozen,Task issued,Task result){
        var proof=tree(result.getLogOutput());var plan=frozen.plan();fresh(proof,issued);var commands=new HashSet<String>();proof.path("supportedCommands").forEach(v->commands.add(v.asText()));
        if(!Objects.equals(issued.getId(),result.getId())||!"SUCCESS".equals(result.getStatus())||!"add-quorum-capability/v1".equals(proof.path("schemaVersion").asText())||!plan.operationId().toString().equals(proof.path("operationId").asText())||!plan.candidateHostId().equals(issued.getHostId())||!issued.getHostId().equals(proof.path("hostId").asText())||!frozen.digest().equals(proof.path("planDigest").asText())||!plan.contextHash().equals(proof.path("contextHash").asText())||!proof.path("candidateReady").asBoolean()||!proof.path("payloadVerified").asBoolean()||!proof.path("rootMutationSupported").asBoolean()||!commands.equals(Set.of(KafkaExpansionLedger.CANDIDATE,KafkaExpansionLedger.PARTICIPANT,KafkaExpansionLedger.VALIDATE)))throw invalid("Candidate readiness/capabilities do not match the frozen operation.");
    }
    public Observation read(KafkaExpansionPlan.Frozen frozen,String target,Task issued,Task result){
        var report=tree(result.getLogOutput());Instant at=fresh(report,issued);var receipt=report.path("receipt");var state=report.path("observation");var plan=frozen.plan();
        if(!Objects.equals(issued.getId(),result.getId())||!"SUCCESS".equals(result.getStatus())||!plan.operationId().toString().equals(receipt.path("operationId").asText())||!frozen.digest().equals(receipt.path("planDigest").asText())||!issued.getHostId().equals(receipt.path("hostId").asText())||!state.isObject())throw invalid("Native receipt belongs to a different operation.");
        if(target.startsWith("candidate:")){
            String role=target.substring("candidate:".length());var process=plan.process(role);
            if(!KafkaExpansionLedger.CANDIDATE.equals(issued.getCommand())||!plan.candidateHostId().equals(issued.getHostId())||!"add-quorum-candidate/v1".equals(receipt.path("schemaVersion").asText())||!role.equals(receipt.path("role").asText())||process.nodeId()!=receipt.path("nodeId").asInt(-1)||!plan.kafkaClusterId().equals(receipt.path("kafkaClusterId").asText())||!plan.unit(role).equals(receipt.path("unit").asText())||!plan.configPath(role).equals(receipt.path("configPath").asText()))throw invalid("Candidate native identity does not match its frozen role.");
            if("MATCHING".equals(state.path("storageState").asText())){
                var expected=new HashSet<>(process.dataPaths());if(process.metadataPath()!=null&&!process.metadataPath().isEmpty())expected.add(process.metadataPath());var paths=new HashSet<String>();var directories=new HashSet<String>();var fields=state.path("directoryIds").fields();while(fields.hasNext()){var field=fields.next();if(!paths.add(field.getKey())||!directory(field.getValue().asText())||!directories.add(field.getValue().asText()))throw invalid("Candidate storage identity proof is ambiguous.");}if(!paths.equals(expected))throw invalid("Candidate storage proof is incomplete.");String metadata=process.metadataPath()==null||process.metadataPath().isEmpty()?process.dataPaths().getFirst():process.metadataPath();if(!state.path("directoryIds").path(metadata).asText().equals(state.path("metadataDirectoryId").asText()))throw invalid("Candidate metadata directory identity differs.");
            }
        }else{
            var member=plan.participants().stream().filter(p->p.key().equals(target)).findFirst().orElseThrow(()->invalid("Unreviewed participant target."));
            if(!KafkaExpansionLedger.PARTICIPANT.equals(issued.getCommand())||!member.hostId().equals(issued.getHostId())||!"add-quorum-participant/v1".equals(receipt.path("schemaVersion").asText())||!member.unit().equals(receipt.path("unit").asText())||member.nodeId()!=receipt.path("nodeId").asInt(-1)||!member.configSha256().equals(receipt.path("originalSha256").asText())||!member.unitSha256().equals(state.path("unitSha256").asText())||!state.path("storageVerified").asBoolean()||state.path("active").asBoolean()&&!state.path("runtimeVerified").asBoolean()||!member.configSha256().equals(state.path("configSha256").asText())&&!receipt.path("expandedSha256").asText().equals(state.path("configSha256").asText()))throw invalid("Participant unit, configuration, storage or runtime identity differs.");
        }
        return new Observation(receipt,state,at,target);
    }
    public Set<String> completed(KafkaExpansionPlan plan,Step step,Observation proof,boolean admitted){
        var state=proof.state();String phase=proof.phase();boolean candidate=step.target().startsWith("candidate:");
        switch(step.kind()){
            case PREPARE->{require(candidate&&Set.of("PREPARED","FORMATTED","STARTED","ADMISSION_SENT").contains(phase)&&state.path("configOwned").asBoolean()&&state.path("unitOwned").asBoolean(),"Candidate preparation is not proven.");if(!"PREPARED".equals(phase))throw invalid("Unexpected later candidate state during preparation.");require("MISSING".equals(state.path("storageState").asText())&&!state.path("serviceActive").asBoolean(),"Prepared candidate storage is not empty.");}
            case FORMAT->{require(candidate&&Set.of("FORMATTED","START_INTENT","STARTED","ADMISSION_INTENT","ADMISSION_SENT").contains(phase)&&state.path("configOwned").asBoolean()&&state.path("unitOwned").asBoolean()&&"MATCHING".equals(state.path("storageState").asText())&&proof.receipt().path("directoryIds").equals(state.path("directoryIds")),"Durable formatted candidate identities are not proven.");}
            case STAGE_VOTERS->{require(!candidate&&Set.of("STAGED","STOP_INTENT","STOPPED","APPLY_INTENT","CONFIG_APPLIED","START_INTENT","STARTED").contains(phase)&&state.path("backupVerified").asBoolean()&&state.path("stageVerified").asBoolean()&&hash(proof.receipt().path("expandedSha256").asText()),"Static participant staging/backups are not proven.");}
            case STOP->{require(!candidate&&"STOPPED".equals(phase)&&state.path("stopped").asBoolean()&&!state.path("active").asBoolean(),"Participant stop is not durably confirmed.");}
            case APPLY_VOTERS->{require(!candidate&&"CONFIG_APPLIED".equals(phase)&&state.path("stopped").asBoolean()&&expanded(proof),"Stopped participant expanded configuration is not proven.");}
            case START->{if(candidate)runningCandidate(proof);else require("STARTED".equals(phase)&&state.path("active").asBoolean()&&state.path("runtimeVerified").asBoolean()&&expanded(proof),"Participant restart/configuration is not proven.");}
            case ADMIT->{runningCandidate(proof);require(admitted,"Controller admission needs independent live voter/observer evidence.");}
            default->throw invalid("This checkpoint needs backend verification.");
        }
        return step.kind().requiredFacts();
    }
    public void runningCandidate(Observation proof){require(Set.of("STARTED","ADMISSION_INTENT","ADMISSION_SENT").contains(proof.phase())&&proof.state().path("configOwned").asBoolean()&&proof.state().path("unitOwned").asBoolean()&&"MATCHING".equals(proof.state().path("storageState").asText())&&proof.state().path("serviceActive").asBoolean()&&proof.state().path("runtimeVerified").asBoolean()&&proof.receipt().path("directoryIds").equals(proof.state().path("directoryIds")),"Operation-owned running candidate is not durably proven.");}
    public boolean expanded(Observation proof){return proof.state().path("backupVerified").asBoolean()&&proof.state().path("stageVerified").asBoolean()&&hash(proof.receipt().path("expandedSha256").asText())&&proof.receipt().path("expandedSha256").asText().equals(proof.state().path("configSha256").asText());}
    private Instant fresh(JsonNode report,Task task){try{Instant now=Instant.now(),at=Instant.parse(report.path("observedAt").asText());var params=tree(task.getParameters());Instant expires=Instant.parse(params.path("dispatch_expires_at").asText());if(at.isBefore(now.minusSeconds(60))||at.isAfter(now.plusSeconds(5))||at.isAfter(expires.plusSeconds(5)))throw invalid("Native observation expired.");return at;}catch(Exception e){throw invalid("Fresh bounded native observation is required.");}}
    private JsonNode tree(String raw){try{if(raw==null||raw.length()>2*1024*1024)throw invalid("Missing or oversized native evidence.");return mapper.readTree(raw);}catch(Exception e){throw invalid("Malformed native evidence.");}}
    private static boolean directory(String value){return value.matches("[A-Za-z0-9_-]{22}")&&!"AAAAAAAAAAAAAAAAAAAAAA".equals(value);}
    private static boolean hash(String value){return value.matches("[a-f0-9]{64}");}
    private static void require(boolean value,String reason){if(!value)throw invalid(reason);}
    private static IllegalStateException invalid(String reason){return new IllegalStateException(reason);}
}
