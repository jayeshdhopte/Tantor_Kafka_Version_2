package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.KafkaExpansionProperties;
import io.translab.tantor.server.domain.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.*;
import static io.translab.tantor.server.service.KafkaExpansionWorkflow.*;

/** Executes only the ordered frozen workflow; resumes its own receipts/history. */
@Component @RequiredArgsConstructor
public class KafkaExpansionJobHandler implements JobHandler {
    private final KafkaExpansionLedger ledger;
    private final JobService jobs;
    private final AgentTaskAwaiter awaiter;
    private final KafkaExpansionNativeEvidence nativeEvidence;
    private final KafkaExpansionLiveVerifier live;
    private final KafkaExpansionProperties policy;
    private final ObjectMapper mapper;
    @Override public boolean supports(JobType type){return type==JobType.QUORUM_EXPANSION;}
    @Override public void execute(Job job){
        var op=ledger.forJob(job.getId());if(op.getState()==KafkaExpansionState.SUCCEEDED){for(var step:jobs.getSteps(job.getId()))if(step.getStatus()!=JobStepStatus.SUCCESS)jobs.completeStep(step.getId(),"Recovered committed publication; no native action was repeated.");return;}
        var frozen=ledger.frozen(op);var workflow=frozen.steps();var jobSteps=jobs.getSteps(job.getId());if(jobSteps.size()!=workflow.size())throw new IllegalStateException("Expansion job does not match its frozen workflow.");
        try{
            for(int i=0;i<workflow.size();i++){
                var action=workflow.get(i);var step=jobSteps.get(i);var current=ledger.forJob(job.getId());var checkpoints=ledger.checkpoints(current);
                if(checkpoints.snapshot().get(action.id())==State.CONFIRMED){if(step.getStatus()!=JobStepStatus.SUCCESS)jobs.completeStep(step.getId(),"Recovered durable checkpoint; no mutation was repeated.");continue;}
                if(!checkpoints.next().orElseThrow().id().equals(action.id()))throw new IllegalStateException("Expansion checkpoint order changed.");jobs.startStep(step.getId());drain(step);
                if(action.kind().mutation()&&action.kind()!=Kind.PUBLISH)mutation(current,frozen,action,step,checkpoints.snapshot().get(action.id())==State.INTENT);
                else server(current,frozen,action,step);
                jobs.completeStep(step.getId(),"Verified "+action.kind()+" for "+action.target()+"; durable evidence recorded.");
            }
        }catch(Exception failure){ledger.recovery(op.getId(),failure.getMessage());jobs.getSteps(job.getId()).stream().filter(s->s.getStatus()==JobStepStatus.IN_PROGRESS).findFirst().ifPresent(s->jobs.failStep(s.getId(),failure.getMessage()));throw new IllegalStateException("Expansion requires reconciliation; all parent reservations are retained. "+failure.getMessage(),failure);}
    }
    private void mutation(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,Step action,JobStep step,boolean uncertain)throws Exception{
        KafkaExpansionNativeEvidence.Observation previous=null;
        if(uncertain){previous=inspect(op,frozen,step,action.target());if(action.kind()!=Kind.ADMIT){try{var facts=nativeEvidence.completed(frozen.plan(),action,previous,false);confirm(op,action,facts);return;}catch(IllegalStateException incomplete){if(action.target().startsWith("candidate:")&&Set.of("FOREIGN","PARTIAL","UNKNOWN").contains(previous.state().path("storageState").asText()))throw incomplete;}}}
        String controllerProof=null;boolean admissionDispatched=false;
        if(action.kind()==Kind.ADMIT||action.kind()==Kind.START&&"candidate:broker".equals(action.target())){
            boolean brokerObserved=action.kind()==Kind.START&&previous!=null&&previous.state().path("serviceActive").asBoolean()&&previous.state().path("runtimeVerified").asBoolean()&&previous.state().path("configOwned").asBoolean()&&previous.state().path("unitOwned").asBoolean()&&previous.receipt().path("directoryIds").equals(previous.state().path("directoryIds"));
            var proof=controllerProof(op,frozen,step,action.kind()==Kind.START,brokerObserved);controllerProof=mapper.writeValueAsString(proof);admissionDispatched=proof.voterVerified()||previous!=null&&"ADMISSION_SENT".equals(previous.phase());
            if(action.kind()==Kind.ADMIT&&admissionDispatched&&previous!=null){nativeEvidence.completed(frozen.plan(),action,previous,true);confirm(op,action,action.kind().requiredFacts());return;}
            // The membership observation may have replaced this step's latest
            // task reference. Restore the actual effect target's fresh receipt.
            if(uncertain)inspect(op,frozen,step,action.target());
        }
        var task=ledger.dispatch(op.getId(),step.getId(),false,controllerProof);var result=awaiter.await(task.getId(),180);var proof=nativeEvidence.read(frozen,action.target(),task,result);
        if(action.kind()==Kind.ADMIT)admissionDispatched|="ADMISSION_SENT".equals(proof.phase());
        confirm(op,action,nativeEvidence.completed(frozen.plan(),action,proof,admissionDispatched));
    }
    private void server(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,Step action,JobStep step)throws Exception{
        var plan=frozen.plan();
        if(action.kind()!=Kind.PUBLISH)ledger.beginServerStep(op.getId(),action.id());
        switch(action.kind()){
            case FINAL_VALIDATE->{live.baseline(plan);candidateValidation(op,frozen,step);for(var member:plan.participants()){var proof=inspect(op,frozen,step,member.key());if(!proof.state().path("active").asBoolean()||proof.state().path("pid").asInt()!=member.pid()||!member.startTicks().equals(proof.state().path("startTicks").asText())||!member.configSha256().equals(proof.state().path("configSha256").asText()))throw new IllegalStateException("Participant process changed since review.");}live.baseline(plan);}
            case PRE_MAINTENANCE->{live.baseline(plan);candidateValidation(op,frozen,step);for(var member:plan.participants()){var proof=inspect(op,frozen,step,member.key());if(!"STAGED".equals(proof.phase())||!proof.state().path("active").asBoolean()||!proof.state().path("backupVerified").asBoolean()||!proof.state().path("stageVerified").asBoolean())throw new IllegalStateException("All original participant units and verified staging are required before downtime.");}live.baseline(plan);}
            case VERIFY_CONFIG->staticConfigurations(op,frozen,step);
            case CATCH_UP->awaitController(op,frozen,step,false);
            case VERIFY_CONTROLLER->awaitController(op,frozen,step,true);
            case VERIFY_BROKER->{var proof=inspect(op,frozen,step,"candidate:broker");nativeEvidence.runningCandidate(proof);awaitBroker(plan);}
            case VERIFY_HEALTH->stableHealth(op,frozen,step);
            case PUBLISH->{jointVerification(op,frozen,step);ledger.publish(op.getId(),new Evidence(frozen.digest(),action.id(),action.target(),Instant.now(),Set.of("ALL_PLANNED_ROLES_VERIFIED")));return;}
            default->throw new IllegalStateException("Unexpected server expansion stage: "+action.kind());
        }
        confirm(op,action,action.kind().requiredFacts());
    }
    private void candidateValidation(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step){var task=ledger.dispatch(op.getId(),step.getId(),false,null);nativeEvidence.capability(frozen,task,awaiter.await(task.getId(),180));}
    private KafkaExpansionNativeEvidence.Observation inspect(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step,String target){var task=ledger.inspect(op.getId(),step.getId(),target);return nativeEvidence.read(frozen,target,task,awaiter.await(task.getId(),180));}
    private boolean staticConfigurations(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step){if(frozen.plan().quorumMode()!=Mode.STATIC)return false;for(var member:frozen.plan().participants()){var proof=inspect(op,frozen,step,member.key());if(!nativeEvidence.expanded(proof))throw new IllegalStateException("Expanded voters must remain consistent in every reviewed participant configuration.");}return true;}
    private KafkaExpansionLiveVerifier.ControllerProof controllerProof(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step,boolean member){return controllerProof(op,frozen,step,member,false);}
    private KafkaExpansionLiveVerifier.ControllerProof controllerProof(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step,boolean member,boolean brokerObserved){boolean configurations=staticConfigurations(op,frozen,step);var candidate=inspect(op,frozen,step,"candidate:controller");nativeEvidence.runningCandidate(candidate);return live.controller(frozen,candidate.metadataDirectoryId(),member,configurations,brokerObserved||brokerConfirmed(op,frozen));}
    private void awaitController(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step,boolean member)throws Exception{Instant deadline=Instant.now().plusSeconds(Math.max(10,policy.getRegistrationTimeoutSeconds()));while(true){try{controllerProof(op,frozen,step,member);return;}catch(KafkaExpansionQuorum.NotReady waiting){if(!Instant.now().isBefore(deadline))throw waiting;pause();}}}
    private void awaitBroker(KafkaExpansionPlan plan)throws Exception{Instant deadline=Instant.now().plusSeconds(Math.max(10,policy.getRegistrationTimeoutSeconds()));while(true){try{live.broker(plan);return;}catch(KafkaExpansionQuorum.NotReady waiting){if(!Instant.now().isBefore(deadline))throw waiting;pause();}}}
    private void stableHealth(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step)throws Exception{jointVerification(op,frozen,step);int stable=0;Instant deadline=Instant.now().plusSeconds(Math.max(10,policy.getRegistrationTimeoutSeconds()));while(stable<Math.max(2,policy.getStableSamples())){try{live.health(frozen.plan());stable++;}catch(KafkaExpansionQuorum.NotReady waiting){stable=0;if(!Instant.now().isBefore(deadline))throw waiting;}if(stable<Math.max(2,policy.getStableSamples()))pause();}}
    private void jointVerification(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,JobStep step){boolean configurations=frozen.plan().quorumMode()==Mode.STATIC;for(var member:frozen.plan().participants()){var proof=inspect(op,frozen,step,member.key());if(!proof.state().path("active").asBoolean()||!proof.state().path("runtimeVerified").asBoolean()||configurations&&!nativeEvidence.expanded(proof))throw new IllegalStateException("All original participants must be active with verified runtime and consistent configuration before publication.");}var controller=inspect(op,frozen,step,"candidate:controller");nativeEvidence.runningCandidate(controller);KafkaExpansionNativeEvidence.Observation broker=null;if(frozen.plan().layout()==Layout.SEPARATE){broker=inspect(op,frozen,step,"candidate:broker");nativeEvidence.runningCandidate(broker);}live.controller(frozen,controller.metadataDirectoryId(),true,configurations,brokerConfirmed(op,frozen));live.health(frozen.plan());if(controller.observedAt().isBefore(Instant.now().minusSeconds(60))||broker!=null&&broker.observedAt().isBefore(Instant.now().minusSeconds(60)))throw new IllegalStateException("Joint candidate verification expired before publication; reconcile with fresh observations.");}
    private boolean brokerConfirmed(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen){var checkpoints=ledger.checkpoints(ledger.forJob(op.getJobId()));return frozen.steps().stream().filter(s->s.kind()==Kind.START&&s.target().equals("candidate:broker")).anyMatch(s->checkpoints.snapshot().get(s.id())==State.CONFIRMED);}
    private void confirm(KafkaExpansionOperation op,Step action,Set<String> facts){ledger.confirm(op.getId(),new Evidence(op.getPlanDigest(),action.id(),action.target(),Instant.now(),facts));}
    private void drain(JobStep step){if(step.getAgentTaskId()==null)return;String status=awaiter.status(step.getAgentTaskId());if(!Set.of("SUCCESS","FAILED","CANCELLED").contains(Objects.toString(status,""))){try{awaiter.await(step.getAgentTaskId(),180);}catch(RuntimeException e){if(!Set.of("SUCCESS","FAILED","CANCELLED").contains(Objects.toString(awaiter.status(step.getAgentTaskId()),"")))throw e;}}}
    private void pause()throws InterruptedException{Thread.sleep(Math.max(1,policy.getStabilityPollSeconds())*1000L);}
}
