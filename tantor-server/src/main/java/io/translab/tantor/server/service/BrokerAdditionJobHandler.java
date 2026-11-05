package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.*;

/** Dedicated broker workflow: completed checkpoints survive job retries and backend restarts. */
@Component @RequiredArgsConstructor
public class BrokerAdditionJobHandler implements JobHandler {
    private final BrokerAdditionLedger ledger;
    private final JobService jobs;
    private final AddNodeReadinessService readiness;
    private final BrokerAdditionKafka kafka;
    private final AddNodeArtifactCatalog artifacts;
    private final AgentTaskAwaiter awaiter;
    private final ClusterRepository clusters;
    private final ObjectMapper mapper;
    private final io.translab.tantor.server.config.ArtifactRepositoryProperties artifactPolicy;
    private final io.translab.tantor.server.config.KafkaDeploymentProperties deploymentPolicy;
    @org.springframework.beans.factory.annotation.Value("${tantor.add-node.registration-timeout-seconds:120}") private int registrationTimeoutSeconds=120;
    @Override public boolean supports(JobType type){return type==JobType.BROKER_ADDITION;}

    @Override public void execute(Job job) {
        var op=ledger.forJob(job.getId());
        if(op.getState()==BrokerAdditionState.SUCCEEDED)return;
        try {
            var steps=jobs.getSteps(job.getId());
            if(steps.size()!=6)throw new IllegalStateException("Incomplete broker execution plan.");
            var validation=steps.get(0);
            if(validation.getStatus()!=JobStepStatus.SUCCESS) {
                jobs.startStep(validation.getId());ledger.validating(op.getId());
                var proposal=ledger.proposed(op);
                var preview=readiness.startForOperation(op.getClusterId(),op.getHostId(),job.getId(),proposal.brokerRack());
                jobs.attachAgentTask(validation.getId(),preview.previewId());awaiter.await(preview.previewId(),90);
                preview=readiness.getForOperation(op.getClusterId(),preview.previewId(),job.getId());
                var cluster=cluster(op);var config=config(cluster);
                var p=BrokerAdditionPlan.freeze(preview,op.getId(),op.getNodeId(),proposal.quorumVoters());
                var runtime=runtimeAdapter(cluster);
                String properties=kafka.properties(cluster,p);
                var payload=new BrokerAdditionLedger.Payload(p,properties,BrokerAdditionLedger.hash(ledger.json(p)+"\n"+properties+"\n"+ledger.json(runtime)),kafka.voterBaseline(cluster),runtime);
                ledger.freeze(op.getId(),payload);
                var task=ledger.dispatch(op.getId(),validation.getId(),"VALIDATE_ADD_BROKER_PLAN",BrokerAdditionState.VALIDATING);
                var proof=mapper.readTree(awaiter.await(task.getId(),90).getLogOutput());
                if(!"add-broker/v1".equals(proof.path("schemaVersion").asText())||!op.getId().toString().equals(proof.path("operationId").asText())||!op.getHostId().equals(proof.path("hostId").asText())||!p.contextHash().equals(proof.path("contextHash").asText())||!proof.path("payloadVerified").asBoolean()||!proof.path("rootMutationSupported").asBoolean()||proof.path("supportedActions").size()!=4)
                    throw new IllegalStateException("Compatible root internal agent and complete broker mutation capabilities are required.");
                jobs.completeStep(validation.getId(),"Final readiness, frozen inherited config, approved package and agent capabilities verified.");
            }
            op=ledger.forJob(job.getId());var payload=ledger.payload(op);
            if(op.getState()==BrokerAdditionState.RECOVERY_REQUIRED) reconcile(op,payload,steps.stream().filter(s->s.getStatus()!=JobStepStatus.SUCCESS).findFirst().orElse(steps.get(5)));
            mutation(op,payload,steps.get(1),"PREPARE_ADD_BROKER",BrokerAdditionState.PREPARING,BrokerAdditionState.PREPARED,"PREPARED");
            mutation(op,payload,steps.get(2),"FORMAT_ADD_BROKER",BrokerAdditionState.FORMATTING,BrokerAdditionState.FORMATTED,"FORMATTED");
            mutation(op,payload,steps.get(3),"START_ADD_BROKER",BrokerAdditionState.STARTING,BrokerAdditionState.VERIFYING,"STARTED");
            var verify=steps.get(4);jobs.startStep(verify.getId());
            String evidence=verifyStable(op,payload,verify);jobs.completeStep(verify.getId(),evidence);
            var publication=steps.get(5);jobs.startStep(publication.getId());
            // Re-verify just before transactional inventory publication, including after resume.
            evidence=kafka.verify(cluster(op),payload,config(cluster(op)));verifyCandidate(op,payload,publication);ledger.publish(op.getId(),evidence);jobs.completeStep(publication.getId(),evidence);
        } catch(Exception e) {
            ledger.recovery(op.getId(),e.getMessage());
            jobs.getSteps(job.getId()).stream().filter(s->s.getStatus()==JobStepStatus.IN_PROGRESS).findFirst().ifPresent(s->jobs.failStep(s.getId(),e.getMessage()));
            throw new IllegalStateException("Broker addition requires reconciliation; reservation retained. "+e.getMessage(),e);
        }
    }
    private void mutation(BrokerAdditionOperation op,BrokerAdditionLedger.Payload payload,JobStep step,String command,BrokerAdditionState intent,BrokerAdditionState complete,String expectedPhase)throws Exception {
        if(step.getStatus()==JobStepStatus.SUCCESS)return;
        var current=ledger.forJob(op.getJobId());
        // Reconciliation can prove this action already completed even when its callback was lost.
        if((complete==BrokerAdditionState.PREPARED&&Set.of(BrokerAdditionState.PREPARED,BrokerAdditionState.FORMATTED,BrokerAdditionState.VERIFYING).contains(current.getState()))
                ||(complete==BrokerAdditionState.FORMATTED&&Set.of(BrokerAdditionState.FORMATTED,BrokerAdditionState.VERIFYING).contains(current.getState()))
                ||(complete==BrokerAdditionState.VERIFYING&&current.getState()==BrokerAdditionState.VERIFYING)) {
            jobs.completeStep(step.getId(),"Reconciled operation-owned checkpoint; mutation was not replayed.");return;
        }
        jobs.startStep(step.getId());
        assertCurrent(op,payload);
        var task=ledger.dispatch(op.getId(),step.getId(),command,intent);
        if(!command.equals(task.getCommand()))throw new IllegalStateException("Previous action is still in flight; preserve its task and reconcile after completion.");
        var result=awaiter.await(task.getId(),180);
        var proof=receipt(op,payload,result.getLogOutput());
        if(!expectedPhase.equals(proof.path("phase").asText()))throw new IllegalStateException("Candidate action checkpoint is not proven.");
        if(!"PREPARED".equals(expectedPhase)&&!"MATCHING".equals(proof.path("storageState").asText()))throw new IllegalStateException("Storage identity evidence is incomplete.");
        if("STARTED".equals(expectedPhase)&&!proof.path("serviceActive").asBoolean())throw new IllegalStateException("Candidate service is not active.");
        ledger.checkpoint(op.getId(),complete,result.getLogOutput());jobs.completeStep(step.getId(),result.getLogOutput());
    }
    private void reconcile(BrokerAdditionOperation op,BrokerAdditionLedger.Payload payload,JobStep step)throws Exception {
        if(step.getAgentTaskId()!=null) {
            String status=awaiter.status(step.getAgentTaskId());
            if(!Set.of("SUCCESS","FAILED","CANCELLED").contains(status))awaiter.await(step.getAgentTaskId(),180);
        }
        var task=ledger.dispatch(op.getId(),step.getId(),"INSPECT_ADD_BROKER",BrokerAdditionState.RECOVERY_REQUIRED);
        var proof=receipt(op,payload,awaiter.await(task.getId(),90).getLogOutput());
        String phase=proof.path("phase").asText(),storage=proof.path("storageState").asText();
        var checkpoint=switch(phase) {
            case "ABSENT","CLAIMED" -> {
                if(!"MISSING".equals(storage)||proof.path("serviceActive").asBoolean())throw new IllegalStateException("Unowned or partially prepared candidate needs operator investigation.");
                yield BrokerAdditionState.PREPARING;
            }
            case "PREPARED" -> {
                if(!"MISSING".equals(storage)||proof.path("serviceActive").asBoolean())throw new IllegalStateException("Unexpected candidate storage/process before formatting.");yield BrokerAdditionState.PREPARED;
            }
            case "FORMAT_INTENT","FORMATTED" -> {
                if(!"MATCHING".equals(storage)||proof.path("serviceActive").asBoolean())throw new IllegalStateException("Partial or unknown format outcome; formatter cannot be replayed.");yield BrokerAdditionState.FORMATTED;
            }
            case "START_INTENT","STARTED" -> {
                if(!"MATCHING".equals(storage))throw new IllegalStateException("Candidate storage identity is not proven.");yield proof.path("serviceActive").asBoolean()?BrokerAdditionState.VERIFYING:BrokerAdditionState.FORMATTED;
            }
            default -> throw new IllegalStateException("Unknown candidate ownership state.");
        };
        ledger.checkpoint(op.getId(),checkpoint,proof.toString());
    }
    private JsonNode receipt(BrokerAdditionOperation op,BrokerAdditionLedger.Payload payload,String raw)throws Exception {
        var proof=mapper.readTree(raw);
        if(!"add-broker/v1".equals(proof.path("schemaVersion").asText())||!op.getId().toString().equals(proof.path("operationId").asText())||!op.getHostId().equals(proof.path("hostId").asText())||!payload.digest().equals(proof.path("planDigest").asText())||proof.path("nodeId").asInt()!=op.getNodeId()||!payload.plan().kafkaClusterId().equals(proof.path("kafkaClusterId").asText()))throw new IllegalStateException("Agent receipt is not bound to this operation and frozen plan.");
        Instant timestamp=Instant.parse(proof.path("observedAt").asText());if(timestamp.isBefore(Instant.now().minusSeconds(180))||timestamp.isAfter(Instant.now().plusSeconds(5)))throw new IllegalStateException("Agent receipt is stale or clock is inconsistent.");return proof;
    }
    private String verifyStable(BrokerAdditionOperation op,BrokerAdditionLedger.Payload payload,JobStep step)throws Exception {
        String evidence="";
        long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(Math.max(0,registrationTimeoutSeconds));
        for(int sample=0;sample<3;) {
            try {evidence=kafka.verify(cluster(op),payload,config(cluster(op)));verifyCandidate(op,payload,step);sample++;}
            catch(BrokerAdditionKafka.RegistrationPending pending) {
                sample=0;if(System.nanoTime()>=deadline)throw pending;
            }
            if(sample<3)Thread.sleep(sample==0?2000:5000);
        }
        return evidence+" Three successful observations separated by five seconds.";
    }
    private void verifyCandidate(BrokerAdditionOperation op,BrokerAdditionLedger.Payload payload,JobStep step)throws Exception {
        var task=ledger.dispatch(op.getId(),step.getId(),"INSPECT_ADD_BROKER",BrokerAdditionState.VERIFYING);
        var proof=receipt(op,payload,awaiter.await(task.getId(),90).getLogOutput());
        if(!Set.of("STARTED","START_INTENT").contains(proof.path("phase").asText())||!"MATCHING".equals(proof.path("storageState").asText())||!proof.path("serviceActive").asBoolean())throw new IllegalStateException("Fresh candidate process/storage/owned config verification failed.");
    }
    private void assertCurrent(BrokerAdditionOperation op,BrokerAdditionLedger.Payload payload) {
        jobs.assertBrokerAdditionOwnsCluster(op.getClusterId(),op.getJobId());var cluster=cluster(op);
        if(!payload.plan().kafkaClusterId().equals(cluster.getKafkaClusterId())||!payload.voterBaseline().equals(kafka.voterBaseline(cluster)))throw new IllegalStateException("Cluster identity/voter baseline changed.");
        String pinned=Objects.toString(config(cluster).get("artifact_id"),"");var approved=artifacts.resolve(payload.plan().kafkaVersion(),pinned);
        if(!approved.checksum().equalsIgnoreCase(payload.plan().artifactChecksum()))throw new IllegalStateException("Approved artifact changed.");
        if(!kafka.properties(cluster,payload.plan()).equals(payload.properties()))throw new IllegalStateException("Inherited broker configuration changed after acceptance.");
        if(!runtimeAdapter(cluster).equals(payload.runtime()))throw new IllegalStateException("Approved monitoring/runtime configuration changed after acceptance.");
    }
    private BrokerAdditionLedger.RuntimeOptions runtimeAdapter(Cluster cluster) {
        if(Boolean.TRUE.equals(cluster.getNodeExporterEnabled()))throw new IllegalStateException("Node-exporter host installation requires a validated adapter; inherited monitoring cannot be silently dropped.");
        if(Boolean.FALSE.equals(cluster.getJmxEnabled()))return new BrokerAdditionLedger.RuntimeOptions("","",0,deploymentPolicy.getLimitNofile());
        var artifact=artifacts.resolveType("JMX_EXPORTER",null,artifactPolicy.getJmxExporterArtifactId());
        int port=cluster.getJmxExporterPort()==null?7071:cluster.getJmxExporterPort();
        var config=config(cluster);Object override=config.getOrDefault("jmx_port",config.get("broker_jmx_port"));if(override!=null)port=Integer.parseInt(String.valueOf(override));
        if(port<1||port>65535)throw new IllegalStateException("Explicit valid JMX exporter port is required.");
        return new BrokerAdditionLedger.RuntimeOptions(artifact.id(),artifact.checksum(),port,deploymentPolicy.getLimitNofile());
    }
    private Cluster cluster(BrokerAdditionOperation op){return clusters.findWithServicesById(op.getClusterId()).orElseThrow();}
    private Map<String,Object> config(Cluster c){try{return mapper.readValue(Objects.toString(c.getConfigJson(),"{}"),new TypeReference<>(){});}catch(Exception e){throw new IllegalStateException("Cluster configuration is unreadable.",e);}}
}
