package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.util.*;
import static io.translab.tantor.server.service.KafkaExpansionWorkflow.*;

/** Short transactional ownership boundary; agent/Kafka waits belong in the handler. */
@Service @RequiredArgsConstructor
public class KafkaExpansionLedger {
    public static final String CANDIDATE="EXECUTE_QUORUM_CANDIDATE",PARTICIPANT="EXECUTE_QUORUM_PARTICIPANT",VALIDATE="VALIDATE_QUORUM_EXPANSION";
    private final KafkaExpansionOperationRepository operations;
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final ClusterServiceAssignmentRepository assignments;
    private final JobStepRepository steps;
    private final TaskRepository tasks;
    private final JobService jobs;
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final io.translab.tantor.server.config.AddNodeInspectionProperties policy;
    @Value("${tantor.add-node-expansion.execution-enabled:false}") private boolean executionEnabled;
    public record Accepted(UUID operationId,UUID jobId,KafkaExpansionState state) {}
    public record Dispatch(String hostId,String command,String action,String role,String unit) {}

    /** Internal entry point: the HTTP controller must resolve a server-owned fresh preview first. */
    @Transactional
    public Accepted accept(KafkaExpansionPlan.Frozen reviewed,String requestKey,String requestDigest){
        if(!executionEnabled)throw conflict("Controller/separate-process execution is held until release acceptance passes.");
        if(requestKey==null||!requestKey.matches("[A-Za-z0-9_-]{16,128}")||requestDigest==null||!requestDigest.matches("[a-f0-9]{64}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Reviewed inputs and an idempotency key are required.");
        var frozen=KafkaExpansionPlan.read(mapper,reviewed.raw(),reviewed.digest());var plan=frozen.plan();
        var cluster=clusters.findForServiceUpdate(plan.clusterId()).orElseThrow(()->conflict("Cluster not found."));
        var previous=operations.findByClusterIdAndRequestKey(plan.clusterId(),requestKey);
        if(previous.isPresent()){var op=previous.get();if(!requestDigest.equals(op.getRequestDigest()))throw conflict("Idempotency key already belongs to different reviewed inputs.");return accepted(op);}
        assertAllowed(plan);AddNodeProtectedHostGuard.cluster(cluster,hosts,policy,mapper);
        if(!Objects.equals(plan.kafkaClusterId(),cluster.getKafkaClusterId())||!Objects.equals(plan.kafkaVersion(),cluster.getKafkaVersion())||!"SUCCESS".equals(cluster.getStatus())||!"kraft".equalsIgnoreCase(cluster.getMode())||"EXTERNAL".equalsIgnoreCase(cluster.getOriginType()))throw conflict("Managed cluster identity or availability changed.");
        // Deterministic host lock order is shared by all expansion accepts.
        for(String id:plan.reservedHosts())if(jdbc.queryForList("SELECT id FROM kf_hosts WHERE id=? FOR UPDATE",id).size()!=1)throw conflict("An enrolled expansion host disappeared.");
        var candidate=hosts.findById(plan.candidateHostId()).orElseThrow();
        if(candidate.getClusterId()!=null||assignments.findReservationsForLiveClusters().stream().anyMatch(s->candidate.getId().equals(s.getHostId())))throw conflict("Candidate host is already assigned.");
        if(!Objects.equals(plan.candidateIp(),candidate.getHostIp()))throw conflict("Candidate address changed.");
        var memberServices=assignments.findByClusterId(plan.clusterId());var expected=new HashSet<UUID>();
        for(var member:plan.participants())for(UUID id:member.serviceIds()){
            var service=memberServices.stream().filter(s->id.equals(s.getId())).findFirst().orElseThrow(()->conflict("Participant assignment changed."));
            if(!Objects.equals(service.getHostId(),member.hostId())||!Objects.equals(service.getNodeId(),member.nodeId())||service.getRole()==null||!member.role().contains(service.getRole().replace("broker_controller","broker,controller")))throw conflict("Participant identity changed.");expected.add(id);
        }
        var actual=memberServices.stream().filter(s->s.getRole()!=null&&(s.getRole().contains("broker")||s.getRole().contains("controller"))).map(ClusterServiceAssignment::getId).collect(java.util.stream.Collectors.toSet());
        if(!expected.equals(actual))throw conflict("The complete participant scope changed.");
        Job job=new Job();job.setType(JobType.QUORUM_EXPANSION);job.setResourceKey("cluster:"+plan.clusterId());job.setRollbackSupported(false);job.setPayload(json(Map.of("operationId",plan.operationId(),"clusterId",plan.clusterId(),"layout",plan.layout(),"mode",plan.quorumMode())));
        var jobSteps=new ArrayList<JobStep>();for(var workflow:frozen.steps()){var step=new JobStep();step.setStepOrder(jobSteps.size()+1);step.setName(workflow.kind().name().replace('_',' ')+" · "+workflow.target());step.setTargetId(workflow.target());step.setPayload(json(workflow));jobSteps.add(step);}job=jobs.createJob(job,jobSteps);
        var op=new KafkaExpansionOperation();op.setId(plan.operationId());op.setClusterId(plan.clusterId());op.setHostId(plan.candidateHostId());op.setParticipantHosts(plan.participants().stream().map(KafkaExpansionPlan.Participant::hostId).distinct().sorted().toArray(String[]::new));op.setControllerId(plan.process("controller").nodeId());if(plan.layout()==Layout.SEPARATE)op.setBrokerId(plan.process("broker").nodeId());op.setJobId(job.getId());op.setRequestKey(requestKey);op.setRequestDigest(requestDigest);op.setPlan(frozen.raw());op.setPlanDigest(frozen.digest());operations.saveAndFlush(op);return accepted(op);
    }
    public KafkaExpansionOperation forJob(UUID jobId){return operations.findByJobId(jobId).orElseThrow(()->conflict("Expansion operation not found."));}
    public KafkaExpansionOperation get(UUID clusterId,UUID operationId){return operations.findById(operationId).filter(op->clusterId.equals(op.getClusterId())).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Expansion operation not found for this cluster."));}
    public Optional<Accepted> acceptedRequest(UUID clusterId,String key,String digest){if(key==null||!key.matches("[A-Za-z0-9_-]{16,128}"))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"A valid idempotency key is required.");return operations.findByClusterIdAndRequestKey(clusterId,key).map(op->{if(!Objects.equals(digest,op.getRequestDigest()))throw conflict("Idempotency key already belongs to different reviewed inputs.");return accepted(op);});}
    public KafkaExpansionPlan.Frozen frozen(KafkaExpansionOperation op){return KafkaExpansionPlan.read(mapper,op.getPlan(),op.getPlanDigest());}
    public Checkpoints checkpoints(KafkaExpansionOperation op){try{return new Checkpoints(op.getPlanDigest(),frozen(op).steps(),mapper.readValue(op.getCheckpoints(),new TypeReference<Map<String,State>>(){}));}catch(Exception e){throw new IllegalStateException("Invalid persisted expansion checkpoints.",e);}}

    @Transactional
    public Task dispatch(UUID operationId,UUID stepId,boolean inspection,String controllerProof){
        return dispatchInternal(operationId,stepId,inspection,controllerProof,null);
    }
    @Transactional
    public Task inspect(UUID operationId,UUID stepId,String target){
        if(target==null||target.isBlank())throw conflict("A frozen inspection target is required.");
        return dispatchInternal(operationId,stepId,true,null,target);
    }
    private Task dispatchInternal(UUID operationId,UUID stepId,boolean inspection,String controllerProof,String inspectionTarget){
        var op=owned(operationId);var frozen=frozen(op);var checkpoint=checkpoints(op);var workflow=checkpoint.next().orElseThrow(()->conflict("Expansion has no incomplete step."));
        var step=steps.findById(stepId).orElseThrow();if(!op.getJobId().equals(step.getJob().getId())||!jsonTree(step.getPayload()).path("id").asText().equals(workflow.id()))throw conflict("Foreign or out-of-order expansion job step.");
        assertAllowed(frozen.plan());
        var routeStep=inspectionTarget==null?workflow:new Step(workflow.id(),workflow.kind(),inspectionTarget,workflow.role());
        var dispatch=route(frozen.plan(),routeStep,inspection);
        AddNodeProtectedHostGuard.hostId(dispatch.hostId(),hosts,policy);
        // Never replace a pending task or dispatch another effect behind it.
        var outstanding=jdbc.queryForList("SELECT a.task_id,a.step_id FROM kf_quorum_expansion_action a JOIN kf_tasks t ON t.id=a.task_id WHERE a.operation_id=? AND t.status NOT IN ('SUCCESS','FAILED','CANCELLED')",op.getId());
        if(!outstanding.isEmpty()){
            if(outstanding.size()==1&&stepId.toString().equals(outstanding.getFirst().get("step_id").toString())){
                var pending=tasks.findById(UUID.fromString(outstanding.getFirst().get("task_id").toString())).orElseThrow();var parameters=jsonTree(pending.getParameters());
                if(dispatch.hostId().equals(pending.getHostId())&&dispatch.command().equals(pending.getCommand())&&dispatch.action().equals(parameters.path("action").asText())&&(!inspection||routeStep.target().equals(parameters.path("inspection_target").asText())))return pending;
            }
            throw conflict("An earlier expansion task is still in flight. Preserve it and reconcile after completion.");
        }
        if(!inspection){
            State state=checkpoint.snapshot().get(workflow.id());
            if(state==State.INTENT&&workflow.kind().mutation()){
                if(step.getAgentTaskId()==null)throw conflict("Uncertain mutation requires a fresh native inspection.");
                var prior=tasks.findById(step.getAgentTaskId()).orElseThrow();validateInspection(op,frozen,workflow,prior);
                // Keep INTENT durable throughout a reconciled retry. Native receipts
                // decide whether the effect is absent, already complete or unsafe.
            }else checkpoint.intent(workflow.id());
        }
        jdbc.queryForObject("SELECT set_config('tantor.quorum_expansion_id',?,true)",String.class,op.getId().toString());
        var parameters=new LinkedHashMap<String,String>();parameters.put("expansion_plan",frozen.raw());parameters.put("plan_digest",frozen.digest());parameters.put("action",dispatch.action());parameters.put("dispatch_expires_at",Instant.now().plusSeconds(120).toString());if(inspection)parameters.put("inspection_target",routeStep.target());if(dispatch.role()!=null)parameters.put("role",dispatch.role());if(dispatch.unit()!=null)parameters.put("participant_unit",dispatch.unit());if(controllerProof!=null)parameters.put("controller_proof",controllerProof);
        Task task=new Task();task.setClusterId(op.getClusterId());task.setHostId(dispatch.hostId());task.setCommand(dispatch.command());task.setStatus("PENDING");task.setParameters(json(parameters));task=tasks.saveAndFlush(task);
        jdbc.update("INSERT INTO kf_quorum_expansion_action(task_id,operation_id,step_id,workflow_step,command,action,mutation) VALUES (?,?,?,?,?,?,?)",task.getId(),op.getId(),stepId,workflow.id(),dispatch.command(),dispatch.action(),!inspection&&workflow.kind().mutation());
        step.setAgentTaskId(task.getId());steps.saveAndFlush(step);op.setCheckpoints(json(checkpoint.snapshot()));op.setState(KafkaExpansionState.EXECUTING);operations.saveAndFlush(op);return task;
    }
    @Transactional
    public void beginServerStep(UUID operationId,String stepId){var op=owned(operationId);var checkpoint=checkpoints(op);if(checkpoint.next().orElseThrow().kind().mutation())throw conflict("Native mutations and publication use their own durable dispatch boundary.");checkpoint.intent(stepId);op.setCheckpoints(json(checkpoint.snapshot()));op.setState(KafkaExpansionState.EXECUTING);operations.saveAndFlush(op);}
    @Transactional
    public void confirm(UUID operationId,Evidence proof){var op=owned(operationId);var checkpoint=checkpoints(op);if(checkpoint.next().orElseThrow().kind()==Kind.PUBLISH)throw conflict("Publication cannot be confirmed outside its transaction.");assertDrained(op.getId());checkpoint.confirm(proof,Instant.now());op.setCheckpoints(json(checkpoint.snapshot()));op.setLastEvidence(json(proof));op.setLastError(null);operations.saveAndFlush(op);}
    @Transactional
    public void recovery(UUID operationId,String reason){var op=operations.findForUpdate(operationId).orElseThrow();if(op.getState().reserved()){op.setState(KafkaExpansionState.RECOVERY_REQUIRED);op.setLastError(reason);operations.saveAndFlush(op);}}
    @Transactional
    public Accepted cancelSafe(UUID clusterId,UUID operationId){
        clusters.findForServiceUpdate(clusterId).orElseThrow();var op=operations.findForUpdate(operationId).filter(o->clusterId.equals(o.getClusterId())).orElseThrow();if(op.getState()==KafkaExpansionState.CANCELLED_SAFE)return accepted(op);if(!op.getState().reserved())throw conflict("A completed expansion cannot be cancelled.");
        assertDrained(op.getId());Integer mutations=jdbc.queryForObject("SELECT count(*) FROM kf_quorum_expansion_action WHERE operation_id=? AND mutation",Integer.class,op.getId());
        if(mutations==null||mutations!=0||!checkpoints(op).canCancelBeforeMutation())throw conflict("A mutation was dispatched. Retain all reservations and reconcile/resume the same operation.");
        op.setState(KafkaExpansionState.CANCELLED_SAFE);op.setLastError("Cancelled before any native mutation intent.");operations.saveAndFlush(op);jobs.updateJobStatus(op.getJobId(),JobStatus.FAILED);return accepted(op);
    }

    @Transactional
    public void publish(UUID operationId,Evidence jointVerification){
        var existing=operations.findById(operationId).orElseThrow();if(existing.getState()==KafkaExpansionState.SUCCEEDED)return;
        var op=owned(operationId);var frozen=frozen(op);var plan=frozen.plan();var checkpoint=checkpoints(op);var next=checkpoint.next().orElseThrow();
        if(next.kind()!=Kind.PUBLISH||!Objects.equals(jointVerification.stepId(),next.id())||!Objects.equals(jointVerification.planDigest(),frozen.digest())||!Objects.equals(jointVerification.target(),next.target())||jointVerification.observedAt()==null||jointVerification.observedAt().isBefore(Instant.now().minusSeconds(60))||jointVerification.observedAt().isAfter(Instant.now().plusSeconds(5))||jointVerification.facts()==null||!jointVerification.facts().contains("ALL_PLANNED_ROLES_VERIFIED"))throw conflict("Fresh joint role verification and completed prior checkpoints are required.");
        assertDrained(op.getId());var cluster=clusters.findWithServicesById(op.getClusterId()).orElseThrow();
        if(!Objects.equals(cluster.getKafkaClusterId(),plan.kafkaClusterId())||!Objects.equals(cluster.getKafkaVersion(),plan.kafkaVersion()))throw conflict("Cluster identity changed before publication.");
        var current=assignments.findReservationsForLiveClusters();if(current.stream().anyMatch(s->op.getHostId().equals(s.getHostId())))throw conflict("Candidate already has a published service assignment.");
        jdbc.queryForObject("SELECT set_config('tantor.quorum_expansion_id',?,true)",String.class,op.getId().toString());
        var ids=new ArrayList<>(cluster.getNodeIds()==null?List.<Integer>of():cluster.getNodeIds());
        for(var process:plan.processes()){
            if(ids.contains(process.nodeId()))throw conflict("Candidate node identity already exists in inventory.");
            var assignment=new ClusterServiceAssignment();assignment.setCluster(cluster);assignment.setHostId(plan.candidateHostId());assignment.setRole(process.role());assignment.setNodeId(process.nodeId());assignment.setStatus("SUCCESS");
            var config=new LinkedHashMap<String,Object>();config.put("service_name",plan.unit(process.role()).replaceFirst("\\.service$",""));config.put("config_path",plan.configPath(process.role()));config.put("data_paths",String.join(",",process.dataPaths()));config.put("metadata_path",Objects.toString(process.metadataPath(),""));config.put("log_path",process.logPath());config.put("java_home",plan.javaHome());config.put("runtime_user",plan.runtimeUser());config.put("listener_port",process.port());config.put("heap_xmx",process.heapXmx());config.put("install_base_path",plan.deploymentSettings()==null?plan.parcelDir():plan.deploymentSettings().installPath());if(!process.brokerRack().isEmpty())config.put("broker.rack",process.brokerRack());
            if(plan.deploymentSettings()!=null)config.put("add_node_operation_id",op.getId().toString());assignment.setConfigJson(json(config));if(process.runtime().jmxPort()>0)assignment.setJmxExporterPort(process.runtime().jmxPort());assignments.saveAndFlush(assignment);ids.add(process.nodeId());
        }
        var host=hosts.findById(op.getHostId()).orElseThrow();if(host.getClusterId()!=null)throw conflict("Candidate host ownership changed.");host.setClusterId(op.getClusterId());hosts.saveAndFlush(host);cluster.setNodeIds(ids);
        var controller=plan.process("controller");var endpoints=new TreeSet<String>();var voters=new TreeSet<String>();for(var voter:plan.voters()){endpoints.add(endpoint(voter.hostIp(),voter.port()));voters.add(voter.nodeId()+"@"+endpoint(voter.hostIp(),voter.port()));}endpoints.add(endpoint(plan.candidateIp(),controller.port()));voters.add(controller.nodeId()+"@"+endpoint(plan.candidateIp(),controller.port()));
        var config=configuration(cluster.getConfigJson());if(plan.quorumMode()==Mode.STATIC){config.put("controller_quorum_voters",String.join(",",voters));if(config.containsKey("quorum_voters"))config.put("quorum_voters",String.join(",",voters));}else{config.put("controller_quorum_bootstrap_servers",String.join(",",endpoints));if(config.containsKey("controller_bootstrap_servers"))config.put("controller_bootstrap_servers",String.join(",",endpoints));}config.put("controller_endpoints",List.copyOf(endpoints));cluster.setConfigJson(json(config));
        if(plan.layout()==Layout.SEPARATE){var seeds=new LinkedHashSet<String>();for(String seed:Objects.toString(cluster.getBootstrapServers(),"").split(","))if(!seed.isBlank())seeds.add(seed.strip());seeds.add(endpoint(plan.candidateIp(),plan.process("broker").port()));cluster.setBootstrapServers(String.join(",",seeds));}clusters.saveAndFlush(cluster);
        if(checkpoint.snapshot().get(next.id())==State.PENDING)checkpoint.intent(next.id());var facts=new HashSet<>(jointVerification.facts());facts.add("TASKS_DRAINED");facts.add("PUBLICATION_COMMITTED");var committed=new Evidence(frozen.digest(),next.id(),next.target(),jointVerification.observedAt(),Set.copyOf(facts));checkpoint.confirm(committed,Instant.now());op.setCheckpoints(json(checkpoint.snapshot()));op.setLastEvidence(json(committed));op.setLastError(null);op.setState(KafkaExpansionState.SUCCEEDED);operations.saveAndFlush(op);
    }
    private KafkaExpansionOperation owned(UUID id){var existing=operations.findById(id).orElseThrow();jobs.assertQuorumExpansionOwnsCluster(existing.getClusterId(),existing.getJobId());var op=operations.findForUpdate(id).orElseThrow();if(!op.getState().reserved())throw conflict("Expansion ownership is already closed.");return op;}
    private void assertDrained(UUID id){Integer pending=jdbc.queryForObject("SELECT count(*) FROM kf_quorum_expansion_action a JOIN kf_tasks t ON t.id=a.task_id WHERE a.operation_id=? AND t.status NOT IN ('SUCCESS','FAILED','CANCELLED')",Integer.class,id);if(pending==null||pending!=0)throw conflict("Expansion task outcomes remain in flight.");}
    private void validateInspection(KafkaExpansionOperation op,KafkaExpansionPlan.Frozen frozen,Step workflow,Task task){
        Integer linked=jdbc.queryForObject("SELECT count(*) FROM kf_quorum_expansion_action WHERE operation_id=? AND task_id=? AND workflow_step=? AND NOT mutation",Integer.class,op.getId(),task.getId(),workflow.id());if(linked==null||linked!=1)throw conflict("Inspection is not linked to this operation checkpoint.");
        var params=jsonTree(task.getParameters());var report=jsonTree(task.getLogOutput());var receipt=report.path("receipt");Instant at;try{at=Instant.parse(report.path("observedAt").asText());}catch(Exception e){throw conflict("Fresh native inspection is required.");}
        if(!"SUCCESS".equals(task.getStatus())||!"INSPECT".equals(params.path("action").asText())||!workflow.target().equals(params.path("inspection_target").asText())||!frozen.digest().equals(params.path("plan_digest").asText())||!op.getId().toString().equals(receipt.path("operationId").asText())||!frozen.digest().equals(receipt.path("planDigest").asText())||!task.getHostId().equals(receipt.path("hostId").asText())||at.isBefore(Instant.now().minusSeconds(60))||at.isAfter(Instant.now().plusSeconds(5)))throw conflict("Uncertain mutation requires a fresh matching native receipt.");
        var route=route(frozen.plan(),workflow,true);if(!route.hostId().equals(task.getHostId())||!route.command().equals(task.getCommand())||route.role()!=null&&!route.role().equals(receipt.path("role").asText())||route.unit()!=null&&!route.unit().equals(receipt.path("unit").asText()))throw conflict("Native inspection belongs to a different process.");
    }
    public static Dispatch route(KafkaExpansionPlan plan,Step step,boolean inspection){
        if((step.kind()==Kind.FINAL_VALIDATE||step.kind()==Kind.PRE_MAINTENANCE)&&!inspection)return new Dispatch(plan.candidateHostId(),VALIDATE,"VALIDATE",null,null);
        if(step.target().startsWith("candidate:")){String role=step.target().substring("candidate:".length());plan.process(role);if(!inspection&&!Set.of(Kind.PREPARE,Kind.FORMAT,Kind.START,Kind.ADMIT).contains(step.kind()))throw new IllegalArgumentException("This stage needs backend verification, not a native mutation.");return new Dispatch(plan.candidateHostId(),CANDIDATE,inspection?"INSPECT":step.kind().name(),role,null);}
        var member=plan.participants().stream().filter(p->p.key().equals(step.target())).findFirst().orElseThrow(()->new IllegalArgumentException("This stage has no native participant target."));
        if(!inspection&&(plan.quorumMode()!=Mode.STATIC||!Set.of(Kind.STAGE_VOTERS,Kind.STOP,Kind.APPLY_VOTERS,Kind.START).contains(step.kind())))throw new IllegalArgumentException("Unsupported existing-participant action.");return new Dispatch(member.hostId(),PARTICIPANT,inspection?"INSPECT":step.kind().name(),null,member.unit());
    }
    private Accepted accepted(KafkaExpansionOperation op){return new Accepted(op.getId(),op.getJobId(),op.getState());}
    private void assertAllowed(KafkaExpansionPlan plan){if(policy.excludes(plan.candidateHostId(),plan.candidateIp())||plan.participants().stream().anyMatch(p->policy.excludes(p.hostId(),p.hostIp())))throw conflict("An expansion host is excluded from Add Node operations.");}
    private String json(Object value){try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("Cannot persist expansion evidence.",e);}}
    private JsonNode jsonTree(String raw){try{if(raw==null||raw.length()>2*1024*1024)throw conflict("Missing or oversized expansion evidence.");return mapper.readTree(raw);}catch(com.fasterxml.jackson.core.JsonProcessingException e){throw conflict("Invalid expansion evidence.");}}
    private Map<String,Object> configuration(String raw){try{return mapper.readValue(Objects.toString(raw,"{}"),new TypeReference<>(){});}catch(Exception e){throw conflict("Stored cluster configuration is unreadable.");}}
    private static String endpoint(String host,int port){return (host.contains(":")?"["+host+"]":host)+":"+port;}
    private static ResponseStatusException conflict(String reason){return new ResponseStatusException(HttpStatus.CONFLICT,reason);}
}
