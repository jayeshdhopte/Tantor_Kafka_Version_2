package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Transactional ownership/checkpoint boundary; never awaits an agent inside a transaction. */
@Service @RequiredArgsConstructor
public class BrokerAdditionLedger {
    private final BrokerAdditionOperationRepository operations;
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final ClusterServiceAssignmentRepository assignments;
    private final JobStepRepository steps;
    private final TaskRepository tasks;
    private final JobService jobs;
    private final BrokerAdditionPlanningService planning;
    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;
    @Value("${tantor.add-node.execution-enabled:false}") private boolean executionEnabled;

    public record RuntimeOptions(String jmxArtifactId,String jmxChecksum,int jmxPort,int limitNofile) {}
    public record Payload(BrokerAdditionPlan plan,String properties,String digest,String voterBaseline,RuntimeOptions runtime) {}
    public record Accepted(UUID operationId,UUID jobId,String state) {}

    @Transactional
    public Accepted accept(UUID clusterId,UUID previewId,String contextHash,String requestKey) {
        if(!executionEnabled) throw new ResponseStatusException(HttpStatus.CONFLICT,"Broker execution is held until the collective release gates pass.");
        if(requestKey==null||!requestKey.matches("[A-Za-z0-9_-]{16,128}")||contextHash==null||!contextHash.matches("[a-f0-9]{64}"))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"A valid idempotency key and reviewed context are required.");
        clusters.findForServiceUpdate(clusterId).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Cluster not found."));
        String requestDigest=hash(previewId+":"+contextHash);
        var prior=operations.findByClusterIdAndRequestKey(clusterId,requestKey);
        if(prior.isPresent()) {
            var op=prior.get();if(!requestDigest.equals(op.getRequestDigest())) throw new ResponseStatusException(HttpStatus.CONFLICT,"Idempotency key already belongs to different reviewed inputs.");
            return new Accepted(op.getId(),op.getJobId(),op.getState().name());
        }
        var review=planning.review(clusterId,previewId);
        if(!contextHash.equals(review.contextHash())) throw new ResponseStatusException(HttpStatus.CONFLICT,"The reviewed context changed. Run checks again.");
        var plan=review.proposedPlan().forOperation(UUID.randomUUID());
        jdbc.queryForList("SELECT id FROM kf_hosts WHERE id=? FOR UPDATE",plan.hostId());
        var host=hosts.findById(plan.hostId()).orElseThrow();
        if(host.getClusterId()!=null || assignments.findReservationsForLiveClusters().stream().anyMatch(s->plan.hostId().equals(s.getHostId())))
            throw new ResponseStatusException(HttpStatus.CONFLICT,"Candidate host is already assigned.");
        Job job=new Job();job.setType(JobType.BROKER_ADDITION);job.setResourceKey("cluster:"+clusterId);job.setPayload(json(Map.of("operationId",plan.operationId(),"clusterId",clusterId)));job.setRollbackSupported(false);
        var jobSteps=new ArrayList<JobStep>();
        for(String name:List.of("Final broker validation","Prepare candidate broker","Format candidate storage","Start candidate broker","Verify live broker","Publish verified membership")) {
            JobStep step=new JobStep();step.setStepOrder(jobSteps.size()+1);step.setName(name);step.setTargetId(plan.hostId());jobSteps.add(step);
        }
        job=jobs.createJob(job,jobSteps);
        var op=new BrokerAdditionOperation();op.setId(plan.operationId());op.setClusterId(clusterId);op.setHostId(plan.hostId());op.setJobId(job.getId());op.setRequestKey(requestKey);op.setRequestDigest(requestDigest);op.setNodeId(plan.nodeId());op.setPlan(json(plan));
        operations.saveAndFlush(op);
        return new Accepted(op.getId(),op.getJobId(),op.getState().name());
    }

    public BrokerAdditionOperation forJob(UUID jobId) {return operations.findByJobId(jobId).orElseThrow(()->new IllegalArgumentException("Broker operation not found."));}
    public BrokerAdditionOperation get(UUID clusterId,UUID operationId) {return operations.findById(operationId).filter(o->clusterId.equals(o.getClusterId())).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Broker operation not found for this cluster."));}
    @Transactional
    public Accepted cancelSafe(UUID clusterId,UUID operationId) {
        clusters.findForServiceUpdate(clusterId).orElseThrow();
        var op=operations.findForUpdate(operationId).filter(o->clusterId.equals(o.getClusterId())).orElseThrow();
        if(op.getState()==BrokerAdditionState.CANCELLED_SAFE)return new Accepted(op.getId(),op.getJobId(),op.getState().name());
        if(!op.getState().reserved())throw new ResponseStatusException(HttpStatus.CONFLICT,"A completed broker cannot be cancelled.");
        Integer mutations=jdbc.queryForObject("SELECT count(*) FROM kf_broker_addition_action WHERE operation_id=? AND command IN ('PREPARE_ADD_BROKER','FORMAT_ADD_BROKER','START_ADD_BROKER')",Integer.class,op.getId());
        if(mutations==null||mutations!=0)throw new ResponseStatusException(HttpStatus.CONFLICT,"Candidate mutation was dispatched; preserve its reservation and reconcile/resume instead of cancelling.");
        op.setState(BrokerAdditionState.CANCELLED_SAFE);op.setLastError("Cancelled before any candidate mutation was dispatched.");operations.saveAndFlush(op);
        jobs.updateJobStatus(op.getJobId(),JobStatus.FAILED);jobs.appendLog(op.getJobId(),op.getLastError());
        return new Accepted(op.getId(),op.getJobId(),op.getState().name());
    }
    public Payload payload(BrokerAdditionOperation op) {try{return mapper.readValue(op.getPlan(),Payload.class);}catch(Exception e){throw new IllegalStateException("Frozen broker payload not available.",e);}}
    public BrokerAdditionPlan proposed(BrokerAdditionOperation op) {try{var tree=mapper.readTree(op.getPlan());return mapper.treeToValue(tree.has("plan")?tree.path("plan"):tree,BrokerAdditionPlan.class);}catch(Exception e){throw new IllegalStateException("Proposed broker plan not available.",e);}}

    @Transactional
    public void validating(UUID id) {var op=owned(id);op.getState().requireAdvanceTo(BrokerAdditionState.VALIDATING);op.setState(BrokerAdditionState.VALIDATING);operations.save(op);}
    @Transactional
    public void freeze(UUID id,Payload payload) {
        var op=owned(id);if(op.getState()!=BrokerAdditionState.VALIDATING)throw new IllegalStateException("Final validation ownership is required.");
        if(!op.getId().equals(payload.plan().operationId())||!op.getHostId().equals(payload.plan().hostId())||op.getNodeId()!=payload.plan().nodeId())throw new IllegalStateException("Frozen operation identity mismatch.");
        payload.plan().validate();op.setPlan(json(payload));op.setLastError(null);operations.save(op);
    }

    @Transactional
    public Task dispatch(UUID id,UUID stepId,String command,BrokerAdditionState intent) {
        var op=owned(id);var step=steps.findById(stepId).orElseThrow();
        if(!op.getJobId().equals(step.getJob().getId()))throw new IllegalStateException("Foreign broker step.");
        if(!Set.of("PREPARE_ADD_BROKER","FORMAT_ADD_BROKER","START_ADD_BROKER","INSPECT_ADD_BROKER","VALIDATE_ADD_BROKER_PLAN").contains(command))throw new IllegalArgumentException("Unsupported broker action.");
        if(step.getAgentTaskId()!=null) {
            var previous=tasks.findById(step.getAgentTaskId()).orElseThrow();
            if(!Set.of("SUCCESS","FAILED","CANCELLED").contains(previous.getStatus()))return previous;
            if("SUCCESS".equals(previous.getStatus())&&command.equals(previous.getCommand())&&!Set.of("INSPECT_ADD_BROKER","VALIDATE_ADD_BROKER_PLAN").contains(command))return previous;
            // Failed mutation is never replaced directly; a fresh successful inspection is mandatory.
            if(!Set.of("INSPECT_ADD_BROKER","VALIDATE_ADD_BROKER_PLAN").contains(command) && !Set.of("INSPECT_ADD_BROKER","INSPECT_ADD_NODE","VALIDATE_ADD_BROKER_PLAN").contains(previous.getCommand()))throw new IllegalStateException("Reconcile the prior action before retrying mutation.");
        }
        if(!Set.of("INSPECT_ADD_BROKER","VALIDATE_ADD_BROKER_PLAN").contains(command)) {op.getState().requireAdvanceTo(intent);op.setState(intent);}
        var payload=payload(op);
        jdbc.queryForObject("SELECT set_config('tantor.broker_addition_id',?,true)",String.class,op.getId().toString());
        Task task=new Task();task.setClusterId(op.getClusterId());task.setHostId(op.getHostId());task.setCommand(command);task.setStatus("PENDING");
        task.setParameters(json(Map.of("broker_plan",json(payload.plan()),"server_properties",payload.properties(),"plan_digest",payload.digest(),"runtime_options",json(payload.runtime()))));
        task=tasks.saveAndFlush(task);
        jdbc.update("INSERT INTO kf_broker_addition_action(operation_id,task_id,step_id,command) VALUES (?,?,?,?)",op.getId(),task.getId(),stepId,command);
        step.setAgentTaskId(task.getId());steps.saveAndFlush(step);operations.saveAndFlush(op);return task;
    }

    @Transactional
    public void checkpoint(UUID id,BrokerAdditionState state,String evidence) {
        var op=owned(id);op.getState().requireAdvanceTo(state);op.setState(state);op.setLastEvidence(evidence);op.setLastError(null);operations.saveAndFlush(op);
    }
    @Transactional
    public void recovery(UUID id,String reason) {
        var op=operations.findForUpdate(id).orElseThrow();
        if(op.getState().reserved()){op.setState(BrokerAdditionState.RECOVERY_REQUIRED);op.setLastError(reason);operations.saveAndFlush(op);}
    }
    @Transactional
    public void publish(UUID id,String evidence) {
        var op=owned(id);if(op.getState()==BrokerAdditionState.SUCCEEDED)return;
        if(op.getState()!=BrokerAdditionState.VERIFYING)throw new IllegalStateException("Live broker verification is required before publication.");
        var payload=payload(op);var plan=payload.plan();var cluster=clusters.findWithServicesById(op.getClusterId()).orElseThrow();
        if(!Objects.equals(cluster.getKafkaClusterId(),plan.kafkaClusterId()))throw new IllegalStateException("Kafka cluster identity changed.");
        jdbc.queryForObject("SELECT set_config('tantor.broker_addition_id',?,true)",String.class,op.getId().toString());
        var assignment=new ClusterServiceAssignment();assignment.setCluster(cluster);assignment.setHostId(op.getHostId());assignment.setNodeId(op.getNodeId());assignment.setRole("broker");assignment.setStatus("SUCCESS");
        String configPath="/etc/tantor-kafka/add-broker/"+op.getId()+"/server.properties";
        var serviceConfig=new LinkedHashMap<String,Object>(Map.of("service_name","tantor-kafka-broker-"+op.getNodeId(),"config_path",configPath,"data_paths",String.join(",",plan.dataPaths()),"metadata_path",plan.metadataPath(),"log_path",plan.logPath(),"install_base_path",plan.installPath(),"java_home",plan.javaHome(),"runtime_user",plan.runtimeUser(),"listener_port",plan.brokerPort()));
        if(!plan.brokerRack().isBlank())serviceConfig.put("broker.rack",plan.brokerRack());
        assignment.setConfigJson(json(serviceConfig));
        if(payload.runtime().jmxPort()>0)assignment.setJmxExporterPort(payload.runtime().jmxPort());
        assignments.saveAndFlush(assignment);
        var host=hosts.findById(op.getHostId()).orElseThrow();host.setClusterId(op.getClusterId());hosts.saveAndFlush(host);
        var nodeIds=new ArrayList<>(cluster.getNodeIds());if(!nodeIds.contains(op.getNodeId()))nodeIds.add(op.getNodeId());cluster.setNodeIds(nodeIds);
        var bootstrap=new LinkedHashSet<String>();if(cluster.getBootstrapServers()!=null)for(String seed:cluster.getBootstrapServers().split(","))if(!seed.isBlank())bootstrap.add(seed.trim());
        bootstrap.add((plan.hostIp().contains(":")?"["+plan.hostIp()+"]":plan.hostIp())+":"+plan.brokerPort());cluster.setBootstrapServers(String.join(",",bootstrap));clusters.saveAndFlush(cluster);
        op.setState(BrokerAdditionState.SUCCEEDED);op.setLastEvidence(evidence);op.setLastError(null);operations.saveAndFlush(op);
    }
    private BrokerAdditionOperation owned(UUID id) {
        var op=operations.findById(id).orElseThrow();
        jobs.assertBrokerAdditionOwnsCluster(op.getClusterId(),op.getJobId());
        return operations.findForUpdate(id).orElseThrow();
    }
    public String json(Object value){try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalStateException("Cannot encode broker evidence.",e);}}
    public static String hash(String value){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
}
