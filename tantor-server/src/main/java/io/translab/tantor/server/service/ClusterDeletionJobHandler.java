package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.audit.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.util.*;

@Service @RequiredArgsConstructor
public class ClusterDeletionJobHandler implements JobHandler {
    private final JobService jobs;
    private final TaskRepository tasks;
    private final HostRepository hosts;
    private final AgentTaskAwaiter awaiter;
    private final AuditService audit;
    private final AuditLogRepository auditLogs;
    private final ObjectMapper mapper;
    @Override public boolean supports(JobType type) {return type==JobType.CLUSTER_DELETION;}
    @Override public void execute(Job job) {
        Map<String,Object> details=read(job.getPayload());UUID clusterId=UUID.fromString(String.valueOf(details.get("clusterId")));
        List<String> failures=new ArrayList<>();
        for (JobStep step : jobs.getSteps(job.getId())) {
            if (step.getStatus()==JobStepStatus.SUCCESS) continue;
            try {
                if (step.getAgentTaskId()==null) throw new IllegalStateException("Deletion step has no linked cleanup task; cleanup cannot be inferred.");
                Task task=tasks.findById(step.getAgentTaskId()).orElseThrow(()->new IllegalStateException("Linked cleanup task is unavailable."));
                if (!"DELETE_CLUSTER".equals(task.getCommand()) || !Objects.equals(task.getHostId(),step.getTargetId())) throw new IllegalStateException("Invalid linked cleanup task.");
                if (task.getClusterId()!=null && !clusterId.equals(task.getClusterId())) throw new IllegalStateException("Cleanup task belongs to another cluster.");
                if (Set.of("FAILED","CANCELLED").contains(Objects.toString(task.getStatus(),""))) {
                    if (step.getRetryCount()==null || step.getRetryCount()==0) throw new IllegalStateException("Agent cleanup failed: " + Objects.toString(task.getErrorMsg(), Objects.toString(task.getLogOutput(), "No agent cleanup details were reported.")));
                    var host=hosts.findById(task.getHostId());
                    if (host.isPresent() && host.get().getClusterId()!=null && !clusterId.equals(host.get().getClusterId())) throw new IllegalStateException("Host belongs to another cluster; previous deletion must not be retried there.");
                    Task retry=new Task();retry.setHostId(task.getHostId());retry.setClusterId(task.getClusterId());retry.setCommand("DELETE_CLUSTER");retry.setParameters(task.getParameters());retry.setStatus("PENDING");retry=tasks.save(retry);
                    jobs.attachAgentTask(step.getId(),retry.getId());task=retry;
                }
                if (step.getStatus()!=JobStepStatus.IN_PROGRESS) jobs.startStep(step.getId());
                Task result=awaiter.await(task.getId());jobs.completeStep(step.getId(),result.getLogOutput());
            } catch (RuntimeException e) {
                jobs.failStep(step.getId(),e.getMessage());failures.add(step.getName()+": "+e.getMessage());
                if (Thread.currentThread().isInterrupted()) throw e;
            }
        }
        boolean success=failures.isEmpty();details.put("jobId",job.getId().toString());details.put("attempt",job.getRetryCount()==null?0:job.getRetryCount());details.put("failures",failures);
        String action=success ? Boolean.TRUE.equals(details.get("force")) ? "CLUSTER_FORCE_DELETE_CLEANUP_COMPLETED" : "CLUSTER_DELETED" : "CLUSTER_DELETION_FAILED";
        if (!auditLogs.existsDeletionEvent(action,job.getId().toString(),((Number)details.get("attempt")).intValue()))
            audit.recordAs(job.getRequestedBy(),"MANAGEMENT_SERVER",null,"CLUSTER_CHANGE",action,"CLUSTER",clusterId.toString(),clusterId,success?"SUCCESS":"FAILED",null,null,null,details);
        if (!success) throw new IllegalStateException("Cluster cleanup failed on " + failures.size() + " host(s). See the failed steps; completed host cleanup is preserved.");
    }
    private Map<String,Object> read(String value) {try{return mapper.readValue(value,new com.fasterxml.jackson.core.type.TypeReference<>(){});}catch(Exception e){throw new IllegalArgumentException("Invalid deletion job history",e);}}
    @Override public void rollback(Job job) {throw new UnsupportedOperationException("Cluster deletion cannot be rolled back.");}
}
