package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.*;

/** Delete only persisted Tantor service deployments, then unregister Kafka.
 * Discovery-only service paths and manually added REST connections are never
 * interpreted as ownership or as authorization to delete an installation. */
@Service @RequiredArgsConstructor
public class ExternalClusterDeletionService {
    private final JobService jobs;
    private final ExternalDataServiceRepository services;
    private final ExternalServiceTaskRepository tasks;
    private final ExternalDataServiceManager manager;
    private final ExternalClusterService external;
    private final ExternalClusterRepository clusters;
    private final ClusterRepository mirrors;
    private final ObjectMapper mapper;

    @Transactional
    public UUID queue(ExternalCluster cluster, String actor, boolean force) {
        List<ExternalDataService> owned = services.findByClusterId(cluster.getId()).stream()
                .filter(service -> !"DELETED".equals(service.getStatus())).toList();
        if (owned.isEmpty()) return null;
        Job job = new Job(); job.setType(JobType.CLUSTER_DELETION);
        job.setResourceKey("cluster:" + cluster.getId()); job.setRequestedBy(actor); job.setRollbackSupported(false);
        Map<String,Object> payload = new LinkedHashMap<>();
        payload.put("clusterId",cluster.getId().toString()); payload.put("clusterName",cluster.getName());
        payload.put("mode","EXTERNAL"); payload.put("externalCleanup",true); payload.put("force",force);
        payload.put("kafkaClusterId",cluster.getKafkaClusterId()); payload.put("kafkaVersion",cluster.getKafkaVersion());
        job.setPayload(json(payload));
        List<JobStep> steps = new ArrayList<>();
        for (var service : owned) {
            if (!ExternalDataServiceManager.ROLES.contains(service.getRole())) throw new IllegalStateException("Unsupported owned service role.");
            JobStep step = new JobStep(); step.setStepOrder(steps.size()+1); step.setTargetId(service.getAgentId());
            step.setName("Remove Tantor " + service.getRole() + " instance " + service.getId() + " on " + service.getAgentId());
            step.setPayload(json(Map.of("serviceId",service.getId().toString()))); steps.add(step);
        }
        JobStep finish = new JobStep(); finish.setStepOrder(steps.size()+1); finish.setTargetId(cluster.getId().toString());
        finish.setName("Unregister external cluster; preserve existing Kafka and shared agents");
        finish.setPayload("{\"operation\":\"UNREGISTER\"}"); steps.add(finish);
        Job saved = jobs.createJob(job,steps);
        setStatus(cluster.getId(),"DELETING");
        jobs.appendLog(saved.getId(),"External deletion will remove only Tantor-deployed Schema Registry/Connect instances and their supporting units. Existing Kafka and shared agents are preserved.");
        return saved.getId();
    }

    public List<String> execute(Job job, UUID clusterId) {
        List<String> failures = new ArrayList<>();
        List<JobStep> steps = jobs.getSteps(job.getId());
        for (JobStep step : steps) {
            if (step.getStatus()==JobStepStatus.SUCCESS) continue;
            Map<String,String> payload = manager.read(step.getPayload());
            if ("UNREGISTER".equals(payload.get("operation"))) continue;
            try {
                var service=services.findById(UUID.fromString(payload.get("serviceId"))).orElseThrow(()->new IllegalStateException("Owned deployment record is missing; cleanup cannot be inferred."));
                if (!clusterId.equals(service.getClusterId()) || !service.getAgentId().equals(step.getTargetId())) throw new IllegalStateException("Cleanup deployment belongs to another cluster or agent.");
                manager.requireCluster(clusterId); manager.requireAgent(clusterId,service.getAgentId());
                jobs.startStep(step.getId());
                ExternalServiceTask task = step.getExternalServiceTaskId()==null ? null : tasks.findById(step.getExternalServiceTaskId()).orElseThrow();
                if (task!=null && (!"CLEAN".equals(task.getCommand()) || !service.getId().equals(task.getServiceId()) || !service.getAgentId().equals(task.getAgentId()))) throw new IllegalStateException("Invalid linked external cleanup task.");
                if (task!=null && "FAILED".equals(task.getStatus())) {
                    if (step.getRetryCount()==null || step.getRetryCount()==0) throw new IllegalStateException(task.getMessage());
                    task=null;
                }
                if (task==null) {
                    task=manager.enqueue(service,"CLEAN"); jobs.attachExternalServiceTask(step.getId(),task.getId());
                }
                String output=await(task.getId());
                service.setStatus("DELETED"); service.setLastError(null); services.save(service);
                jobs.completeStep(step.getId(),output);
            } catch (Exception error) {
                if (error instanceof InterruptedException) Thread.currentThread().interrupt();
                jobs.failStep(step.getId(),error.getMessage()); failures.add(step.getName()+": "+error.getMessage());
                if (Thread.currentThread().isInterrupted()) throw new IllegalStateException("External cleanup interrupted",error);
            }
        }
        if (!failures.isEmpty()) { setStatus(clusterId,"DELETE_FAILED"); return failures; }
        for (JobStep step : steps) {
            if (step.getStatus()==JobStepStatus.SUCCESS || !"UNREGISTER".equals(manager.read(step.getPayload()).get("operation"))) continue;
            jobs.startStep(step.getId());
            try {
                external.deleteExternalCluster(clusterId);
                jobs.completeStep(step.getId(),"Tantor deployments removed and registration deleted. Existing Kafka services and shared agents remain running.");
            } catch (RuntimeException error) {
                jobs.failStep(step.getId(),error.getMessage()); failures.add(error.getMessage()); setStatus(clusterId,"DELETE_FAILED");
            }
        }
        return failures;
    }

    private String await(UUID id) throws InterruptedException {
        while (true) {
            var task=tasks.findById(id).orElseThrow();
            if ("SUCCESS".equals(task.getStatus())) return Objects.toString(task.getMessage(),"");
            if ("FAILED".equals(task.getStatus())) throw new IllegalStateException(task.getMessage());
            if (Instant.now().isAfter(task.getDeadline())) {
                manager.expire(id); throw new IllegalStateException("External cleanup task timed out. Registration and completed cleanup are preserved; reconnect the agent and use Retry.");
            }
            Thread.sleep(1000);
        }
    }
    private void setStatus(UUID id,String status) {
        clusters.findById(id).ifPresent(cluster->{cluster.setStatus(status);clusters.save(cluster);});
        mirrors.findById(id).ifPresent(cluster->{cluster.setStatus(status);mirrors.save(cluster);});
    }
    private String json(Object value) { try {return mapper.writeValueAsString(value);} catch(Exception error) {throw new IllegalArgumentException("Invalid external cleanup history",error);} }
}
