package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.*;

@Service @RequiredArgsConstructor
public class ClusterDeletionHistoryService {
    private final JobService jobs;
    private final DeploymentService deployment;
    private final HostRepository hosts;
    private final ObjectMapper mapper;

    @Transactional
    public UUID queue(Cluster cluster, String actor, boolean force) {
        List<JobStep> steps = new ArrayList<>();
        var grouped = new LinkedHashMap<String, List<ClusterServiceAssignment>>();
        for (var service : cluster.getServices()) grouped.computeIfAbsent(service.getHostId(), ignored -> new ArrayList<>()).add(service);
        for (var entry : grouped.entrySet()) {
            String hostId = entry.getKey(); var host = hosts.findById(hostId);
            String hostname = host.map(Host::getHostname).orElse(hostId), ip = host.map(Host::getHostIp).orElse("IP not reported");
            JobStep step = new JobStep(); step.setStepOrder(steps.size()+1); step.setTargetId(hostId);
            var nodeIds = entry.getValue().stream().map(ClusterServiceAssignment::getNodeId).filter(Objects::nonNull).distinct().toList();
            step.setName("Clean cluster services" + (nodeIds.isEmpty() ? "" : " for nodes " + nodeIds) + " on " + hostname + " (" + ip + ")");
            step.setPayload(json(Map.of("hostId",hostId,"hostname",hostname,"ipAddress",ip,"nodeIds",nodeIds)));
            steps.add(step);
        }
        Job job = job(cluster.getId(), cluster.getName(), cluster.getKafkaClusterId(), cluster.getKafkaVersion(), cluster.getMode(), actor, force);
        var snapshot = read(job.getPayload()); snapshot.put("cleanupHosts", steps.stream().map(step -> read(step.getPayload())).toList());job.setPayload(json(snapshot));
        Job saved = jobs.createJob(job, steps);
        for (JobStep step : steps) {
            List<Map<String,Object>> data = new ArrayList<>(); boolean kafka = false;
            for (var service : grouped.get(step.getTargetId())) {
                if (Set.of("schema_registry", "kafka_connect").contains(service.getRole())) {
                    Map<String,Object> config = read(service.getConfigJson());config.put("kind",service.getRole());data.add(config);
                } else kafka = true;
            }
            UUID task = deployment.deleteClusterFromHost(cluster.getId(),step.getTargetId(),cluster.getKafkaVersion(),cluster.getConfigJson(),data,kafka);
            jobs.attachAgentTask(step.getId(),task);
        }
        jobs.appendLog(saved.getId(),"Deletion requested by " + actor + " for cluster '" + cluster.getName() + "'. Cleanup dispatched to " + steps.size() + " host(s).");
        return saved.getId();
    }

    @Transactional
    public UUID completed(UUID id, String name, String kafkaId, String version, String mode, String actor, boolean force, boolean external) {
        Job job = job(id,name,kafkaId,version,mode,actor,force);
        Instant now = Instant.now();job.setStatus(JobStatus.SUCCESS);job.setStartTime(now);job.setEndTime(now);
        JobStep step = new JobStep();step.setStepOrder(1);step.setTargetId(id.toString());step.setStatus(JobStepStatus.SUCCESS);step.setStartTime(now);step.setEndTime(now);
        step.setName(external ? "Remove external cluster registration and unbind discovery agents" : "Remove cluster metadata; no host cleanup required");
        step.setLogs(external ? "External registration removed. Running Kafka services were not stopped." : "Cluster metadata removed successfully.");
        job.setLogs("Deleted cluster registration '" + name + "' by " + actor + ".");
        return jobs.createJob(job,List.of(step)).getId();
    }
    private Job job(UUID id,String name,String kafkaId,String version,String mode,String actor,boolean force) {
        Job job=new Job();job.setType(JobType.CLUSTER_DELETION);job.setResourceKey("cluster:"+id);job.setRequestedBy(actor);job.setRollbackSupported(false);
        Map<String,Object> snapshot=new LinkedHashMap<>();snapshot.put("clusterId",id.toString());snapshot.put("clusterName",name);snapshot.put("kafkaClusterId",kafkaId);snapshot.put("kafkaVersion",version);snapshot.put("mode",mode);snapshot.put("force",force);job.setPayload(json(snapshot));return job;
    }
    private Map<String,Object> read(String value) {try{return value==null?new LinkedHashMap<>():mapper.readValue(value,new com.fasterxml.jackson.core.type.TypeReference<>(){});}catch(Exception e){throw new IllegalArgumentException("Invalid saved cleanup configuration",e);}}
    private String json(Object value) {try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("Unable to save deletion history",e);}}
}
