package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.util.*;

/** Deletion references completed frozen additions, never guessed unit names or paths. */
@Service @RequiredArgsConstructor
public class AddNodeCleanupPlanService {
    private final BrokerAdditionOperationRepository brokers;
    private final KafkaExpansionOperationRepository expansions;
    private final ClusterServiceAssignmentRepository assignments;
    private final ObjectMapper mapper;
    public record Process(UUID operationId,String planDigest,String hostId,String role,int nodeId,String unit,String configPath,
                          String kafkaClusterId,List<String> dataPaths,String metadataPath,String logPath,String runtimeHome) {}
    public record Plan(String schemaVersion,UUID clusterId,String hostId,List<Process> processes) {}
    public Optional<Plan> resolve(UUID clusterId,String hostId){
        var result=new ArrayList<Process>();
        try {
            for(var op:brokers.findByClusterIdAndHostIdAndState(clusterId,hostId,BrokerAdditionState.SUCCEEDED)){
                var payload=mapper.readValue(op.getPlan(),BrokerAdditionLedger.Payload.class);var p=payload.plan();p.validate();
                if(!op.getId().equals(p.operationId())||!hostId.equals(p.hostId())||op.getNodeId()!=p.nodeId()||!BrokerAdditionLedger.hash(mapper.writeValueAsString(p)+"\n"+payload.properties()+(payload.runtime()==null?"":"\n"+mapper.writeValueAsString(payload.runtime()))).equals(payload.digest()))throw new IllegalStateException("Frozen broker cleanup identity changed.");
                String file="add-broker/v2".equals(p.schemaVersion())?"broker.properties":"server.properties";
                result.add(new Process(op.getId(),payload.digest(),hostId,"broker",p.nodeId(),p.serviceName()+".service",p.configPath(),p.kafkaClusterId(),p.dataPaths(),p.metadataPath(),p.logPath(),p.parcelDir()+"/kafka/"+p.kafkaVersion()));
            }
            for(var op:expansions.findByClusterIdAndHostIdAndState(clusterId,hostId,KafkaExpansionState.SUCCEEDED)){
                var p=KafkaExpansionPlan.read(mapper,op.getPlan(),op.getPlanDigest()).plan();if(!op.getId().equals(p.operationId())||!clusterId.equals(p.clusterId())||!hostId.equals(p.candidateHostId()))throw new IllegalStateException("Frozen expansion cleanup identity changed.");
                for(var process:p.processes())result.add(new Process(op.getId(),op.getPlanDigest(),hostId,process.role(),process.nodeId(),p.unit(process.role()),p.configPath(process.role()),p.kafkaClusterId(),process.dataPaths(),process.metadataPath(),process.logPath(),p.parcelDir()+"/kafka/"+p.kafkaVersion()));
            }
            var services=assignments.findByClusterId(clusterId).stream().filter(s->hostId.equals(s.getHostId())&&s.getRole()!=null&&(s.getRole().contains("broker")||s.getRole().contains("controller"))).toList();
            if(result.isEmpty()){
                for(var service:services)if(Objects.toString(service.getConfigJson(),"").matches("(?s).*(/etc/tantor-kafka/add-|add_node_operation_id).*"))throw new IllegalStateException("Added-node cleanup ownership is missing; deletion is blocked.");
                return Optional.empty();
            }
            if(result.size()>2||services.size()!=result.size()||result.stream().map(Process::unit).distinct().count()!=result.size())throw new IllegalStateException("Ambiguous added-node cleanup scope.");
            for(var process:result){var matches=services.stream().filter(s->process.role().equals(s.getRole())&&Objects.equals(process.nodeId(),s.getNodeId())).toList();if(matches.size()!=1)throw new IllegalStateException("Cleanup membership differs from the completed addition.");var config=mapper.readTree(matches.getFirst().getConfigJson());if(!process.configPath().equals(config.path("config_path").asText())||!process.unit().equals(config.path("service_name").asText().replaceFirst("\\.service$","")+".service"))throw new IllegalStateException("Added-node service references changed.");}
            return Optional.of(new Plan("add-node-cleanup/v1",clusterId,hostId,List.copyOf(result)));
        }catch(com.fasterxml.jackson.core.JsonProcessingException e){throw new IllegalStateException("Frozen added-node cleanup plan is unreadable.",e);}
    }
}
