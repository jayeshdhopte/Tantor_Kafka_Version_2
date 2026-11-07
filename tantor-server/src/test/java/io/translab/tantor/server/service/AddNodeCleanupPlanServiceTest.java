package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AddNodeCleanupPlanServiceTest {
    final BrokerAdditionOperationRepository brokers=mock(BrokerAdditionOperationRepository.class);final KafkaExpansionOperationRepository expansions=mock(KafkaExpansionOperationRepository.class);final ClusterServiceAssignmentRepository assignments=mock(ClusterServiceAssignmentRepository.class);final ObjectMapper mapper=new ObjectMapper().findAndRegisterModules();final AddNodeCleanupPlanService service=new AddNodeCleanupPlanService(brokers,expansions,assignments,mapper);
    @Test void legacyHostKeepsExistingCleanupPath(){assertTrue(service.resolve(UUID.randomUUID(),"legacy").isEmpty());}
    @Test void missingAdditionOwnershipBlocksDeletionRatherThanLeavingOrphanUnits(){var cluster=UUID.randomUUID();var assignment=new ClusterServiceAssignment();assignment.setHostId("candidate");assignment.setRole("broker");assignment.setConfigJson("{\"config_path\":\"/etc/tantor-kafka/add-broker/op/broker.properties\"}");when(assignments.findByClusterId(cluster)).thenReturn(List.of(assignment));assertThrows(IllegalStateException.class,()->service.resolve(cluster,"candidate"));}
    @Test void separateCleanupBindsBothPublishedRolesToTheExactFrozenOperation()throws Exception{
        var frozen=new KafkaExpansionPlanTest().plan().freeze(mapper);var p=frozen.plan();var op=new KafkaExpansionOperation();op.setId(p.operationId());op.setPlan(frozen.raw());op.setPlanDigest(frozen.digest());when(expansions.findByClusterIdAndHostIdAndState(p.clusterId(),p.candidateHostId(),KafkaExpansionState.SUCCEEDED)).thenReturn(List.of(op));var published=new ArrayList<ClusterServiceAssignment>();for(var role:p.processes()){var assignment=new ClusterServiceAssignment();assignment.setHostId(p.candidateHostId());assignment.setRole(role.role());assignment.setNodeId(role.nodeId());assignment.setConfigJson(mapper.writeValueAsString(Map.of("service_name",p.unit(role.role()),"config_path",p.configPath(role.role()))));published.add(assignment);}when(assignments.findByClusterId(p.clusterId())).thenReturn(published);
        var cleanup=service.resolve(p.clusterId(),p.candidateHostId()).orElseThrow();assertEquals(2,cleanup.processes().size());assertEquals(p.operationId(),cleanup.processes().getFirst().operationId());assertEquals(p.configPath("broker"),cleanup.processes().get(1).configPath());assertEquals(p.process("controller").dataPaths(),cleanup.processes().getFirst().dataPaths());
        published.getFirst().setConfigJson("{\"service_name\":\"controller\",\"config_path\":\"/opt/kafka/config/controller.properties\"}");assertThrows(IllegalStateException.class,()->service.resolve(p.clusterId(),p.candidateHostId()));
    }
    @Test void brokerCleanupIncludesRuntimeBoundDigestAndKeepsLegacyFilename()throws Exception{
        for(String schema:List.of("add-broker/v1","add-broker/v2")){
            UUID cluster=UUID.randomUUID(),id=UUID.randomUUID();var p=new BrokerAdditionPlan(schema,id,"candidate","a".repeat(64),"abcdefghijklmnopqrstuv",2,"192.0.2.3",9092,"4.3.0","root","/opt/java","/srv/parcels","b".repeat(64),List.of("/data/broker"),"","/var/log/broker","/opt/install","1g","DYNAMIC",List.of("192.0.2.2:9093"),"","");
            var runtime=new BrokerAdditionLedger.RuntimeOptions(null,null,0,100000);String properties="process.roles=broker\nnode.id=2\n";String digest=BrokerAdditionLedger.hash(mapper.writeValueAsString(p)+"\n"+properties+"\n"+mapper.writeValueAsString(runtime));var payload=new BrokerAdditionLedger.Payload(p,properties,digest,"baseline",runtime);var op=new BrokerAdditionOperation();op.setId(id);op.setNodeId(2);op.setPlan(mapper.writeValueAsString(payload));when(brokers.findByClusterIdAndHostIdAndState(cluster,"candidate",BrokerAdditionState.SUCCEEDED)).thenReturn(List.of(op));
            var assignment=new ClusterServiceAssignment();assignment.setHostId("candidate");assignment.setRole("broker");assignment.setNodeId(2);String file=schema.endsWith("v1")?"server.properties":"broker.properties";assignment.setConfigJson(mapper.writeValueAsString(Map.of("service_name","tantor-kafka-broker-2","config_path","/etc/tantor-kafka/add-broker/"+id+"/"+file)));when(assignments.findByClusterId(cluster)).thenReturn(List.of(assignment));
            var cleanup=service.resolve(cluster,"candidate").orElseThrow().processes().getFirst();assertEquals(digest,cleanup.planDigest());assertTrue(cleanup.configPath().endsWith("/"+file));
        }
    }
}
