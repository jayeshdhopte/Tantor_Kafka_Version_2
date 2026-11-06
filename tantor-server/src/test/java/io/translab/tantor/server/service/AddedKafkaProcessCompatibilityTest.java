package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class AddedKafkaProcessCompatibilityTest {
    ClusterServiceAssignment process(String role,int id,String unit){var service=new ClusterServiceAssignment();service.setHostId("candidate");service.setRole(role);service.setNodeId(id);service.setConfigJson(unit==null?"{}":"{\"service_name\":\""+unit+"\"}");return service;}
    @Test void newBrokerAndControllerRemainSeparateRestartTargetsOnOneHost(){var cluster=new Cluster();cluster.setServices(List.of(process("controller",102,"tantor-kafka-controller-102.service"),process("broker",2,"tantor-kafka-broker-2")));assertEquals(List.of("candidate:tantor-kafka-controller-102","candidate:tantor-kafka-broker-2"),RollingRestartPlan.topology(cluster).stream().map(RollingRestartPlan::key).toList());assertEquals(1,RollingRestartPlan.select(cluster,"BROKERS",List.of()).size());assertEquals("tantor-kafka-broker-2",RollingRestartPlan.unit(RollingRestartPlan.select(cluster,"BROKERS",List.of()).getFirst()));}
    @Test void existingUnannotatedManagedProcessesRetainTheirOriginalUnitNames(){assertEquals("broker",RollingRestartPlan.unit(process("broker",1,null)));assertEquals("controller",RollingRestartPlan.unit(process("controller",101,null)));assertEquals("kafka",RollingRestartPlan.unit(process("broker_controller",1,null)));}
    @Test void invalidRecordedUnitIsRejectedInsteadOfRestartingAnotherService(){assertThrows(IllegalArgumentException.class,()->RollingRestartPlan.unit(process("broker",2,"../broker")));assertThrows(IllegalArgumentException.class,()->RollingRestartPlan.recordedUnit("broker;reboot"));}
}
