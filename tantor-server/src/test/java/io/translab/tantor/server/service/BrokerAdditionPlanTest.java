package io.translab.tantor.server.service;

import io.translab.tantor.server.dto.AddNodeReadiness.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BrokerAdditionPlanTest {
    private final UUID operation=UUID.randomUUID();
    private Map<String,Object> inputs() {
        var p=new HashMap<String,Object>();
        p.put("kafkaClusterId","abcdefghijklmnopqrstuv");p.put("hostIp","192.168.3.195");p.put("brokerPort","9092");
        p.put("kafkaVersion","4.1.2");p.put("runtimeUser","kafka");p.put("javaHome","/opt/java/jdk21");
        p.put("parcelDir","/opt/tantor/parcels");p.put("artifactChecksum","a".repeat(64));p.put("dataPaths",List.of("/data/kafka/broker"));
        p.put("metadataPath","/data/kafka/metadata");p.put("logPath","/var/log/kafka/broker");p.put("installPath","/opt/tantor/services/kafka");
        p.put("heapXmx","1g");p.put("quorumMode","STATIC");p.put("controllerEndpoints",List.of("192.168.3.194:9093"));p.put("securityMode","PLAINTEXT");return p;
    }
    private Preview preview(Map<String,Object> p,String state,Instant expiry,boolean complete) {
        var checks=new ArrayList<Check>();
        for(String prefix:List.of("C","B")) for(int i=1;i<=(prefix.equals("C")?21:11);i++)
            checks.add(new Check("%s%02d".formatted(prefix,i),"check",prefix.equals("C")?"COMMON":"BROKER","PASS","expected","observed","",Instant.now(),"AGENT"));
        if(!complete) checks.remove(0);
        return new Preview(UUID.randomUUID(),UUID.randomUUID(),"candidate","b".repeat(64),Instant.now(),expiry,state,checks,p,false);
    }
    private BrokerAdditionPlan freeze(Map<String,Object> p) {return BrokerAdditionPlan.freeze(preview(p,"PASS",Instant.now().plusSeconds(60),true),operation,4,"1@192.168.3.194:9093");}
    @Test void staticBrokerKeepsExistingIdentityAndVoters() {
        var plan=freeze(inputs());assertEquals(4,plan.nodeId());assertEquals(operation,plan.operationId());
        assertEquals("abcdefghijklmnopqrstuv",plan.kafkaClusterId());assertEquals("1@192.168.3.194:9093",plan.quorumVoters());
    }
    @Test void dynamicBrokerDoesNotNeedOrInventInitialControllers() {
        var p=inputs();p.put("quorumMode","DYNAMIC");
        assertEquals("DYNAMIC",BrokerAdditionPlan.freeze(preview(p,"PASS",Instant.now().plusSeconds(60),true),operation,4,"").quorumMode());
    }
    @Test void fourThreeBrokerPreservesInheritedVersionAndClusterIdentity() {
        var p=inputs();p.put("kafkaVersion","4.3.0");p.put("brokerRack","rack2");
        var plan=freeze(p);assertEquals("4.3.0",plan.kafkaVersion());assertEquals("abcdefghijklmnopqrstuv",plan.kafkaClusterId());assertEquals(4,plan.nodeId());assertEquals("rack2",plan.brokerRack());assertEquals("rack2",plan.forOperation(UUID.randomUUID()).brokerRack());
    }
    @Test void blockedExpiredAndIncompleteEvidenceCannotBecomeAPlan() {
        for(var preview:List.of(preview(inputs(),"BLOCKED",Instant.now().plusSeconds(60),true),
                preview(inputs(),"PASS",Instant.now().minusSeconds(1),true),preview(inputs(),"PASS",Instant.now().plusSeconds(60),false)))
            assertThrows(IllegalArgumentException.class,()->BrokerAdditionPlan.freeze(preview,operation,4,"1@192.168.3.194:9093"));
    }
    @Test void brokerCannotBeAControllerVoter() {
        assertThrows(IllegalArgumentException.class,()->BrokerAdditionPlan.freeze(preview(inputs(),"PASS",Instant.now().plusSeconds(60),true),operation,1,"1@192.168.3.194:9093"));
    }
    @Test void pathsAndEndpointsFailClosed() {
        for(var invalid:List.<Map.Entry<String,Object>>of(Map.entry("hostIp","999.999.999.999"),Map.entry("dataPaths",List.of("/data/../etc")),
                Map.entry("dataPaths",List.of("/opt/tantor")),Map.entry("controllerEndpoints",List.of("host:9093\nnode.id=4")),
                Map.entry("securityMode","SASL_SSL"),Map.entry("runtimeUser","kafka;id"),Map.entry("heapXmx",""))) {
            var p=inputs();p.put(invalid.getKey(),invalid.getValue());assertThrows(IllegalArgumentException.class,()->freeze(p),invalid.getKey());
        }
    }
}
