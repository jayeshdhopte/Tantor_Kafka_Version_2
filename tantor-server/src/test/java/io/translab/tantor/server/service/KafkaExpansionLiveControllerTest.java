package io.translab.tantor.server.service;

import io.translab.tantor.server.config.AddNodeInspectionProperties;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Explicitly enabled, read-only compatibility check using the production Admin adapter. */
class KafkaExpansionLiveControllerTest {
    @Test void typedControllerApisWorkAgainstTheNamedExistingController(){
        String endpoint=System.getenv("TANTOR_ADD_NODE_LIVE_CONTROLLER");
        assumeTrue(endpoint!=null&&!endpoint.isBlank(),"No explicit live controller probe requested");
        assertTrue(List.of("192.168.3.191:9093","192.168.3.229:9093").contains(endpoint),"Probe outside authorized test hosts");
        var snapshot=new KafkaExpansionQuorum(new AddNodeInspectionProperties()).observe(List.of(endpoint));
        assertNotNull(snapshot.clusterId());assertTrue(snapshot.clusterId().matches("[A-Za-z0-9_-]{22}"));
        assertTrue(snapshot.voters().containsKey(snapshot.leaderId()));assertTrue(snapshot.highWatermark()>=0);
        assertTrue(snapshot.features().containsKey("metadata.version"));
        System.out.printf("Read-only controller API verified: leader=%d, voters=%s, observers=%s, features=%s%n",snapshot.leaderId(),snapshot.voters().keySet(),snapshot.observers().keySet(),snapshot.features());
    }
}
