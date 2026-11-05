package io.translab.tantor.server.service;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class BrokerAdditionClusterStatusIsolationTest {
 @Test void candidateTasksNeverRewriteHealthyExistingClusterLifecycle(){
  var service=mock(AgentService.class,CALLS_REAL_METHODS);
  for(String command:List.of("INSPECT_ADD_NODE","PROBE_ADD_NODE","VALIDATE_ADD_BROKER_PLAN","INSPECT_ADD_BROKER","PREPARE_ADD_BROKER","FORMAT_ADD_BROKER","START_ADD_BROKER"))
   for(String status:List.of("IN_PROGRESS","FAILED","SUCCESS")){
    var cluster=new Cluster();cluster.setStatus("SUCCESS");var task=new Task();task.setCommand(command);task.setStatus(status);
    ReflectionTestUtils.invokeMethod(service,"updateClusterStatus",cluster,task);
    assertEquals("SUCCESS",cluster.getStatus(),command+" "+status);
   }
 }
}
