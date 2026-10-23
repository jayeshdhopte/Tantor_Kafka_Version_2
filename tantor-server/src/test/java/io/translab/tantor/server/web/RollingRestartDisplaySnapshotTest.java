package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RollingRestartDisplaySnapshotTest {
 @Test void jobSavesHumanLabelsWithoutReplacingExternalDispatchTargets() {
  var clusters=mock(ClusterRepository.class);var jobs=mock(JobService.class);var display=mock(RestartNodeDisplayService.class);var auth=mock(RoleAuthenticationUtil.class);var deployment=mock(DeploymentService.class);
  var controller=new ClusterActionsController(deployment,clusters,jobs,new ObjectMapper(),mock(AuditService.class),auth,display);
  UUID id=UUID.randomUUID();String target="external:"+UUID.randomUUID();String second="external:"+UUID.randomUUID();
  var cluster=new Cluster();cluster.setId(id);cluster.setMode("kraft");cluster.setStatus("SUCCESS");
  var one=new ClusterServiceAssignment();one.setHostId(target);one.setRole("broker");one.setNodeId(1);
  var two=new ClusterServiceAssignment();two.setHostId(second);two.setRole("broker");two.setNodeId(2);cluster.setServices(List.of(one,two));
  one.setConfigJson("{\"service_name\":\"broker.service\"}");two.setConfigJson("{\"service_name\":\"broker.service\"}");
  when(clusters.findWithServicesById(id)).thenReturn(Optional.of(cluster));when(auth.canAccess(any(),anyString())).thenReturn(true);
  when(display.identity(target)).thenReturn(Map.of("hostname","node229.translab.io","ipAddress","192.168.3.229"));
  when(display.identity(second)).thenReturn(Map.of("hostname","node174.translab.io","ipAddress","192.168.3.174"));
  when(jobs.createJob(any(),anyList())).thenAnswer(call->{Job job=call.getArgument(0);job.setId(UUID.randomUUID());return job;});
  var response=controller.startRollingRestart("token",id,null);
  assertThat(response.getStatusCode().value()).isEqualTo(200);
  verify(jobs).createJob(argThat(job->job.getType()==JobType.ROLLING_RESTART),argThat(steps->steps.size()==2 && steps.getFirst().getTargetId().equals(target)
   && steps.getFirst().getName().equals("Restart broker node 1 - node229.translab.io (192.168.3.229)")
   && steps.getFirst().getPayload().contains(target) && steps.getFirst().getPayload().contains("hostname") && steps.getFirst().getPayload().contains("ipAddress")));
  verifyNoInteractions(deployment);
 }
}
