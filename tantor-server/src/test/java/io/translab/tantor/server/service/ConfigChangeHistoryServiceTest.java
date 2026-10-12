package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
class ConfigChangeHistoryServiceTest {
 @Test void historyPreservesValuesFailureStatusAndVersionOrderWithoutSensitiveFields() {
  JobRepository jobs=mock(JobRepository.class); JobStepRepository steps=mock(JobStepRepository.class);
  var service=new ConfigChangeHistoryService(jobs,steps,new ObjectMapper()); UUID cluster=UUID.randomUUID();
  Job job=new Job();job.setId(UUID.randomUUID());job.setStatus(JobStatus.FAILED);job.setRequestedBy("operator");
  JobStep step=new JobStep();step.setStatus(JobStepStatus.FAILED);
  step.setPayload("{\"operation\":\"STATIC\",\"nodeId\":1,\"hostId\":\"node174\",\"role\":\"broker\",\"changes\":{\"broker.session.timeout.ms\":\"\",\"ssl.keystore.password\":\"SECRET\"},\"expectedValues\":{\"broker.session.timeout.ms\":\"2000\"},\"configJson\":\"PRIVATE\"}");
  when(jobs.findByResourceKeyAndType(eq("cluster:"+cluster),eq(JobType.TARGETED_CONFIG_CHANGE),any())).thenReturn(new PageImpl<>(List.of(job,job,job,job,job),PageRequest.of(1,20),25));
  when(steps.findByJobIdOrderByStepOrderAsc(job.getId())).thenReturn(List.of(step));
  var result=service.history(cluster,1);assertThat(result.items().getFirst().version()).isEqualTo(5);
  var node=result.items().getFirst().nodes().getFirst(); assertThat(node.status()).isEqualTo("FAILED");assertThat(node.changes()).hasSize(1);
  assertThat(node.changes().getFirst().oldValue()).isEqualTo("2000");assertThat(node.changes().getFirst().newValue()).isEmpty();
  assertThat(result.toString()).doesNotContain("SECRET","PRIVATE");
  assertThatThrownBy(()->service.history(cluster,-1)).isInstanceOf(IllegalArgumentException.class);
 }
 @Test void deletionHistoryUsesBackedUpOverrideAndNullMeansRemoved() {
  var service=new ConfigChangeHistoryService(mock(JobRepository.class),mock(JobStepRepository.class),new ObjectMapper());
  JobStep step=new JobStep();step.setStatus(JobStepStatus.SUCCESS);
  step.setPayload("{\"removal\":true,\"nodeId\":1,\"changes\":{\"log.local.retention.ms\":null},\"expectedValues\":{\"log.local.retention.ms\":null},\"dynamicRemovalExpected\":{\"log.local.retention.ms\":{\"value\":\"0\",\"source\":\"DYNAMIC_BROKER_CONFIG\"}}}");
  var node=service.node(step);assertThat(node.mode()).isEqualTo("DELETE");assertThat(node.changes().getFirst().oldValue()).isEqualTo("0");assertThat(node.changes().getFirst().newValue()).isNull();
 }
}
