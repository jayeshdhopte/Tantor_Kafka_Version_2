package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.audit.AuditService;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class TopicMessageMaskingIntegrationTest {
 @Test @SuppressWarnings({"rawtypes","unchecked"})
 void topicApiNeverReturnsOrSearchesMaskedValuesAndBrowsingNeverJoinsAGroup(){
  UUID id=UUID.randomUUID();var repo=mock(ExternalClusterRepository.class);var cluster=new ExternalCluster();
  cluster.setConnectionSettings("{\"masking\":[{\"type\":\"MASK\",\"fields\":[\"password\"],\"topicValuesPattern\":\".*\"}]}");
  when(repo.findById(id)).thenReturn(Optional.of(cluster));var masking=new MessageMaskingService(repo,new ObjectMapper());
  var kafka=mock(KafkaAdminService.class);when(kafka.getKafkaClientProperties(id)).thenAnswer(call->{var p=new Properties();p.put("group.id","should-not-be-used");return p;});
  var service=new TopicOperationsService(kafka,mock(AuditService.class),mock(TopicSegmentMetricsService.class),masking);
  var partition=new TopicPartition("orders",0);
  try(var consumers=mockConstruction(KafkaConsumer.class,(consumer,context)->{
   Properties properties=(Properties)context.arguments().getFirst();
   assertThat(properties).doesNotContainKey("group.id").containsEntry("enable.auto.commit","false");
   when(consumer.partitionsFor(eq("orders"),any(java.time.Duration.class))).thenReturn(List.of(new PartitionInfo("orders",0,null,new Node[0],new Node[0])));
   when(consumer.beginningOffsets(anyCollection())).thenReturn(Map.of(partition,0L));when(consumer.endOffsets(anyCollection())).thenReturn(Map.of(partition,1L));
   when(consumer.position(partition)).thenReturn(1L);
   var record=new ConsumerRecord<byte[],byte[]>("orders",0,0L,null,"{\"password\":\"raw-secret\",\"id\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
   when(consumer.poll(any(java.time.Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(partition,List.of(record))));
  })) {
   var response=service.getMessages(id,"orders",List.of(),"oldest",1,null);
   assertThat(response.get("messages").toString()).contains("***").doesNotContain("raw-secret");
   assertThat(service.getMessages(id,"orders",List.of(),"oldest",1,"raw-secret").get("count")).isEqualTo(0);
   for(var consumer:consumers.constructed())verify(consumer).assign(List.of(partition));
  }
 }
}
