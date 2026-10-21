package io.translab.tantor.server.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExternalServiceTopicReadinessTest {
 private final String name="connect-offsets";
 private final AdminClient admin=mock(AdminClient.class);
 private final DescribeTopicsResult topics=mock(DescribeTopicsResult.class);
 private final DescribeConfigsResult configs=mock(DescribeConfigsResult.class);
 private final ConfigResource resource=new ConfigResource(ConfigResource.Type.TOPIC,name);
 private Map<String,TopicDescription> metadata(int rf){
  var replicas=new ArrayList<Node>();for(int i=0;i<rf;i++)replicas.add(new Node(i,"broker"+i,9092));
  return Map.of(name,new TopicDescription(name,false,List.of(new TopicPartitionInfo(0,replicas.get(0),replicas,replicas))));
 }
 private static <T> KafkaFuture<T> failed(Throwable error){var future=new KafkaFutureImpl<T>();future.completeExceptionally(error);return future;}
 private void ready(int rf,String policy){
  when(admin.describeTopics(anyCollection())).thenReturn(topics);when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(metadata(rf)));
  when(admin.describeConfigs(anyCollection())).thenReturn(configs);when(configs.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource,new Config(List.of(new ConfigEntry("cleanup.policy",policy))))));
 }
 @Test void waitsForBrokerConfigMetadataAfterSuccessfulTopicDescription() throws Exception {
  ready(3,"compact");when(configs.all()).thenReturn(failed(new UnknownTopicOrPartitionException()),KafkaFuture.completedFuture(Map.of(resource,new Config(List.of(new ConfigEntry("cleanup.policy","compact"))))));
  ExternalServicesJobHandler.verifyTopic(admin,name,1,3,Duration.ofSeconds(2),Duration.ZERO);
  verify(admin,times(2)).describeConfigs(anyCollection());verify(admin,never()).createTopics(anyCollection());
 }
 @Test void waitsForTopicDescriptionPropagation() throws Exception {
  ready(3,"compact");when(topics.allTopicNames()).thenReturn(failed(new UnknownTopicOrPartitionException()),KafkaFuture.completedFuture(metadata(3)));
  ExternalServicesJobHandler.verifyTopic(admin,name,1,3,Duration.ofSeconds(2),Duration.ZERO);
  verify(admin,times(2)).describeTopics(anyCollection());
 }
 @Test void doesNotHideInvalidReplicationOrCompaction(){
  ready(1,"compact");assertThatThrownBy(()->ExternalServicesJobHandler.verifyTopic(admin,name,1,3,Duration.ofSeconds(1),Duration.ZERO)).hasMessageContaining("incompatible partitions or replication");
  ready(3,"delete");assertThatThrownBy(()->ExternalServicesJobHandler.verifyTopic(admin,name,1,3,Duration.ofSeconds(1),Duration.ZERO)).hasMessageContaining("cleanup.policy=compact");
 }
 @Test void authorizationFailuresDoNotRetry(){
  ready(3,"compact");when(configs.all()).thenReturn(failed(new TopicAuthorizationException(Set.of(name))));
  assertThatThrownBy(()->ExternalServicesJobHandler.verifyTopic(admin,name,1,3,Duration.ofSeconds(1),Duration.ZERO)).hasMessageContaining(name).hasMessageContaining("TopicAuthorizationException");
  verify(admin,times(1)).describeConfigs(anyCollection());
 }
 @Test void missingMetadataFailsWithinBoundedWaitWithTopicName(){
  ready(3,"compact");when(configs.all()).thenReturn(failed(new UnknownTopicOrPartitionException()));
  assertThatThrownBy(()->ExternalServicesJobHandler.verifyTopic(admin,name,1,3,Duration.ofMillis(20),Duration.ofMillis(1))).hasMessageContaining("Timed out").hasMessageContaining(name);
 }
}
