package io.translab.tantor.server.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.stream.Collectors;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KafkaInternalTopicVisibilityTest {
    final AdminClient admin=mock(AdminClient.class);
    final UUID cluster=UUID.randomUUID();
    final KafkaAdminService service=spy(new KafkaAdminService(null,null,null,null,null,null));
    final Set<String> names=Set.of("orders","_customer_topic","_schemas","_schema_encoders","_dek_registry_keys","connect-configs","__consumer_offsets");

    KafkaInternalTopicVisibilityTest() throws Exception {
        doReturn(admin).when(service).getAdminClient(cluster);
        when(admin.listTopics(any(ListTopicsOptions.class))).thenAnswer(call->{
            ListTopicsOptions options=call.getArgument(0);
            var result=mock(ListTopicsResult.class);
            when(result.names()).thenReturn(KafkaFuture.completedFuture(names.stream().filter(n->options.shouldListInternal()||!n.startsWith("__")).collect(Collectors.toSet())));
            return result;
        });
        when(admin.describeTopics(anyCollection())).thenAnswer(call->{
            Collection<String> requested=call.getArgument(0);
            var result=mock(DescribeTopicsResult.class);
            when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(requested.stream().collect(Collectors.toMap(n->n,n->new TopicDescription(n,n.startsWith("__"),List.of())))));
            return result;
        });
        var offsets=mock(ListOffsetsResult.class);
        when(offsets.all()).thenReturn(KafkaFuture.completedFuture(Map.<TopicPartition,ListOffsetsResult.ListOffsetsResultInfo>of()));
        when(admin.listOffsets(anyMap())).thenReturn(offsets);
    }

    @Test void toggleOffFiltersBeforePaginationAndCounts() {
        var page=service.listTopicsPaginated(cluster,0,1,"","name",false);
        assertThat(page.getTotalElements()).isEqualTo(2);
        assertThat(page.getTotalPages()).isEqualTo(2);
        assertThat(page.getContent()).extracting(t->t.getName()).containsExactly("_customer_topic");
        assertThat(page.getContent().getFirst().isInternal()).isFalse();
        assertThat(service.listTopicsPaginated(cluster,0,10,"schema","name",false).getContent()).isEmpty();
    }

    @Test void toggleOnIncludesAndMarksPlatformAndBrokerInternalTopics() {
        var page=service.listTopicsPaginated(cluster,0,100,"","name",true);
        assertThat(page.getTotalElements()).isEqualTo(names.size());
        for(var topic:page.getContent()) {
            assertThat(topic.isInternal()).as(topic.getName()).isEqualTo(!Set.of("orders","_customer_topic").contains(topic.getName()));
        }
        assertThat(service.listTopicsPaginated(cluster,0,10,"dek","name",true).getContent()).extracting(t->t.getName()).containsExactly("_dek_registry_keys");
    }
}
