package io.translab.tantor.server.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaGroupInventoryServiceTest {
    private GroupListing group(String name,String protocol,GroupType type) {
        return new GroupListing(name,Optional.of(type),protocol,Optional.of(GroupState.STABLE));
    }
    @Test void classifiesByKafkaMetadataRatherThanNamePrefixes() {
        assertThat(KafkaGroupInventoryService.describe(group("ordinary-name","sr",GroupType.CLASSIC)).classification()).isEqualTo("SCHEMA_REGISTRY_COORDINATION");
        assertThat(KafkaGroupInventoryService.describe(group("ordinary-name","connect",GroupType.CLASSIC)).classification()).isEqualTo("CONNECT_COORDINATION");
        assertThat(KafkaGroupInventoryService.describe(group("tantor-sr-business-consumer","consumer",GroupType.CLASSIC)).classification()).isEqualTo("CONSUMER");
        assertThat(KafkaGroupInventoryService.describe(group("connect-sink-orders","consumer",GroupType.CONSUMER)).classification()).isEqualTo("CONSUMER");
        assertThat(KafkaGroupInventoryService.describe(group("inactive-consumer","",GroupType.CLASSIC)).classification()).isEqualTo("CONSUMER");
        assertThat(KafkaGroupInventoryService.describe(group("custom-worker","new-protocol",GroupType.UNKNOWN)).classification()).isEqualTo("UNKNOWN");
        assertThat(KafkaGroupInventoryService.describe(group("shared-consumer","share",GroupType.SHARE)).classification()).isEqualTo("OTHER");
    }
    @Test void readsEveryGroupAndDoesNotConvertMetadataFailureIntoAnEmptyResult() {
        UUID id=UUID.randomUUID();var kafka=mock(KafkaAdminService.class);var admin=mock(AdminClient.class);
        when(kafka.getAdminClient(id)).thenReturn(admin);var result=mock(ListGroupsResult.class);
        when(admin.listGroups(any(ListGroupsOptions.class))).thenReturn(result);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(List.of(group("worker","connect",GroupType.CLASSIC),group("app","consumer",GroupType.CONSUMER))));
        var service=new KafkaGroupInventoryService(kafka);
        assertThat(service.list(id)).extracting(KafkaGroupInventoryService.Group::classification).containsExactly("CONSUMER","CONNECT_COORDINATION");
        var failed = new org.apache.kafka.common.internals.KafkaFutureImpl<Collection<GroupListing>>();
        failed.completeExceptionally(new org.apache.kafka.common.errors.ClusterAuthorizationException("denied"));
        when(result.all()).thenReturn(failed);
        assertThatThrownBy(()->service.list(id)).isInstanceOf(IllegalStateException.class).hasMessageContaining("metadata is unavailable");
        verify(admin,never()).listConsumerGroupOffsets(anyString());
    }
}
