package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class KafkaConfigValidationTest {
    @Test void previewUsesValidationOnlyAndValidatesTheWholeBatchWithoutWriting() {
        var service = spy(new KafkaAdminService(null, null, null, new ObjectMapper(), null, null));
        var admin = mock(AdminClient.class); var result = mock(AlterConfigsResult.class);
        UUID cluster = UUID.randomUUID(); doReturn(admin).when(service).getAdminClient(cluster);
        when(admin.incrementalAlterConfigs(anyMap(), any(AlterConfigsOptions.class))).thenReturn(result);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        service.validateBrokerConfigs(cluster, 2, Map.of("log.retention.hours", "72", "log.segment.bytes", "200"));
        verify(admin).incrementalAlterConfigs(argThat(request -> request.size() == 1
                && request.containsKey(new ConfigResource(ConfigResource.Type.BROKER, "2"))
                && request.values().iterator().next().size() == 2), argThat(AlterConfigsOptions::shouldValidateOnly));
        verify(admin, never()).incrementalAlterConfigs(anyMap());
        var failed = new org.apache.kafka.common.internals.KafkaFutureImpl<Void>();
        failed.completeExceptionally(new org.apache.kafka.common.errors.InvalidConfigurationException("Invalid retention"));
        when(result.all()).thenReturn(failed);
        assertThatThrownBy(() -> service.validateBrokerConfigs(cluster, 2, Map.of("log.retention.hours", "abc")))
                .hasMessageContaining("Invalid retention");
    }
}
