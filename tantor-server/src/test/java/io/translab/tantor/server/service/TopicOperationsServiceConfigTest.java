package io.translab.tantor.server.service;

import io.translab.tantor.server.audit.AuditService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicOperationsServiceConfigTest {

    @Test
    void updatesAndResetsTopicPropertiesInOneKafkaRequest() {
        UUID clusterId = UUID.randomUUID();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        AdminClient admin = mock(AdminClient.class);
        KafkaAdminService kafkaAdmin = mock(KafkaAdminService.class);
        DescribeConfigsResult described = mock(DescribeConfigsResult.class);
        AlterConfigsResult altered = mock(AlterConfigsResult.class);
        when(kafkaAdmin.getAdminClient(clusterId)).thenReturn(admin);
        when(admin.describeConfigs(any(), any())).thenReturn(described);
        when(described.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, new Config(List.of(
                entry("retention.ms", "1000", ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false),
                entry("cleanup.policy", "compact", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false)
        )))));
        when(admin.incrementalAlterConfigs(any())).thenReturn(altered);
        when(altered.all()).thenReturn(KafkaFuture.completedFuture(null));

        new TopicOperationsService(kafkaAdmin, mock(AuditService.class))
                .updateTopicConfigs(clusterId, "orders", Map.of("retention.ms", "2000"), List.of("cleanup.policy"));

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Map<ConfigResource, java.util.Collection<AlterConfigOp>>> captor =
                ArgumentCaptor.forClass((Class) Map.class);
        verify(admin).incrementalAlterConfigs(captor.capture());
        var request = captor.getValue();
        assertThat(request.get(resource)).hasSize(2);
        assertThat(request.get(resource)).anySatisfy(op -> {
            assertThat(op.configEntry().name()).isEqualTo("retention.ms");
            assertThat(op.configEntry().value()).isEqualTo("2000");
            assertThat(op.opType()).isEqualTo(AlterConfigOp.OpType.SET);
        });
        assertThat(request.get(resource)).anySatisfy(op -> {
            assertThat(op.configEntry().name()).isEqualTo("cleanup.policy");
            assertThat(op.opType()).isEqualTo(AlterConfigOp.OpType.DELETE);
        });
    }

    @Test
    void rejectsReadOnlyConfigBeforeWriting() {
        UUID clusterId = UUID.randomUUID();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        AdminClient admin = mock(AdminClient.class);
        KafkaAdminService kafkaAdmin = mock(KafkaAdminService.class);
        DescribeConfigsResult described = mock(DescribeConfigsResult.class);
        when(kafkaAdmin.getAdminClient(clusterId)).thenReturn(admin);
        when(admin.describeConfigs(any(), any())).thenReturn(described);
        when(described.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, new Config(List.of(
                entry("retention.ms", "1000", ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, true)
        )))));

        assertThatThrownBy(() -> new TopicOperationsService(kafkaAdmin, mock(AuditService.class))
                .updateTopicConfigs(clusterId, "orders", Map.of("retention.ms", "2000"), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        verify(admin, never()).incrementalAlterConfigs(any());
    }

    private ConfigEntry entry(String name, String value, ConfigEntry.ConfigSource source,
                              boolean sensitive, boolean readOnly) {
        return new ConfigEntry(name, value, source, sensitive, readOnly,
                List.of(), ConfigEntry.ConfigType.STRING, null);
    }
}
