package io.translab.tantor.server.web;

import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.service.ActivityAlertService;
import io.translab.tantor.server.service.KafkaAdminService;
import io.translab.tantor.server.service.PartitionCacheService;
import io.translab.tantor.server.service.TopicOperationsService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicsControllerActivityTest {

    @Test
    void topicSettingsUpdateRequiresPermissionAndRecordsActivity() {
        KafkaAdminService kafkaAdmin = mock(KafkaAdminService.class);
        TopicOperationsService topicOperations = mock(TopicOperationsService.class);
        PartitionCacheService partitionCache = mock(PartitionCacheService.class);
        ClusterRepository clusters = mock(ClusterRepository.class);
        AuditService audit = mock(AuditService.class);
        RoleAuthenticationUtil roles = mock(RoleAuthenticationUtil.class);
        ActivityAlertService activities = mock(ActivityAlertService.class);
        UUID clusterId = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(clusterId);
        cluster.setName("payments-external");
        when(roles.canAccess("Bearer test-token", RoleAuthenticationUtil.TOPIC_MUTATION)).thenReturn(true);
        when(clusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        TopicsController controller = new TopicsController(
                kafkaAdmin, topicOperations, partitionCache, clusters, audit, roles, activities);
        TopicsController.TopicConfigsUpdateRequest request = new TopicsController.TopicConfigsUpdateRequest();
        request.setValues(Map.of("retention.ms", "2000"));
        request.setResets(List.of("cleanup.policy"));

        assertThat(controller.updateConfigs(null, clusterId, "orders", request).getStatusCode().value()).isEqualTo(401);
        assertThat(controller.updateConfigs("Bearer test-token", clusterId, "orders", request).getStatusCode().value()).isEqualTo(204);
        verify(topicOperations).updateTopicConfigs(clusterId, "orders", request.getValues(), request.getResets());
        verify(activities).logAudit(
                eq("INFO"), eq("TOPIC"), eq("UPDATE"),
                eq("Updated configuration for topic orders in cluster payments-external"),
                eq("TOPIC"), eq("orders"), eq(clusterId),
                eq(null), eq(null), eq("SUCCESS"), eq(null), eq(null));
    }

    @Test
    void successfulTopicCreationIsAddedToDashboardActivityFeed() {
        KafkaAdminService kafkaAdmin = mock(KafkaAdminService.class);
        TopicOperationsService topicOperations = mock(TopicOperationsService.class);
        PartitionCacheService partitionCache = mock(PartitionCacheService.class);
        ClusterRepository clusters = mock(ClusterRepository.class);
        AuditService audit = mock(AuditService.class);
        RoleAuthenticationUtil roles = mock(RoleAuthenticationUtil.class);
        ActivityAlertService activities = mock(ActivityAlertService.class);

        UUID clusterId = UUID.randomUUID();
        Cluster externalCluster = new Cluster();
        externalCluster.setId(clusterId);
        externalCluster.setName("payments-external");
        externalCluster.setMode("EXTERNAL");

        when(roles.canAccess("Bearer test-token", RoleAuthenticationUtil.TOPIC_MUTATION)).thenReturn(true);
        when(clusters.findById(clusterId)).thenReturn(Optional.of(externalCluster));

        TopicsController controller = new TopicsController(
                kafkaAdmin, topicOperations, partitionCache, clusters, audit, roles, activities);
        TopicsController.TopicCreateRequest request = new TopicsController.TopicCreateRequest();
        request.setName("transactions");
        request.setPartitions(3);
        request.setReplicationFactor((short) 1);
        request.setConfigs(Map.of());

        var response = controller.createTopic("Bearer test-token", clusterId, request);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        verify(kafkaAdmin).createTopic(clusterId, "transactions", 3, (short) 1, Map.of());
        verify(activities).logAudit(
                eq("INFO"), eq("TOPIC"), eq("CREATE"),
                eq("Created topic transactions in cluster payments-external"),
                eq("TOPIC"), eq("transactions"), eq(clusterId),
                eq(null), eq(null), eq("SUCCESS"), eq(null), eq(null));
        verify(audit).record(eq("CLUSTER_CHANGE"), eq("TOPIC_CREATED"), eq("CLUSTER"),
                eq(clusterId.toString()), eq(clusterId), eq("SUCCESS"),
                eq(null), eq(null), eq(null), any());
    }
}
