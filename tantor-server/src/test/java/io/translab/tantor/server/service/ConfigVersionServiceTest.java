package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class ConfigVersionServiceTest {

    @Test
    void targetedPropertyRevisionsCannotEnterFullFileApplyApproveOrRollback() {
        var repository=mock(io.translab.tantor.server.persistence.ConfigVersionRepository.class);
        var jobs=mock(JobService.class);
        var services=mock(io.translab.tantor.server.repository.ClusterServiceAssignmentRepository.class);
        var cluster=java.util.UUID.randomUUID();var version=new io.translab.tantor.server.domain.ConfigVersion();
        version.setId(java.util.UUID.randomUUID());version.setClusterId(cluster);
        version.setStatus(io.translab.tantor.server.domain.ConfigVersionStatus.FAILED);
        version.setValidationResult("{\"targetedChangeHistory\":true}");
        when(repository.findById(version.getId())).thenReturn(java.util.Optional.of(version));
        var guarded=new ConfigVersionService(repository,services,null,jobs,null,new ObjectMapper(),null);
        assertThatThrownBy(()->guarded.apply(cluster,version.getId(),true)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("selected properties");
        assertThatThrownBy(()->guarded.createRollback(cluster,version.getId())).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("selected properties");
        assertThatThrownBy(()->guarded.approve(cluster,version.getId())).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("selected properties");
        verifyNoInteractions(services);
        verify(repository,never()).save(any());
    }

    private final ConfigVersionService service = new ConfigVersionService(
            null, null, null, null, null, new ObjectMapper(), null);

    @Test
    void previewProducesAddedRemovedAndModifiedDiff() {
        Map<String, Object> oldConfig = new LinkedHashMap<>(Map.of(
                "num.io.threads", "8",
                "obsolete.key", "yes"
        ));
        Map<String, Object> newConfig = new LinkedHashMap<>(Map.of(
                "num.io.threads", "12",
                "compression.type", "lz4"
        ));

        Map<String, Object> preview = service.preview(oldConfig, newConfig, true);

        assertThat(preview.get("valid")).isEqualTo(true);
        assertThat((java.util.List<?>) preview.get("diff")).hasSize(3);
    }

    @Test
    void previewRejectsTopologyManagedChanges() {
        Map<String, Object> preview = service.preview(
                Map.of("node.id", "1", "num.io.threads", "8"),
                Map.of("node.id", "2", "num.io.threads", "8"),
                true
        );

        assertThat(preview.get("valid")).isEqualTo(false);
        assertThat(String.valueOf(preview.get("errors"))).contains("node.id is topology-managed");
    }

    @Test
    void previewRejectsMinIsrAboveReplicationFactor() {
        Map<String, Object> preview = service.preview(
                Map.of("default.replication.factor", "3", "min.insync.replicas", "2"),
                Map.of("default.replication.factor", "2", "min.insync.replicas", "3"),
                true
        );

        assertThat(preview.get("valid")).isEqualTo(false);
        assertThat(String.valueOf(preview.get("errors"))).contains("cannot be greater");
    }
}
