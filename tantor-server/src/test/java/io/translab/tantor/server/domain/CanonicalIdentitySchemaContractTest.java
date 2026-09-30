package io.translab.tantor.server.domain;

import jakarta.persistence.Column;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CanonicalIdentitySchemaContractTest {

    @Test
    void migrationBackfillsOnlyFromExistingRelationalIdentity() throws IOException {
        String sql;
        try (var stream = getClass().getResourceAsStream(
                "/db/migration/V72__bind_canonical_cluster_uuid.sql")) {
            assertNotNull(stream, "V72 migration resource must exist");
            sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8).toLowerCase();
        }

        assertTrue(sql.contains("set canonical_cluster_uuid = id"));
        assertTrue(sql.contains("cluster.id = external_node.cluster_id"));
        assertTrue(sql.contains("alter column canonical_cluster_uuid set not null"));
        assertTrue(sql.contains("unique (canonical_cluster_uuid)"));
        assertTrue(sql.contains("foreign key (canonical_cluster_uuid)"));

        String backfill = sql.substring(sql.indexOf("update kf_external_cluster_nodes"),
                sql.indexOf("-- fail closed"));
        assertFalse(backfill.contains("external_node.host"));
        assertFalse(backfill.contains("hostname"));
        assertFalse(backfill.contains("cluster_name"));
        assertFalse(backfill.contains("bootstrap_servers"));
    }

    @Test
    void entityMappingsExposeDatabaseManagedCanonicalUuid() throws Exception {
        assertReadOnlyCanonicalColumn(Cluster.class);
        assertReadOnlyCanonicalColumn(ExternalClusterNode.class);
    }

    @Test
    void migrationRemovesKraftControllerRoleOnlyFromZooKeeperBrokerRows() throws IOException {
        String sql;
        try (var stream = getClass().getResourceAsStream(
                "/db/migration/V73__normalize_zookeeper_node_roles.sql")) {
            assertNotNull(stream, "V73 migration resource must exist");
            sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8).toLowerCase();
        }

        assertTrue(sql.contains("update kf_external_cluster_nodes"));
        assertTrue(sql.contains("set is_controller = false"));
        assertTrue(sql.contains("node.is_broker = true"));
        assertTrue(sql.contains("node.is_controller = true"));
        assertTrue(sql.contains("cluster.kafka_mode"));
        assertTrue(sql.contains("'zookeeper'"));
    }

    private void assertReadOnlyCanonicalColumn(Class<?> entityType) throws Exception {
        Field field = entityType.getDeclaredField("canonicalClusterUuid");
        Column column = field.getAnnotation(Column.class);

        assertNotNull(column);
        assertEquals("canonical_cluster_uuid", column.name());
        assertFalse(column.insertable());
        assertFalse(column.updatable());
        assertFalse(column.nullable());
    }
}
