package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.ExternalDataService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in, rollback-only check against the existing local Docker database, never the server. */
@EnabledIfEnvironmentVariable(named = "TANTOR_LOCAL_MAPPING_DB_TEST", matches = "true")
class AddServiceMappingDatabaseTest {
    @Test
    void persistsBothRolesAndRetriesWithoutDuplicatingOrFabricatingValues() {
        var dataSource = new DriverManagerDataSource("jdbc:postgresql://tantor-kafka-v2-db-local:5432/tantor",
                "tantor_dev", System.getenv("TANTOR_LOCAL_DB_PASSWORD"));
        var jdbc = new JdbcTemplate(dataSource);
        var mappings = new AddServiceMappingService(jdbc, new ObjectMapper(),
                org.mockito.Mockito.mock(org.springframework.context.ApplicationEventPublisher.class));
        var transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        Long before = jdbc.queryForObject("SELECT count(*) FROM kf_add_service", Long.class);
        transaction.executeWithoutResult(tx -> {
            tx.setRollbackOnly();
            UUID cluster = UUID.randomUUID();
            UUID schemaId = UUID.randomUUID(), connectId = UUID.randomUUID();
            var service = new ExternalDataService(); service.setClusterId(cluster); service.setAgentId("mapping-test-agent");
            service.setId(schemaId); service.setRole("schema_registry"); service.setStatus("PENDING");
            service.setConfigJson("{\"bootstrap_servers\":\"a:9092,b:9092\",\"service_user\":\"root\"}");
            mappings.recordExternal(service, "creator");
            var created = jdbc.queryForMap("SELECT * FROM kf_add_service WHERE add_service_id = ?", schemaId);
            assertThat(created).hasSize(17).containsEntry("broker_ip", "a:9092,b:9092")
                    .containsEntry("user", "root").containsEntry("host_id", "mapping-test-agent");
            for (String field : new String[]{"secured", "security_protocol", "security_mechanism",
                    "trust_store_location", "user_security", "password_security"}) assertThat(created.get(field)).isNull();
            mappings.updateStatus(schemaId, "FAILED", "creator");
            mappings.recordExternal(service, "retry-user");
            mappings.updateStatus(schemaId, "DEPLOYING", "retry-user");
            mappings.updateStatus(schemaId, "ONLINE", "retry-user");
            var retried = jdbc.queryForMap("SELECT * FROM kf_add_service WHERE add_service_id = ?", schemaId);
            assertThat(retried).containsEntry("created_time", created.get("created_time"))
                    .containsEntry("created_by", "creator").containsEntry("updated_by", "retry-user")
                    .containsEntry("status", "ONLINE");
            service.setId(connectId); service.setRole("kafka_connect");
            mappings.recordExternal(service, "creator");
            mappings.updateStatus(UUID.randomUUID(), "ONLINE", "old-job");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_add_service WHERE cluster_id = ?", Long.class, cluster))
                    .isEqualTo(2L);
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_add_service", Long.class)).isEqualTo(before);
    }
}
