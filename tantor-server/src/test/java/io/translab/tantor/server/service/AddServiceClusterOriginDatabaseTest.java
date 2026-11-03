package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.ExternalDataService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** Existing local DB only; service, inventory and audit fixtures all roll back. */
@EnabledIfEnvironmentVariable(named="TANTOR_LOCAL_SERVICE_ORIGIN_DB_TEST",matches="true")
class AddServiceClusterOriginDatabaseTest {
    @org.junit.jupiter.api.BeforeEach void requireIsolatedOverride() {
        if(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL")!=null)
            assertThat(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL")).contains("_check_");
    }
    private final DriverManagerDataSource source=new DriverManagerDataSource(
            System.getenv().getOrDefault("TANTOR_STATUS_AUDIT_TEST_URL","jdbc:postgresql://127.0.0.1:5433/tantor"),
            System.getenv().getOrDefault("TANTOR_STATUS_AUDIT_TEST_USER","tantor_dev"),
            System.getenv().getOrDefault("TANTOR_STATUS_AUDIT_TEST_PASSWORD",System.getenv("TANTOR_LOCAL_DB_PASSWORD")));
    private final JdbcTemplate jdbc=new JdbcTemplate(source);
    private final TransactionTemplate transaction=new TransactionTemplate(new DataSourceTransactionManager(source));
    private final AddServiceMappingService mapping=new AddServiceMappingService(jdbc,new ObjectMapper(),mock(ApplicationEventPublisher.class));

    @Test void newColumnIsNullableTextWithoutADefault() {
        assertThat(jdbc.queryForMap("SELECT data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' AND table_name='kf_add_service' AND column_name='cluster_origin'"))
                .containsEntry("data_type","text").containsEntry("is_nullable","YES").containsEntry("column_default",null);
    }

    @Test void managedSchemaRegistryAndConnectAreInternalAndKeepOriginDuringRetry() {
        rollback(()->{
            UUID cluster=cluster();
            for(String role:new String[]{"schema_registry","kafka_connect"}) {
                var inventory=new Cluster();inventory.setId(cluster);
                var service=new ClusterServiceAssignment();service.setId(UUID.randomUUID());service.setCluster(inventory);
                service.setHostId("origin-fixture-host");service.setRole(role);service.setStatus("PENDING");service.setConfigJson("{\"bootstrap_servers\":\"broker:9092\",\"service_user\":\"kafka\"}");
                mapping.recordManaged(service,"creator");mapping.recordManaged(service,"creator");
                mapping.updateStatus(service.getId(),"FAILED","creator");mapping.updateStatus(service.getId(),"PENDING","retry-user");
                assertThat(row(service.getId())).containsEntry("cluster_origin","INTERNAL").containsEntry("process_role",role)
                        .containsEntry("cluster_id",cluster).containsEntry("updated_by","retry-user").containsEntry("broker_ip","broker:9092");
                assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_add_service WHERE add_service_id=?",Long.class,service.getId())).isEqualTo(1);
            }
        });
    }

    @Test void externalSchemaRegistryAndConnectAreExternalDespiteTheInternalInventoryMirror() {
        rollback(()->{
            UUID cluster=UUID.randomUUID();
            jdbc.update("INSERT INTO kf_external_clusters(id,cluster_name,bootstrap_servers,kafka_cluster_id) VALUES (?,?,'external-broker:9092','ExternalOriginKafkaId')",cluster,"origin-test-"+cluster);
            for(String role:new String[]{"schema_registry","kafka_connect"}) {
                var service=new ExternalDataService();service.setId(UUID.randomUUID());service.setClusterId(cluster);service.setAgentId("origin-fixture-agent");
                service.setRole(role);service.setStatus("PENDING");service.setConfigJson("{\"bootstrap_servers\":\"external-broker:9092\"}");
                mapping.recordExternal(service,"creator");
                assertThat(row(service.getId())).containsEntry("cluster_origin","EXTERNAL").containsEntry("process_role",role)
                        .containsEntry("kafka_cluster_id","ExternalOriginKafkaId").containsEntry("cluster_id",cluster);
            }
        });
    }

    @Test void unknownAssociationsAreRejectedAndNullAssociationRemainsUnclassified() {
        rollback(()->{
            UUID none=UUID.randomUUID();
            jdbc.update("INSERT INTO kf_add_service(add_service_id) VALUES (?)",none);
            assertThat(row(none).get("cluster_origin")).isNull();
        });
        assertThatThrownBy(()->rollback(()->jdbc.update(
                "INSERT INTO kf_add_service(add_service_id,cluster_id) VALUES (?,?)",UUID.randomUUID(),UUID.randomUUID())))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }

    @Test void reassignmentAndCallerSuppliedValuesAreResolvedAgainstActualInventory() {
        rollback(()->{
            UUID internal=cluster(),external=UUID.randomUUID(),service=UUID.randomUUID();
            jdbc.update("INSERT INTO kf_external_clusters(id,cluster_name,bootstrap_servers) VALUES (?,?,'fixture:9092')",external,"origin-test-"+external);
            jdbc.update("INSERT INTO kf_add_service(add_service_id,cluster_id,cluster_origin) VALUES (?,?,'EXTERNAL')",service,internal);
            assertThat(row(service).get("cluster_origin")).isEqualTo("INTERNAL");
            jdbc.update("UPDATE kf_add_service SET cluster_id=? WHERE add_service_id=?",external,service);
            assertThat(row(service).get("cluster_origin")).isEqualTo("EXTERNAL");
            jdbc.update("UPDATE kf_add_service SET cluster_origin='INTERNAL' WHERE add_service_id=?",service);
            assertThat(row(service).get("cluster_origin")).isEqualTo("EXTERNAL");
            jdbc.update("UPDATE kf_add_service SET cluster_id=NULL WHERE add_service_id=?",service);
            assertThat(row(service).get("cluster_origin")).isNull();
        });
    }

    @Test void actualMigrationBackfillFillsOnlyKnownOriginsAndPreservesEveryPreviousField() throws Exception {
        String migration;
        try(var resource=getClass().getResourceAsStream("/db/migration/V94__add_service_cluster_origin.sql")) {
            migration=new String(java.util.Objects.requireNonNull(resource).readAllBytes(),StandardCharsets.UTF_8);
        }
        String backfill=migration.substring(migration.lastIndexOf("UPDATE public.kf_add_service"));
        rollback(()->{
            UUID cluster=UUID.randomUUID(),known=UUID.randomUUID(),unknown=UUID.randomUUID();
            jdbc.update("INSERT INTO kf_clusters(id,cluster_name,kafka_cluster_id,origin_type) VALUES (?,?,'LateKnownKafkaId','UNKNOWN')",cluster,"origin-test-"+cluster);
            jdbc.update("INSERT INTO kf_add_service(add_service_id,cluster_id,status) VALUES (?,?,'PENDING'),(?,NULL,'FAILED')",known,cluster,unknown);
            String previous=jdbc.queryForObject("SELECT (to_jsonb(s)-'cluster_origin')::text FROM kf_add_service s WHERE add_service_id=?",String.class,known);
            jdbc.update("UPDATE kf_clusters SET origin_type='INTERNAL' WHERE id=?",cluster);
            assertThat(row(known).get("cluster_origin")).isNull();
            jdbc.execute(backfill);
            assertThat(row(known).get("cluster_origin")).isEqualTo("INTERNAL");
            assertThat(row(unknown).get("cluster_origin")).isNull();
            assertThat(jdbc.queryForObject("SELECT (to_jsonb(s)-'cluster_origin')::text FROM kf_add_service s WHERE add_service_id=?",String.class,known)).isEqualTo(previous);
        });
    }

    private UUID cluster(){UUID id=UUID.randomUUID();jdbc.update("INSERT INTO kf_clusters(id,cluster_name,kafka_cluster_id) VALUES (?,?,'InternalOriginKafkaId')",id,"origin-test-"+id);return id;}
    private java.util.Map<String,Object> row(UUID id){return jdbc.queryForMap("SELECT * FROM kf_add_service WHERE add_service_id=?",id);}
    private void rollback(Runnable action){transaction.executeWithoutResult(tx->{tx.setRollbackOnly();action.run();});}
}
