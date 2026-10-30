package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/** Existing local DB only. All business fixtures and audit events roll back. */
@EnabledIfEnvironmentVariable(named="TANTOR_LOCAL_CLUSTER_ID_DB_TEST", matches="true")
class NativeKafkaClusterIdDatabaseTest {
    private final DriverManagerDataSource source=new DriverManagerDataSource(
            "jdbc:postgresql://127.0.0.1:5433/tantor", "tantor_dev",System.getenv("TANTOR_LOCAL_DB_PASSWORD"));
    private final JdbcTemplate jdbc=new JdbcTemplate(source);
    private final TransactionTemplate transaction=new TransactionTemplate(new DataSourceTransactionManager(source));
    private static final List<String> TABLES=List.of("kf_activity_logs","kf_add_service","kf_alerts","kf_audit_logs",
            "kf_cluster_audit_log","kf_cluster_operation_lock","kf_cluster_services","kf_config_change","kf_config_versions",
            "kf_data_service_connections","kf_discovery_agents","kf_external_cluster_nodes","kf_external_kafka_tasks",
            "kf_external_services","kf_host_alerts","kf_hosts","kf_nodes","kf_rolling_restart_table","kf_tasks");

    @Test void allNineteenTablesHaveNullableTextAlongsideTheUnchangedUuidAndAnInsertHook() {
        for(String table:TABLES) {
            var column=jdbc.queryForMap("SELECT data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' AND table_name=? AND column_name='kafka_cluster_id'",table);
            assertThat(column).containsEntry("data_type","text").containsEntry("is_nullable","YES").containsEntry("column_default",null);
            assertThat(jdbc.queryForObject("SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=? AND column_name='cluster_id'",String.class,table)).isEqualTo("uuid");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM pg_trigger WHERE tgrelid=('public.' || ?)::regclass AND tgname='trg_fill_native_kafka_cluster_id' AND tgenabled='O'",Long.class,table)).isEqualTo(1);
        }
    }

    @Test void configAddServiceRestartInventoryAndTaskWritersReceiveTheRealIdWithoutPassingIt() {
        rollback(()->{
            UUID cluster=cluster("ActualKafkaId_A"),change=UUID.randomUUID(),service=UUID.randomUUID(),restart=UUID.randomUUID(),task=UUID.randomUUID();
            jdbc.update("INSERT INTO kf_config_change(change_id,cluster_id,status) VALUES (?,?,'PENDING')",change,cluster);
            jdbc.update("INSERT INTO kf_add_service(add_service_id,cluster_id,status) VALUES (?,?,'PENDING')",service,cluster);
            jdbc.update("INSERT INTO kf_rolling_restart_table(unique_id,cluster_id,status) VALUES (?,?,'PENDING')",restart,cluster);
            jdbc.update("INSERT INTO kf_nodes(cluster_id,host_id,role) VALUES (?,'native-id-fixture','broker')",cluster);
            jdbc.update("INSERT INTO kf_tasks(id,cluster_id,command,status) VALUES (?,?,'TEST','PENDING')",task,cluster);
            for(String table:List.of("kf_config_change","kf_add_service","kf_rolling_restart_table","kf_nodes","kf_tasks"))
                assertThat(jdbc.queryForList("SELECT kafka_cluster_id FROM "+table+" WHERE cluster_id=?",String.class,cluster)).containsOnly("ActualKafkaId_A");
            assertThat(jdbc.queryForObject("SELECT cluster_id FROM kf_config_change WHERE change_id=?",UUID.class,change)).isEqualTo(cluster);
        });
    }

    @Test void directAuditAndTriggerGeneratedAuditBothCaptureNativeId() {
        rollback(()->{
            UUID cluster=cluster("AuditKafkaId"),direct=audit(cluster);
            assertThat(auditId(direct)).isEqualTo("AuditKafkaId");
            jdbc.update("UPDATE kf_clusters SET status='SUCCESS',kafka_cluster_id='AuditKafkaIdAfterUpdate' WHERE id=?",cluster);
            assertThat(jdbc.queryForList("SELECT kafka_cluster_id FROM kf_cluster_audit_log WHERE cluster_id=?",String.class,cluster)).containsOnly("AuditKafkaIdAfterUpdate").isNotEmpty();
            assertThat(jdbc.queryForList("SELECT kafka_cluster_id FROM kf_audit_logs WHERE cluster_id=?",String.class,cluster)).contains("AuditKafkaId","AuditKafkaIdAfterUpdate").hasSize(2);
        });
    }

    @Test void externalDiscoveryUsesExternalNativeIdAndBlankIdDoesNotFallBackToStaleMirror() {
        rollback(()->{
            UUID cluster=UUID.randomUUID();
            jdbc.update("INSERT INTO kf_external_clusters(id,cluster_name,bootstrap_servers,kafka_cluster_id) VALUES (?,?,'fixture:9092','ExternalNativeId')",cluster,"native-id-test-"+cluster);
            assertThat(auditId(audit(cluster))).isEqualTo("ExternalNativeId");
            // Simulate stale mirror inventory; external inventory remains authoritative.
            jdbc.update("UPDATE kf_external_clusters SET kafka_cluster_id='  ' WHERE id=?",cluster);
            jdbc.update("UPDATE kf_clusters SET kafka_cluster_id='StaleInternalMirror' WHERE id=?",cluster);
            assertThat(auditId(audit(cluster))).isNull();
        });
    }

    @Test void missingUnknownAndNonClusterIdsRemainNullAndCannotBeFabricatedByTheCaller() {
        rollback(()->{
            UUID cluster=cluster(null),unknown=UUID.randomUUID(),change=UUID.randomUUID();
            assertThat(auditId(audit(cluster))).isNull();
            assertThat(auditId(audit(null))).isNull();
            jdbc.update("INSERT INTO kf_config_change(change_id,cluster_id,kafka_cluster_id) VALUES (?,?,'invented')",change,unknown);
            assertThat(jdbc.queryForObject("SELECT kafka_cluster_id FROM kf_config_change WHERE change_id=?",String.class,change)).isNull();
        });
    }

    @Test void reassignmentRefreshesNativeIdAndClearingAssociationClearsIt() {
        rollback(()->{
            UUID a=cluster("KafkaA"),b=cluster("KafkaB"),id=UUID.randomUUID();
            jdbc.update("INSERT INTO kf_add_service(add_service_id,cluster_id) VALUES (?,?)",id,a);
            jdbc.update("UPDATE kf_add_service SET cluster_id=? WHERE add_service_id=?",b,id);
            assertThat(jdbc.queryForObject("SELECT kafka_cluster_id FROM kf_add_service WHERE add_service_id=?",String.class,id)).isEqualTo("KafkaB");
            jdbc.update("UPDATE kf_add_service SET kafka_cluster_id='wrong' WHERE add_service_id=?",id);
            assertThat(jdbc.queryForObject("SELECT kafka_cluster_id FROM kf_add_service WHERE add_service_id=?",String.class,id)).isEqualTo("KafkaB");
            jdbc.update("UPDATE kf_add_service SET cluster_id=NULL WHERE add_service_id=?",id);
            assertThat(jdbc.queryForObject("SELECT kafka_cluster_id FROM kf_add_service WHERE add_service_id=?",String.class,id)).isNull();
        });
    }

    @Test void historicalAuditSnapshotIsPreservedWhenInventoryChangesAndUpdatesStayForbidden() {
        rollback(()->{
            UUID cluster=cluster("OriginalKafkaId"),event=audit(cluster);
            jdbc.update("UPDATE kf_clusters SET kafka_cluster_id='LaterKafkaId' WHERE id=?",cluster);
            assertThat(auditId(event)).isEqualTo("OriginalKafkaId");
            assertThat(auditId(audit(cluster))).isEqualTo("LaterKafkaId");
            // Last statement: a rejected write aborts this rollback-only transaction.
            assertThatThrownBy(()->jdbc.update("UPDATE kf_audit_logs SET kafka_cluster_id='overwrite' WHERE id=?",event))
                    .isInstanceOf(DataAccessException.class).hasMessageContaining("append-only");
        });
    }

    @Test void auditDeletesStayForbidden() {
        rollback(()->{
            UUID event=audit(cluster("KafkaAuditDelete"));
            assertThatThrownBy(()->jdbc.update("DELETE FROM kf_audit_logs WHERE id=?",event))
                    .isInstanceOf(DataAccessException.class).hasMessageContaining("append-only");
        });
    }

    private UUID cluster(String kafkaId) {
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO kf_clusters(id,cluster_name,kafka_cluster_id,status) VALUES (?,?,?,'PENDING')",id,"native-id-test-"+id,kafkaId);
        return id;
    }
    private UUID audit(UUID cluster) {
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO kf_audit_logs(id,cluster_id,action,resource_type) VALUES (?,?,'NATIVE_ID_TEST','CLUSTER')",id,cluster);
        return id;
    }
    private String auditId(UUID id){return jdbc.queryForObject("SELECT kafka_cluster_id FROM kf_audit_logs WHERE id=?",String.class,id);}
    private void rollback(Runnable body){transaction.executeWithoutResult(tx->{tx.setRollbackOnly();body.run();});}
}
