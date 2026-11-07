package io.translab.tantor.server.service;

import io.translab.tantor.server.repository.ClusterRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.jpa.repository.Query;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

/** Rollback-only fixtures against an explicitly selected isolated PostgreSQL copy. */
@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+")
class AddServiceClusterForeignKeyDatabaseTest {
    private final DriverManagerDataSource source = new DriverManagerDataSource(
            System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"), System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),
            System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD"));
    private final JdbcTemplate jdbc = new JdbcTemplate(source);
    private final TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(source));

    @BeforeEach void requireIsolatedDatabase() {
        assertThat(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL")).contains("_check_");
    }
    private void rollback(Runnable action) {
        transaction.executeWithoutResult(tx -> { tx.setRollbackOnly(); action.run(); });
    }
    private UUID cluster() {
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO kf_clusters(id,cluster_name,status,kafka_cluster_id) VALUES (?,?,'SUCCESS',?)",
                id,"fk-test-"+id,"fk-native-"+id);
        return id;
    }
    private UUID external(String name,String nativeId) {
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO kf_external_clusters(id,cluster_name,bootstrap_servers,kafka_cluster_id) VALUES (?,?,'fixture:9092',?)",id,name,nativeId);
        return id;
    }
    private UUID mapping(UUID cluster,String role) {
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO kf_add_service(add_service_id,cluster_id,process_role,status) VALUES (?,?,?,'PENDING')",id,cluster,role);
        return id;
    }
    private int purge(UUID id) {
        try {
            String sql=ClusterRepository.class.getMethod("purgeById",UUID.class).getAnnotation(Query.class).value();
            return new NamedParameterJdbcTemplate(jdbc).update(sql,Map.of("clusterId",id));
        } catch (ReflectiveOperationException error) { throw new IllegalStateException(error); }
    }

    @Test void foreignKeyIsValidatedAndNullAssociationRemainsPermitted() {
        var constraint=jdbc.queryForMap("""
                SELECT convalidated,pg_get_constraintdef(oid) AS definition FROM pg_constraint
                WHERE conrelid='public.kf_add_service'::regclass AND contype='f' AND conname='fk_add_service_cluster'
                """);
        assertThat(constraint.get("convalidated")).isEqualTo(true);
        assertThat(constraint.get("definition").toString()).contains("FOREIGN KEY (cluster_id)", "REFERENCES kf_clusters(id)", "ON DELETE RESTRICT");
        rollback(()->assertThat(jdbc.queryForObject("SELECT cluster_id FROM kf_add_service WHERE add_service_id=?",
                UUID.class,mapping(null,"schema_registry"))).isNull());
    }

    @Test void bothRolesReferenceInternalAndExternalParentsAndCanBeReassigned() {
        rollback(()->{
            UUID internal=cluster(),external=external("fk-ext-"+UUID.randomUUID(),"fk-native-ext-"+UUID.randomUUID());
            for(String role:new String[]{"schema_registry","kafka_connect"}) {
                UUID service=mapping(internal,role);
                assertThat(jdbc.queryForObject("SELECT cluster_origin FROM kf_add_service WHERE add_service_id=?",String.class,service)).isEqualTo("INTERNAL");
                jdbc.update("UPDATE kf_add_service SET cluster_id=? WHERE add_service_id=?",external,service);
                assertThat(jdbc.queryForObject("SELECT cluster_origin FROM kf_add_service WHERE add_service_id=?",String.class,service)).isEqualTo("EXTERNAL");
            }
        });
    }

    @Test void orphanInsertAndReassignmentAreRejected() {
        assertThatThrownBy(()->rollback(()->mapping(UUID.randomUUID(),"schema_registry")))
                .isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("fk_add_service_cluster");
        assertThatThrownBy(()->rollback(()->{
            UUID id=mapping(cluster(),"kafka_connect");
            jdbc.update("UPDATE kf_add_service SET cluster_id=? WHERE add_service_id=?",UUID.randomUUID(),id);
        })).isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("fk_add_service_cluster");
    }

    @Test void directPhysicalDeletionOfAReferencedParentIsBlocked() {
        assertThatThrownBy(()->rollback(()->{
            UUID id=cluster();mapping(id,"schema_registry");
            jdbc.update("DELETE FROM kf_clusters WHERE id=?",id);
        })).isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("fk_add_service_cluster");
    }

    @Test void purgeRetainsParentAndOriginalMappingButClearsLiveAssignments() {
        rollback(()->{
            UUID id=cluster(),serviceId=mapping(id,"kafka_connect");
            String before=jdbc.queryForObject("SELECT (to_jsonb(s)-'status'-'updated_by')::text FROM kf_add_service s WHERE add_service_id=?",String.class,serviceId);
            String host=jdbc.queryForObject("SELECT id FROM kf_hosts LIMIT 1",String.class);
            jdbc.update("UPDATE kf_hosts SET cluster_id=? WHERE id=?",id,host);
            jdbc.update("INSERT INTO kf_cluster_services(id,cluster_id,host_id,role,status,config_json) VALUES (?,?,?,'kafka_connect','ONLINE','{}')",
                    serviceId,id,host);
            // V103 retires lifecycle fields; the original identity/security mapping remains.
            assertThat(purge(id)).isZero();
            assertThat(jdbc.queryForObject("SELECT status FROM kf_clusters WHERE id=?",String.class,id)).isEqualTo("DELETED");
            assertThat(jdbc.queryForObject("SELECT (to_jsonb(s)-'status'-'updated_by')::text FROM kf_add_service s WHERE add_service_id=?",String.class,serviceId)).isEqualTo(before);
            assertThat(jdbc.queryForObject("SELECT status FROM kf_add_service WHERE add_service_id=?",String.class,serviceId)).isEqualTo("DELETED");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_cluster_services WHERE cluster_id=?",Long.class,id)).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_nodes WHERE cluster_id=?",Long.class,id)).isZero();
            assertThat(jdbc.queryForObject("SELECT cluster_id FROM kf_hosts WHERE id=?",UUID.class,host)).isNull();
            String retained=jdbc.queryForObject("SELECT to_jsonb(c)::text FROM kf_clusters c WHERE id=?",String.class,id);
            assertThat(purge(id)).isZero();
            assertThat(jdbc.queryForObject("SELECT to_jsonb(c)::text FROM kf_clusters c WHERE id=?",String.class,id)).isEqualTo(retained);
        });
    }

    @Test void externalUnregisterAndReregisterPreserveHistoryAndNativeIdentity() {
        rollback(()->{
            String name="fk-reconnect-"+UUID.randomUUID(),nativeId="fk-reconnect-native-"+UUID.randomUUID();
            UUID original=external(name,nativeId),service=mapping(original,"schema_registry");
            assertThat(purge(original)).isZero();
            jdbc.update("DELETE FROM kf_external_clusters WHERE id=?",original);
            UUID replacement=external(name,nativeId);
            assertThat(replacement).isNotEqualTo(original);
            assertThat(jdbc.queryForObject("SELECT cluster_id FROM kf_add_service WHERE add_service_id=?",UUID.class,service)).isEqualTo(original);
            assertThat(jdbc.queryForObject("SELECT kafka_cluster_id FROM kf_clusters WHERE id=?",String.class,original)).isEqualTo(nativeId);
            assertThat(jdbc.queryForObject("SELECT status FROM kf_clusters WHERE id=?",String.class,original)).isEqualTo("DELETED");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_clusters WHERE kafka_cluster_id=? AND status IS DISTINCT FROM 'DELETED'",Long.class,nativeId)).isEqualTo(1);
        });
    }

    @Test void unreferencedParentsStillPurgeAndDuplicateActiveNativeIdsStayRejected() {
        rollback(()->{
            UUID id=cluster();assertThat(purge(id)).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_clusters WHERE id=?",Long.class,id)).isZero();
        });
        assertThatThrownBy(()->rollback(()->{
            UUID id=cluster();
            String nativeId=jdbc.queryForObject("SELECT kafka_cluster_id FROM kf_clusters WHERE id=?",String.class,id);
            jdbc.update("INSERT INTO kf_clusters(id,cluster_name,status,kafka_cluster_id) VALUES (?,?,'SUCCESS',?)",
                    UUID.randomUUID(),"fk-duplicate-"+UUID.randomUUID(),nativeId);
        })).isInstanceOf(DataIntegrityViolationException.class);
    }
}
