package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ClusterRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.jpa.repository.Query;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import static org.assertj.core.api.Assertions.*;

/** Only disposable *_check_* databases. Most fixtures roll back; one real application commit is retained. */
@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class ConfigHistoryForeignKeyDatabaseTest {
    final JdbcTemplate jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),
            System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD")));
    final TransactionTemplate tx=new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
    final ObjectMapper mapper=new ObjectMapper();
    record Link(String table,String primaryKey,String column) {}
    static Stream<Link> directLinks() {
        var links=new ArrayList<Link>(List.of(new Link("kf_config_change","change_id","cluster_id"),
                new Link("kf_config_change_detail","detail_id","change_id"),new Link("kf_config_change_detail","detail_id","config_catalog_id"),
                new Link("kf_config_change_target_node","target_id","change_id")));
        for(String[] table:new String[][]{{"kf_config_change_node_value","node_value_id"},{"kf_config_execution_attempt","attempt_id"},{"kf_config_backup","backup_id"}})
            for(String column:List.of("change_id","detail_id","target_id")) links.add(new Link(table[0],table[1],column));
        links.addAll(List.of(new Link("kf_cluster_operation_lock","lock_id","cluster_id"),new Link("kf_cluster_operation_lock","lock_id","change_id"),
                new Link("kf_health_check","health_check_id","change_id"),new Link("kf_health_check","health_check_id","target_id"),
                new Link("kf_rolling_restart_operation","restart_operation_id","change_id"),new Link("kf_rolling_restart_operation","restart_operation_id","current_target_id"),
                new Link("kf_rolling_restart_node_step","restart_step_id","change_id"),new Link("kf_rolling_restart_node_step","restart_step_id","restart_operation_id"),
                new Link("kf_rolling_restart_node_step","restart_step_id","target_id"),new Link("kf_rolling_restart_table","unique_id","cluster_id")));
        return links.stream();
    }
    void rollback(Runnable body) { tx.executeWithoutResult(status->{status.setRollbackOnly();body.run();jdbc.execute("SET CONSTRAINTS ALL IMMEDIATE");}); }
    UUID change(UUID cluster) { UUID id=UUID.randomUUID();jdbc.update("INSERT INTO kf_config_change(change_id,cluster_id,status) VALUES (?,?,'PENDING')",id,cluster);return id; }
    UUID cluster(boolean external,String nativeId) {
        UUID id=UUID.randomUUID();
        if(external) jdbc.update("INSERT INTO kf_external_clusters(id,cluster_name,bootstrap_servers,kafka_cluster_id) VALUES (?,?,'fixture:9092',?)",id,"history-fk-"+id,nativeId);
        else jdbc.update("INSERT INTO kf_clusters(id,cluster_name,status,kafka_cluster_id) VALUES (?,?,'SUCCESS',?)",id,"history-fk-"+id,nativeId);
        return id;
    }
    int purge(UUID id) {
        try {return new NamedParameterJdbcTemplate(jdbc).update(ClusterRepository.class.getMethod("purgeById",UUID.class).getAnnotation(Query.class).value(),Map.of("clusterId",id));}
        catch(ReflectiveOperationException error){throw new RuntimeException(error);}
    }
    @Test void allDirectAndOwnershipConstraintsAreValidatedDeferredAndNonCascading() {
        var rows=jdbc.queryForList("SELECT convalidated,condeferrable,condeferred,confdeltype,confupdtype FROM pg_constraint WHERE contype='f' AND conname LIKE 'fk_cfg_%'");
        assertThat(rows).hasSize(34);
        rows.forEach(row->assertThat(row).containsEntry("convalidated",true).containsEntry("condeferrable",true).containsEntry("condeferred",true)
                .containsEntry("confdeltype","a").containsEntry("confupdtype","a"));
    }
    @ParameterizedTest @MethodSource("directLinks") void rejectsEveryPopulatedDanglingReference(Link link) {
        assertThatThrownBy(()->rollback(()->jdbc.update("INSERT INTO "+link.table()+" ("+link.primaryKey()+","+link.column()+") VALUES (?,?)",
                UUID.randomUUID(),link.column().equals("config_catalog_id")?Long.MAX_VALUE:UUID.randomUUID())))
                .isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("foreign key");
    }
    static Stream<Link> ownershipLinks() {
        return directLinks().filter(link->List.of("detail_id","target_id","current_target_id","restart_operation_id").contains(link.column()));
    }
    @ParameterizedTest @MethodSource("ownershipLinks") void rejectsValidIdsBelongingToDifferentChanges(Link link) {
        assertThatThrownBy(()->rollback(()->{
            UUID own=change(null),other=change(null),reference=UUID.randomUUID();
            String parent=link.column().equals("detail_id")?"kf_config_change_detail":link.column().equals("restart_operation_id")?"kf_rolling_restart_operation":"kf_config_change_target_node";
            String parentKey=link.column().equals("current_target_id")?"target_id":link.column();
            jdbc.update("INSERT INTO "+parent+" ("+parentKey+",change_id) VALUES (?,?)",reference,other);
            jdbc.update("INSERT INTO "+link.table()+" ("+link.primaryKey()+",change_id,"+link.column()+") VALUES (?,?,?)",UUID.randomUUID(),own,reference);
        })).isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("foreign key");
    }
    @Test void lockCannotReferToAChangeOnAnotherCluster() {
        assertThatThrownBy(()->rollback(()->{
            UUID first=cluster(false,"native-"+UUID.randomUUID()),second=cluster(false,"native-"+UUID.randomUUID());
            jdbc.update("INSERT INTO kf_cluster_operation_lock(lock_id,change_id,cluster_id) VALUES (?,?,?)",UUID.randomUUID(),change(first),second);
        })).isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("fk_cfg_lock_owner");
    }
    @Test void nullableUnresolvedMetadataStillPermitted() {
        rollback(()->directLinks().map(link->new Link(link.table(),link.primaryKey(),"")).distinct().forEach(link->
                jdbc.update("INSERT INTO "+link.table()+" ("+link.primaryKey()+") VALUES (?)",UUID.randomUUID())));
    }
    @Test void accidentalHistoryParentDeletionIsRejected() {
        assertThatThrownBy(()->rollback(()->{
            UUID id=change(null);jdbc.update("INSERT INTO kf_config_change_detail(detail_id,change_id) VALUES (?,?)",UUID.randomUUID(),id);
            jdbc.update("DELETE FROM kf_config_change WHERE change_id=?",id);
        })).isInstanceOf(DataIntegrityViolationException.class).hasMessageContaining("foreign key");
    }
    @Test void deferredViolationAlsoRejectsAnActualCommit() {
        assertThatThrownBy(()->tx.executeWithoutResult(status->jdbc.update(
                "INSERT INTO kf_rolling_restart_table(unique_id,cluster_id) VALUES (?,?)",UUID.randomUUID(),UUID.randomUUID())))
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
    }
    @Test void realStaticMappingCommitsIncludingChildBeforeOperationParentAndBackupRetry() throws Exception {
        UUID id=tx.execute(status->{
            UUID cluster=cluster(false,"commit-native-"+UUID.randomUUID());
            Job job=new Job();job.setId(UUID.randomUUID());job.setType(JobType.TARGETED_CONFIG_CHANGE);job.setRequestedBy("history-fk-test");
            job.setPayload(json(Map.of("clusterId",cluster.toString(),"role","BROKER","scope","SPECIFIC","classification","ROLLING_RESTART_REQUIRED")));
            jdbc.update("INSERT INTO kf_jobs(id,type,status,payload,requested_by) VALUES (?,'TARGETED_CONFIG_CHANGE','PENDING',?::jsonb,?)",job.getId().toString(),job.getPayload(),job.getRequestedBy());
            JobStep step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setStepOrder(1);step.setName("Apply controller property");step.setStatus(JobStepStatus.PENDING);
            step.setPayload(json(Map.of("operation","STATIC","nodeId",7,"role","broker","hostId","history-fk-host","configPath","/fixture/broker.properties",
                    "changes",Map.of("num.io.threads","12"),"expectedValues",Map.of("num.io.threads","8"),
                    "recoveryCheckpoint",Map.of("file",Map.of("num.io.threads","8")))));
            var mapping=new ConfigChangeMappingService(jdbc,mapper);
            mapping.record(job,List.of(step));mapping.record(job,List.of(step));
            job.setStatus(JobStatus.IN_PROGRESS);job.setStartTime(Instant.now());mapping.status(job);
            step.setStatus(JobStepStatus.IN_PROGRESS);step.setStartTime(Instant.now());mapping.step(step);
            mapping.verified(step,"num.io.threads","12","FILE");
            mapping.health(job,step,"PRECHECK",Map.of("healthy",true),true,null);
            step.setStatus(JobStepStatus.SUCCESS);step.setEndTime(Instant.now());mapping.step(step);
            job.setStatus(JobStatus.SUCCESS);job.setEndTime(Instant.now());mapping.status(job);
            jdbc.update("UPDATE kf_jobs SET status='SUCCESS' WHERE id=?",job.getId().toString());
            return job.getId();
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_node_step WHERE change_id=?",Integer.class,id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_backup WHERE change_id=?",Integer.class,id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_audit_logs WHERE change_id=? AND status='SUCCESS'",Integer.class,id)).isPositive();
    }
    @Test void configAndStandaloneHistoryRetainParentsWhileUnreferencedParentsStillPurge() {
        for(boolean standalone:new boolean[]{false,true}) rollback(()->{
            UUID id=cluster(false,"purge-native-"+UUID.randomUUID());
            if(standalone)jdbc.update("INSERT INTO kf_rolling_restart_table(unique_id,cluster_id) VALUES (?,?)",UUID.randomUUID(),id);else change(id);
            assertThat(purge(id)).isZero();
            assertThat(jdbc.queryForObject("SELECT status FROM kf_clusters WHERE id=?",String.class,id)).isEqualTo("DELETED");
            String retained=jdbc.queryForObject("SELECT row_to_json(c)::text FROM kf_clusters c WHERE id=?",String.class,id);
            assertThat(purge(id)).isZero();
            assertThat(jdbc.queryForObject("SELECT row_to_json(c)::text FROM kf_clusters c WHERE id=?",String.class,id)).isEqualTo(retained);
        });
        rollback(()->{UUID id=cluster(false,"unreferenced-"+UUID.randomUUID());assertThat(purge(id)).isEqualTo(1);});
    }
    @Test void externalUnregisterAndReregisterRetainHistoryWithOriginalClusterId() {
        for(boolean standalone:new boolean[]{false,true}) rollback(()->{
            String nativeId="external-history-"+UUID.randomUUID();UUID id=cluster(true,nativeId);
            UUID history=standalone?UUID.randomUUID():change(id);
            if(standalone)jdbc.update("INSERT INTO kf_rolling_restart_table(unique_id,cluster_id) VALUES (?,?)",history,id);
            assertThat(purge(id)).isZero();jdbc.update("DELETE FROM kf_external_clusters WHERE id=?",id);
            UUID replacement=cluster(true,nativeId);assertThat(replacement).isNotEqualTo(id);
            assertThat(jdbc.queryForObject("SELECT cluster_id FROM "+(standalone?"kf_rolling_restart_table WHERE unique_id":"kf_config_change WHERE change_id")+"=?",UUID.class,history)).isEqualTo(id);
        });
    }
    String json(Object value){try{return mapper.writeValueAsString(value);}catch(Exception error){throw new RuntimeException(error);}}
}
