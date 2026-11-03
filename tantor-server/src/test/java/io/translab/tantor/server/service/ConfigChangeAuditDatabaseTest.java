package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

/** Real commits in a disposable *_check_* database; immutable audit fixtures are intentionally retained. */
@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class ConfigChangeAuditDatabaseTest {
    final DriverManagerDataSource source = new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),
            System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"), System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD"));
    final JdbcTemplate jdbc = new JdbcTemplate(source);
    final TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(source));
    final ObjectMapper mapper = new ObjectMapper();

    UUID fixture(boolean sensitive) {
        UUID change = UUID.randomUUID(), cluster = UUID.randomUUID(), target = UUID.randomUUID(), step = UUID.randomUUID();
        jdbc.update("INSERT INTO kf_clusters(id,cluster_name,status,kafka_cluster_id) VALUES (?,?,'SUCCESS',?)",
                cluster,"audit-test-"+cluster,"native-audit-"+cluster);
        jdbc.update("INSERT INTO kf_jobs(id,type,status,requested_by,payload) VALUES (?,'TARGETED_CONFIG_CHANGE','PENDING','audit-test-user','{}'::jsonb)",change.toString());
        jdbc.update("INSERT INTO kf_job_steps(id,job_id,step_order,name,status,payload) VALUES (?,?,1,'fixture','PENDING',?::jsonb)",
                step,change.toString(),"{\"hostname\":\"fixture-broker\",\"ipAddress\":\"192.0.2.21\"}");
        // Insert source first: its deferred event must still see targets/properties added later.
        jdbc.update("INSERT INTO kf_config_change(change_id,cluster_id,title,status,requested_by,requested_at,created_at,updated_at,target_role,target_scope,execution_type) VALUES (?,?,'2 properties','PENDING','audit-test-user',now(),now(),now(),'BROKER','SPECIFIC','DYNAMIC_ALLOWED')",change,cluster);
        jdbc.update("INSERT INTO kf_config_change_target_node(target_id,change_id,planned_order,target_status) VALUES (?,?,1,'PENDING')",target,change);
        jdbc.update("INSERT INTO kf_config_execution_attempt(attempt_id,change_id,target_id,requested_payload,updated_at) VALUES (?,?,?,?::jsonb,now())",
                UUID.randomUUID(),change,target,"{\"nodeId\":2,\"hostId\":\"fixture-host\",\"stepId\":\""+step+"\"}");
        for (String key : sensitive ? List.of("ssl.password", "custom.setting") : List.of("log.segment.bytes", "message.max.bytes")) {
            UUID detail = UUID.randomUUID();
            jdbc.update("INSERT INTO kf_config_change_detail(detail_id,change_id,config_key,operation,is_sensitive,sequence_no) VALUES (?,?,?,'SET',?,1)",detail,change,key,sensitive && key.equals("custom.setting"));
            jdbc.update("INSERT INTO kf_config_change_node_value(node_value_id,change_id,detail_id,target_id,value_before,desired_value,is_sensitive,source_before,before_present) VALUES (?,?,?,?,?,?,false,'STATIC_BROKER_CONFIG',true)",
                    UUID.randomUUID(),change,detail,target,sensitive?"never-store-before":"100",sensitive?"never-store-after":"200");
        }
        return change;
    }
    UUID create(boolean sensitive) { return tx.execute(status -> fixture(sensitive)); }
    void status(UUID change, String state) {
        tx.executeWithoutResult(ignored -> jdbc.update("UPDATE kf_config_change SET status=?,updated_at=clock_timestamp(),completed_at=CASE WHEN ?='SUCCESS' THEN now() ELSE completed_at END WHERE change_id=?",state,state,change));
    }
    int count(UUID change) { return jdbc.queryForObject("SELECT count(*) FROM kf_audit_logs WHERE change_id=?",Integer.class,change); }
    JsonNode details(UUID change, String state) throws Exception {
        return mapper.readTree(jdbc.queryForObject("SELECT details::text FROM kf_audit_logs WHERE change_id=? AND status=? ORDER BY created_time DESC LIMIT 1",String.class,change,state));
    }
    @Test void requestedEventCapturesFullCommittedPlanAndInitiatingUser() throws Exception {
        UUID change = create(false);
        assertThat(count(change)).isEqualTo(1);
        var row = jdbc.queryForMap("SELECT * FROM kf_audit_logs WHERE change_id=?",change);
        assertThat(row.get("created_by")).isEqualTo("audit-test-user");
        assertThat(row.get("correlation_id")).isEqualTo(change);
        assertThat(row.get("action")).isEqualTo("CONFIG_CHANGE_REQUESTED");
        assertThat(row.get("host_name")).isEqualTo("fixture-broker");
        JsonNode detail = details(change,"PENDING");
        assertThat(detail.path("affectedNodes").get(0).path("properties").size()).isEqualTo(2);
        assertThat(detail.path("affectedNodes").get(0).path("nodeId").asText()).isEqualTo("2");
        assertThat(detail.path("after_state").get(0).path("properties").get(0).path("value").isNull()).isTrue();
        assertThat(detail.path("reconstructed").asBoolean()).isFalse();
    }
    @Test void completionCapturesVerifiedValuesAndRepeatedStatusWritesDoNotDuplicate() throws Exception {
        UUID change = create(false);
        for (int i=0;i<5;i++) status(change,"IN_PROGRESS");
        tx.executeWithoutResult(ignored -> {
            jdbc.update("UPDATE kf_config_change_node_value SET value_after='200',source_after='DYNAMIC_BROKER_CONFIG',verification_status='SUCCESS',verified_at=now() WHERE change_id=?",change);
            jdbc.update("UPDATE kf_config_change_target_node SET target_status='SUCCESS' WHERE change_id=?",change);
            jdbc.update("UPDATE kf_config_change SET status='SUCCESS',completed_at=now(),updated_at=now() WHERE change_id=?",change);
        });
        assertThat(count(change)).isEqualTo(3);
        JsonNode detail = details(change,"SUCCESS");
        assertThat(detail.path("event_type").asText()).isEqualTo("CONFIG_CHANGE_COMPLETED");
        assertThat(detail.path("before_state").get(0).path("properties").get(0).path("value").asText()).isEqualTo("100");
        assertThat(detail.path("after_state").get(0).path("properties").get(0).path("value").asText()).isEqualTo("200");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_change_audit WHERE change_id=?",Integer.class,change)).isEqualTo(3);
    }
    @Test void abortedStatusAndAbortedCreationLeaveNoPhantomAudit() {
        UUID change = create(false);
        tx.executeWithoutResult(rollback -> {
            jdbc.update("UPDATE kf_config_change SET status='FAILED' WHERE change_id=?",change);
            rollback.setRollbackOnly();
        });
        assertThat(count(change)).isEqualTo(1);
        UUID aborted = tx.execute(rollback -> { UUID id=fixture(false); rollback.setRollbackOnly(); return id; });
        assertThat(count(aborted)).isZero();
    }
    @Test void retryAndRollbackTransitionsPreserveInitiatorAndDistinctEvents() {
        UUID change = create(false);
        List<String> states = List.of("IN_PROGRESS","FAILED","PENDING","IN_PROGRESS","FAILED",
                "ROLLBACK_PENDING","ROLLING_BACK","ROLLBACK_FAILED","ROLLBACK_PENDING","ROLLING_BACK","ROLLED_BACK");
        states.forEach(state -> status(change,state));
        assertThat(count(change)).isEqualTo(states.size()+1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_audit_logs WHERE change_id=? AND created_by <> 'audit-test-user'",Integer.class,change)).isZero();
        assertThat(jdbc.queryForObject("SELECT action FROM kf_audit_logs WHERE change_id=? AND status='ROLLED_BACK'",String.class,change)).isEqualTo("CONFIG_CHANGE_ROLLED_BACK");
    }
    @Test void sensitiveFlagsAndSensitiveNamesAreBothRedacted() throws Exception {
        UUID change=create(true);
        String json=details(change,"PENDING").toString();
        assertThat(json).doesNotContain("never-store-before","never-store-after");
        assertThat(details(change,"PENDING").path("affectedNodes").get(0).path("properties").get(0).path("sensitive").asBoolean()).isTrue();
    }
    @Test void historicalSnapshotIsExplicitAndReplayingItsIdCannotRewriteAnAuditRow() throws Exception {
        UUID change=create(false), event=UUID.randomUUID();
        String query="SELECT record_config_change_audit(?,NULL,'PENDING',?,now(),true)";
        jdbc.queryForObject(query,UUID.class,change,event);
        String before=jdbc.queryForObject("SELECT details::text FROM kf_audit_logs WHERE id=?",String.class,event);
        jdbc.queryForObject(query,UUID.class,change,event);
        assertThat(count(change)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT details::text FROM kf_audit_logs WHERE id=?",String.class,event)).isEqualTo(before);
        assertThat(mapper.readTree(before).path("reconstructed").asBoolean()).isTrue();
    }
}
