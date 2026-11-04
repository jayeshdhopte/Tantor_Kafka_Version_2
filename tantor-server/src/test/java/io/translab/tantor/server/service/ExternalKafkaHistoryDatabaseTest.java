package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class ExternalKafkaHistoryDatabaseTest {
    final JdbcTemplate db=new JdbcTemplate(new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD")));
    final TransactionTemplate tx=new TransactionTemplate(new DataSourceTransactionManager(db.getDataSource()));
    record Fixture(UUID cluster,String agent,UUID task,UUID step){}
    Fixture fixture(String type,String status){return fixture(type,status,null);}
    Fixture fixture(String type,String status,Fixture owner){
        UUID cluster=owner==null?UUID.randomUUID():owner.cluster(),task=UUID.randomUUID(),step=UUID.randomUUID();String agent=owner==null?"history-check-"+UUID.randomUUID():owner.agent(),job=UUID.randomUUID().toString();
        if(owner==null){
            db.update("INSERT INTO kf_external_clusters(id,cluster_name,bootstrap_servers) VALUES (?,?,'localhost:9092')",cluster,"history-"+cluster);
            db.update("INSERT INTO kf_discovery_agents(id) VALUES (?)",agent);
        }
        db.update("INSERT INTO kf_external_kafka_tasks(id,cluster_id,agent_id,command,parameters,status,message,claim_token,deadline) VALUES (?,?,?,'VERIFY_KAFKA_SERVICE','{}',?,'original execution result','private-claim',now()+interval '1 hour')",task,cluster,agent,status);
        db.update("INSERT INTO kf_jobs(id,type,status) VALUES (?,?,'SUCCESS')",job,type);
        db.update("INSERT INTO kf_job_steps(id,job_id,step_order,name,status,external_kafka_task_id) VALUES (?,?,1,'historical execution','SUCCESS',?)",step,job,task);
        return new Fixture(cluster,agent,task,step);
    }
    void rollback(Runnable work){tx.executeWithoutResult(s->{s.setRollbackOnly();work.run();db.execute("SET CONSTRAINTS ALL IMMEDIATE");});}
    void retained(Fixture f){
        assertThat(db.queryForObject("SELECT external_kafka_task_id FROM kf_job_steps WHERE id=?",UUID.class,f.step())).isEqualTo(f.task());
        assertThat(db.queryForMap("SELECT source_cluster_id,source_agent_id,status,message,claim_token,archived_at FROM kf_external_kafka_tasks WHERE id=?",f.task()))
            .containsEntry("source_cluster_id",f.cluster()).containsEntry("source_agent_id",f.agent()).containsEntry("status","SUCCESS").containsEntry("message","original execution result").containsEntry("claim_token",null).doesNotContainEntry("archived_at",null);
    }
    @Test void deletingRegistrationsRetainsAllThreeConfigAndSixRestartReferences(){rollback(()->{
        List<Fixture> fixtures=new ArrayList<>();Fixture owner=null;
        for(int i=0;i<9;i++){
            var f=fixture(i<3?"TARGETED_CONFIG_CHANGE":"ROLLING_RESTART","SUCCESS",owner);fixtures.add(f);owner=f;
        }
        db.update("DELETE FROM kf_external_clusters WHERE id=?",owner.cluster());
        for(var f:fixtures){retained(f);
            assertThat(db.queryForObject("SELECT cluster_id FROM kf_external_kafka_tasks WHERE id=?",UUID.class,f.task())).isNull();
        }
        db.update("DELETE FROM kf_discovery_agents WHERE id=?",owner.agent());fixtures.forEach(this::retained);
    });}
    @Test void agentDeletionBeforeClusterDeletionAlsoKeepsHistory(){rollback(()->{
        var f=fixture("ROLLING_RESTART","SUCCESS");db.update("DELETE FROM kf_discovery_agents WHERE id=?",f.agent());retained(f);
        db.update("DELETE FROM kf_external_clusters WHERE id=?",f.cluster());retained(f);
    });}
    @Test void uncompletedTasksAreArchivedFailedAndCannotBeClaimed(){rollback(()->{
        for(String status:List.of("PENDING","IN_PROGRESS")){
            var f=fixture("ROLLING_RESTART",status);db.update("DELETE FROM kf_external_clusters WHERE id=?",f.cluster());
            assertThat(db.queryForObject("SELECT status FROM kf_external_kafka_tasks WHERE id=?",String.class,f.task())).isEqualTo("FAILED");
            assertThat(db.queryForObject("SELECT count(*) FROM kf_external_kafka_tasks WHERE id=? AND status IN ('PENDING','IN_PROGRESS') AND deadline>now()",Integer.class,f.task())).isZero();
            assertThat(db.queryForObject("SELECT claim_token FROM kf_external_kafka_tasks WHERE id=?",String.class,f.task())).isNull();
        }
    });}
    @Test void archivedTaskCannotBeRewritten(){
        assertThatThrownBy(()->rollback(()->{var f=fixture("ROLLING_RESTART","SUCCESS");db.update("DELETE FROM kf_external_clusters WHERE id=?",f.cluster());db.update("UPDATE kf_external_kafka_tasks SET message='rewritten' WHERE id=?",f.task());})).hasMessageContaining("immutable");
    }
    @Test void archivedTaskCannotBeDeleted(){
        assertThatThrownBy(()->rollback(()->{var f=fixture("ROLLING_RESTART","SUCCESS");db.update("DELETE FROM kf_external_clusters WHERE id=?",f.cluster());db.update("DELETE FROM kf_external_kafka_tasks WHERE id=?",f.task());})).hasMessageContaining("retained with job history");
    }
    @Test void referencedTaskCannotBeDeletedEvenBeforeArchival(){
        assertThatThrownBy(()->rollback(()->{var f=fixture("ROLLING_RESTART","SUCCESS");db.update("DELETE FROM kf_external_kafka_tasks WHERE id=?",f.task());})).hasMessageContaining("foreign key constraint");
    }
    @Test void activeTasksStillAcceptLegitimateCompletion(){rollback(()->{
        var f=fixture("TARGETED_CONFIG_CHANGE","IN_PROGRESS");db.update("UPDATE kf_external_kafka_tasks SET status='SUCCESS',message='completed' WHERE id=?",f.task());
        assertThat(db.queryForObject("SELECT archived_at FROM kf_external_kafka_tasks WHERE id=?",java.sql.Timestamp.class,f.task())).isNull();
        assertThat(db.queryForObject("SELECT status FROM kf_external_kafka_tasks WHERE id=?",String.class,f.task())).isEqualTo("SUCCESS");
    });}
    @Test void provenanceCannotBeSpoofedOrReassigned(){
        assertThatThrownBy(()->rollback(()->{var f=fixture("ROLLING_RESTART","SUCCESS");db.update("UPDATE kf_external_kafka_tasks SET source_agent_id='someone-else' WHERE id=?",f.task());})).hasMessageContaining("provenance");
    }
    @Test void lateAgentCompletionCannotChangeAnArchivedTask(){rollback(()->{
        var f=fixture("ROLLING_RESTART","IN_PROGRESS");db.update("DELETE FROM kf_external_clusters WHERE id=?",f.cluster());
        assertThat(db.update("UPDATE kf_external_kafka_tasks SET status='SUCCESS',message='late callback' WHERE id=? AND agent_id=? AND claim_token='private-claim' AND status='IN_PROGRESS' AND deadline>now()",f.task(),f.agent())).isZero();
    });}
    @Test void newRegistrationDoesNotReassignOldEvidence(){rollback(()->{
        var old=fixture("ROLLING_RESTART","SUCCESS");db.update("DELETE FROM kf_external_clusters WHERE id=?",old.cluster());
        var fresh=fixture("ROLLING_RESTART","PENDING");retained(old);
        assertThat(db.queryForObject("SELECT source_cluster_id FROM kf_external_kafka_tasks WHERE id=?",UUID.class,fresh.task())).isEqualTo(fresh.cluster()).isNotEqualTo(old.cluster());
    });}
}
