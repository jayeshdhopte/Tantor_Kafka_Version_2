package io.translab.tantor.server.service;

import io.translab.tantor.server.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;

/** Committed fixtures intentionally remain in an explicitly selected disposable database. */
@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class ClusterOperationLockDatabaseTest {
    final JdbcTemplate jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),
            System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD")));
    final TransactionTemplate tx=new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
    UUID cluster(boolean external) {
        UUID id=UUID.randomUUID();
        if(external) jdbc.update("INSERT INTO kf_external_clusters(id,cluster_name,bootstrap_servers,kafka_cluster_id) VALUES (?,?,'fixture:9092',?)",id,"lock-test-"+id,"lock-native-"+id);
        else jdbc.update("INSERT INTO kf_clusters(id,cluster_name,status) VALUES (?,?,'SUCCESS')",id,"lock-test-"+id);
        return id;
    }
    String job(UUID cluster,String type) {
        return tx.execute(status->{
            String id=UUID.randomUUID().toString();
            jdbc.update("INSERT INTO kf_jobs(id,type,status,resource_key,payload,requested_by,created_at,updated_at,retry_count) VALUES (?,?,'PENDING',?,?::jsonb,'lock-test-user',now(),now(),0)",
                    id,type,"cluster:"+cluster,"{\"clusterId\":\""+cluster+"\"}");
            if(!type.equals("ROLLING_RESTART")) jdbc.update("INSERT INTO kf_config_change(change_id,cluster_id,status,requested_by) VALUES (?,?,'PENDING','lock-test-user')",UUID.fromString(id),cluster);
            return id;
        });
    }
    void state(String id,String state,int retry) {
        tx.executeWithoutResult(ignored->jdbc.update("UPDATE kf_jobs SET status=?,retry_count=?,updated_at=clock_timestamp(),end_time=CASE WHEN ? IN ('SUCCESS','FAILED','PARTIAL_SUCCESS','ROLLED_BACK','ROLLBACK_FAILED') THEN clock_timestamp() ELSE NULL END WHERE id=?",state,retry,state,id));
    }
    List<Map<String,Object>> locks(String id) {return jdbc.queryForList("SELECT * FROM kf_cluster_operation_lock WHERE job_id=? ORDER BY acquired_at",id);}
    @Test void bothOperationFamiliesPersistTheSameExplicitProtectionContract() {
        for(String type:List.of("TARGETED_CONFIG_CHANGE","ROLLING_RESTART")) {
            String id=job(cluster(false),type);
            var initial=locks(id).get(0);
            assertThat(initial).containsEntry("lock_status","ACTIVE").containsEntry("protection_mode","JOB_RESOURCE_SERIALIZATION")
                    .containsEntry("attempt_number",1).containsEntry("execution_phase","APPLY").containsEntry("fencing_token",null);
            if(type.equals("ROLLING_RESTART"))assertThat(initial.get("change_id")).isNull();else assertThat(initial.get("change_id")).isEqualTo(UUID.fromString(id));
            state(id,"IN_PROGRESS",0);state(id,"SUCCESS",0);
            var closed=locks(id).get(0);assertThat(closed.get("acquired_at")).isEqualTo(initial.get("acquired_at"));
            assertThat(closed.get("released_at")).isNotNull();
            tx.executeWithoutResult(ignored->jdbc.update("UPDATE kf_jobs SET logs='extra historical log' WHERE id=?",id));
            assertThat(locks(id).get(0)).isEqualTo(closed);
        }
    }
    @Test void retriesAndRollbackHaveSeparateIntervalsWithoutMovingTheOriginalRelease() {
        String id=job(cluster(false),"TARGETED_CONFIG_CHANGE");
        state(id,"IN_PROGRESS",0);state(id,"FAILED",0);var forward=locks(id).get(0);
        state(id,"ROLLBACK_PENDING",0);state(id,"ROLLING_BACK",0);state(id,"ROLLBACK_FAILED",0);
        state(id,"ROLLBACK_PENDING",1);state(id,"ROLLING_BACK",1);state(id,"ROLLED_BACK",1);
        assertThat(locks(id)).hasSize(3);
        assertThat(locks(id).get(0)).isEqualTo(forward);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_cluster_operation_lock a JOIN kf_cluster_operation_lock b ON a.job_id=b.job_id AND a.lock_id<b.lock_id WHERE a.job_id=? AND tstzrange(a.acquired_at,a.released_at,'[)') && tstzrange(b.acquired_at,b.released_at,'[)')",Integer.class,id)).isZero();
    }
    UUID task(String id,UUID cluster,boolean external) {
        return tx.execute(ignored->{
            UUID task=UUID.randomUUID();
            if(external) jdbc.update("INSERT INTO kf_external_kafka_tasks(id,cluster_id,agent_id,command,parameters,status,deadline) VALUES (?,?,?,'RESTART_SERVICE','{}','IN_PROGRESS',now()+interval '5 minutes')",
                    task,cluster,jdbc.queryForObject("SELECT id FROM kf_discovery_agents LIMIT 1",String.class));
            else jdbc.update("INSERT INTO kf_tasks(id,host_id,cluster_id,command,parameters,status) VALUES (?,?,?,'RESTART_SERVICE','{}'::jsonb,'IN_PROGRESS')",
                    task,jdbc.queryForObject("SELECT id FROM kf_hosts LIMIT 1",String.class),cluster);
            jdbc.update("INSERT INTO kf_job_steps(id,job_id,step_order,name,status,"+(external?"external_kafka_task_id":"agent_task_id")+") VALUES (?,?,1,'lock fixture','IN_PROGRESS',?)",UUID.randomUUID(),id,task);
            return task;
        });
    }
    @Test void unfinishedInternalAndExternalTasksRetainOwnershipUntilTheirCallbacksComplete() throws Exception {
        for(boolean external:new boolean[]{false,true}) {
            UUID cluster=cluster(external);String id=job(cluster,"ROLLING_RESTART");state(id,"IN_PROGRESS",0);
            UUID task=task(id,cluster,external);state(id,"FAILED",0);
            assertThat(locks(id).get(0)).containsEntry("lock_status","WAITING_AGENT").containsEntry("released_at",null);
            assertThat(jdbc.queryForObject("SELECT cluster_operation_has_unfinished_tasks(?)",Boolean.class,id)).isTrue();
            String query=JobRepository.class.getMethod("findWithUnfinishedRestartTasks").getAnnotation(Query.class).value();
            assertThat(jdbc.queryForList(query).stream().map(row->row.get("id"))).contains(id);
            assertThatThrownBy(()->job(cluster,"TARGETED_CONFIG_CHANGE")).hasStackTraceContaining("uq_operation_lock_live_cluster");
            tx.executeWithoutResult(ignored->jdbc.update("UPDATE "+(external?"kf_external_kafka_tasks":"kf_tasks")+" SET status='SUCCESS' WHERE id=?",task));
            assertThat(locks(id).get(0)).containsEntry("lock_status","RELEASED");
            String next=job(cluster,"TARGETED_CONFIG_CHANGE");state(next,"SUCCESS",0);
        }
    }
    @Test void sameJobResumeHandsOffWaitingOwnershipWithoutAdmittingAnotherJob() {
        UUID cluster=cluster(false);String id=job(cluster,"ROLLING_RESTART");state(id,"IN_PROGRESS",0);
        UUID task=task(id,cluster,false);state(id,"FAILED",0);state(id,"PENDING",1);
        assertThat(locks(id)).hasSize(2);assertThat(locks(id).get(0)).containsEntry("release_reason","ATTEMPT_HANDOFF");
        assertThatThrownBy(()->job(cluster,"TARGETED_CONFIG_CHANGE")).hasStackTraceContaining("uq_jobs_active_resource");
        tx.executeWithoutResult(ignored->jdbc.update("UPDATE kf_tasks SET status='SUCCESS' WHERE id=?",task));
        state(id,"SUCCESS",1);
    }
    @Test void simultaneousConfigAndRestartRequestsHaveOneWinnerAndIndependentClustersProceed() throws Exception {
        UUID shared=cluster(false);var start=new CountDownLatch(1);var executor=Executors.newFixedThreadPool(2);
        try {
            List<Future<Boolean>> attempts=new ArrayList<>();
            for(String type:List.of("TARGETED_CONFIG_CHANGE","ROLLING_RESTART"))attempts.add(executor.submit(()->{
                start.await();try{job(shared,type);return true;}catch(RuntimeException rejected){return false;}
            }));
            start.countDown();int winners=0;for(var attempt:attempts)if(attempt.get(20,TimeUnit.SECONDS))winners++;
            assertThat(winners).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_cluster_operation_lock WHERE cluster_id=? AND lock_status='ACTIVE'",Integer.class,shared)).isEqualTo(1);
            String independent=job(cluster(false),"ROLLING_RESTART");state(independent,"SUCCESS",0);
        } finally {executor.shutdownNow();}
    }
    @Test void abortedQueueLeavesNoPhantomLedgerEntry() {
        UUID cluster=cluster(false);String id=UUID.randomUUID().toString();
        tx.executeWithoutResult(status->{status.setRollbackOnly();jdbc.update("INSERT INTO kf_jobs(id,type,status,resource_key,payload) VALUES (?,'ROLLING_RESTART','PENDING',?,'{}'::jsonb)",id,"cluster:"+cluster);});
        assertThat(locks(id)).isEmpty();
    }
    @Test void liveAcquisitionIsNotBackdatedToAnOldJobObjectCreationTime() {
        UUID cluster=cluster(false);String previous=job(cluster,"ROLLING_RESTART");state(previous,"SUCCESS",0);
        var released=(java.sql.Timestamp)locks(previous).get(0).get("released_at");
        String next=UUID.randomUUID().toString();
        tx.executeWithoutResult(ignored->jdbc.update("INSERT INTO kf_jobs(id,type,status,resource_key,payload,created_at,retry_count) VALUES (?,'ROLLING_RESTART','PENDING',?,'{}'::jsonb,now()-interval '1 hour',0)",next,"cluster:"+cluster));
        assertThat(((java.sql.Timestamp)locks(next).get(0).get("acquired_at")).toInstant()).isAfterOrEqualTo(released.toInstant());
        state(next,"SUCCESS",0);
    }
}
