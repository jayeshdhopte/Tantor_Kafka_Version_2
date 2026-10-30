package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

/** Local-only fixtures. Every INSERT/UPDATE is rolled back; no audit mutation or cleanup DELETE. */
@EnabledIfEnvironmentVariable(named="TANTOR_LOCAL_CONFIG_MAPPING_DB_TEST",matches="true")
class ConfigChangeMappingDatabaseTest {
    final DriverManagerDataSource source=new DriverManagerDataSource("jdbc:postgresql://127.0.0.1:5433/tantor","tantor_dev",System.getenv("TANTOR_LOCAL_DB_PASSWORD"));
    final JdbcTemplate jdbc=new JdbcTemplate(source);
    final ConfigChangeMappingService mapping=new ConfigChangeMappingService(jdbc,new ObjectMapper());
    final TransactionTemplate transactions=new TransactionTemplate(new DataSourceTransactionManager(source));

    @Test void dynamicBatchStoresSnapshotsBackupsVerificationAndRetryWithoutRestartRows() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,false);var steps=steps(job,false);
            mapping.record(job,steps);mapping.record(job,steps);
            assertThat(count("kf_config_change_detail",job)).isEqualTo(2);
            assertThat(count("kf_config_change_target_node",job)).isEqualTo(2);
            assertThat(count("kf_config_change_node_value",job)).isEqualTo(4);
            assertThat(count("kf_rolling_restart_operation",job)).isZero();
            assertThat(count("kf_rolling_restart_node_step",job)).isZero();
            job.setStatus(JobStatus.IN_PROGRESS);job.setStartTime(Instant.now());mapping.status(job);
            for(var step:steps) {
                start(step);mapping.step(step);
                var payload=read(step.getPayload());
                payload.put("recoveryCheckpoint",Map.of("dynamic",Map.of(
                        "log.retention.hours",Map.of("value","168","source","DYNAMIC_BROKER_CONFIG","isSensitive",false),
                        "log.retention.bytes",Map.of("value","-1","source","DEFAULT_CONFIG","isSensitive",false))));
                step.setPayload(json(payload));mapping.step(step);
                mapping.health(job,step,"PRECHECK",Map.of("healthy",true,"visibleBrokers",2,"controllerId",99),true,null);
                step.setLogs("Configuration backup (7-day retention): /tmp/mapping-node-"+step.getStepOrder()+".bak\nLive dynamic property checkpoint: /tmp/mapping-node-"+step.getStepOrder()+".json");
                mapping.verified(step,"log.retention.hours","72","DYNAMIC_BROKER_CONFIG");
                mapping.verified(step,"log.retention.bytes","2048","DYNAMIC_BROKER_CONFIG");
                step.setStatus(JobStepStatus.SUCCESS);step.setEndTime(Instant.now());mapping.step(step);
            }
            var retry=steps.get(1);retry.setStatus(JobStepStatus.FAILED);mapping.step(retry);
            retry.setRetryCount(1);retry.setStartTime(null);retry.setEndTime(null);retry.setStatus(JobStepStatus.PENDING);mapping.step(retry);
            start(retry);mapping.step(retry);retry.setStatus(JobStepStatus.SUCCESS);retry.setEndTime(Instant.now());mapping.step(retry);
            job.setStatus(JobStatus.SUCCESS);job.setEndTime(Instant.now());mapping.status(job);
            assertThat(count("kf_config_execution_attempt",job)).isEqualTo(6);
            assertThat(count("kf_config_backup",job)).isGreaterThanOrEqualTo(8);
            assertThat(count("kf_health_check",job)).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_change_node_value WHERE change_id=? AND value_before='168' AND value_after='72' AND verification_status='SUCCESS'",Long.class,job.getId())).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT lock_status FROM kf_cluster_operation_lock WHERE change_id=?",String.class,job.getId())).isEqualTo("RELEASED");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_change_detail d JOIN kf_config_catalog c ON c.config_catalog_id=d.config_catalog_id WHERE d.change_id=?",Long.class,job.getId())).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_change_node_value WHERE change_id=? AND before_present=FALSE AND rollback_operation='REMOVE'",Long.class,job.getId())).isEqualTo(2);
        });
    }

    @Test void staticBatchPopulatesBothRestartTablesAndRetainsOriginalCheckpointDuringRollback() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,true);var steps=steps(job,true);mapping.record(job,steps);
            job.setStatus(JobStatus.IN_PROGRESS);job.setStartTime(Instant.now());mapping.status(job);
            for(var step:steps) {
                start(step);mapping.step(step);var payload=read(step.getPayload());
                payload.put("recoveryCheckpoint",Map.of("file",Map.of("queued.max.requests","500","socket.request.max.bytes","104857600")));
                payload.put("configMutationStarted",true);step.setPayload(json(payload));mapping.step(step);
                mapping.health(job,step,"EXECUTION",Map.of("healthy",true,"visibleBrokers",2),true,null);
                step.setLogs("Rollback checkpoint (7-day retention): /tmp/static-node-"+step.getStepOrder()+".bak\nActive config values verified");
                step.setStatus(JobStepStatus.SUCCESS);step.setEndTime(Instant.now());mapping.step(step);
            }
            job.setStatus(JobStatus.SUCCESS);job.setEndTime(Instant.now());mapping.status(job);
            for(String table:List.of("kf_config_change","kf_config_change_detail","kf_config_change_target_node","kf_config_change_node_value",
                    "kf_cluster_operation_lock","kf_health_check","kf_config_execution_attempt","kf_config_backup","kf_rolling_restart_operation","kf_rolling_restart_node_step")) assertThat(count(table,job)).as(table).isPositive();
            assertThat(jdbc.queryForObject("SELECT completed_count FROM kf_rolling_restart_operation WHERE change_id=?",Integer.class,job.getId())).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_change_node_value WHERE change_id=? AND verification_status='SUCCESS' AND source_after='FILE'",Long.class,job.getId())).isEqualTo(4);
            job.setStatus(JobStatus.ROLLING_BACK);job.setEndTime(null);mapping.status(job);
            for(var step:steps) {
                start(step);mapping.step(step);
                mapping.verified(step,"queued.max.requests","500","FILE");
                mapping.verified(step,"socket.request.max.bytes","104857600","FILE");
                step.setStatus(JobStepStatus.ROLLED_BACK);step.setEndTime(Instant.now());mapping.step(step);
            }
            job.setStatus(JobStatus.ROLLED_BACK);job.setEndTime(Instant.now());mapping.status(job);
            assertThat(jdbc.queryForObject("SELECT restart_status FROM kf_rolling_restart_operation WHERE change_id=?",String.class,job.getId())).isEqualTo("ROLLED_BACK");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_execution_attempt WHERE change_id=? AND phase='ROLLBACK'",Long.class,job.getId())).isEqualTo(4);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_change_node_value WHERE change_id=? AND value_before='500' AND value_after='500'",Long.class,job.getId())).isEqualTo(2);
        });
    }

    @Test void versionedFileAndLegacyRollingPlansUseTheSameProjectionAndAllowPerNodeDesiredValues() {
        rollback(() -> {
            Job file=job(JobType.CONFIG_CHANGE,true);var fileSteps=steps(file,true);
            var fileRequest=read(file.getPayload());fileRequest.put("configVersionId",UUID.randomUUID().toString());file.setPayload(json(fileRequest));
            for(var step:fileSteps){var payload=read(step.getPayload());payload.put("operation","patch_service");payload.put("checkpointRecovery",true);step.setPayload(json(payload));}
            mapping.record(file,fileSteps);
            assertThat(count("kf_rolling_restart_node_step",file)).isEqualTo(2);
            Job legacy=job(JobType.ROLLING_CONFIG_UPDATE,true);
            var request=read(legacy.getPayload());request.put("rollingRestart",true);
            request.put("changes",List.of(
                    Map.of("host","one","nodeId",1,"configFilePath","/etc/kafka/server.properties","properties",List.of(Map.of("key","queued.max.requests","oldValue","500","newValue","600"))),
                    Map.of("host","two","nodeId",2,"configFilePath","/etc/kafka/server.properties","properties",List.of(Map.of("key","queued.max.requests","oldValue","500","newValue","700")))));
            legacy.setPayload(json(request));var steps=new ArrayList<JobStep>();
            for(int i=1;i<=2;i++){var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(legacy);step.setStepOrder(i);step.setTargetId(i==1?"one":"two");step.setName("WRITE_CONFIG: "+step.getTargetId());steps.add(step);}
            mapping.record(legacy,steps);
            assertThat(count("kf_config_change_detail",legacy)).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT desired_value FROM kf_config_change_detail WHERE change_id=?",String.class,legacy.getId())).isNull();
            assertThat(jdbc.queryForList("SELECT desired_value FROM kf_config_change_node_value WHERE change_id=? ORDER BY desired_value",String.class,legacy.getId())).containsExactly("600","700");
        });
    }

    @Test void sensitiveValuesAreNotCopiedAndMissingFactsStayNull() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,false);var step=steps(job,false).get(0);
            var payload=read(step.getPayload());payload.put("changes",Map.of("sasl.jaas.config","DO_NOT_COPY_SECRET"));payload.put("expectedValues",Map.of("sasl.jaas.config","PRIOR_SECRET"));
            payload.put("recoveryCheckpoint",Map.of("dynamic",Map.of("sasl.jaas.config",Map.of("value","PRIOR_SECRET","source","DYNAMIC_BROKER_CONFIG","isSensitive",true))));
            step.setPayload(json(payload));mapping.record(job,List.of(step));start(step);mapping.step(step);mapping.verified(step,"sasl.jaas.config","DO_NOT_COPY_SECRET",null);
            assertThat(jdbc.queryForObject("SELECT desired_value FROM kf_config_change_detail WHERE change_id=?",String.class,job.getId())).isNull();
            var values=jdbc.queryForMap("SELECT value_before,desired_value,value_after,secret_ref_before FROM kf_config_change_node_value WHERE change_id=?",job.getId());
            assertThat(values.values()).allMatch(Objects::isNull);
            assertThat(jdbc.queryForObject("SELECT requested_payload::text FROM kf_config_execution_attempt WHERE change_id=?",String.class,job.getId())).doesNotContain("DO_NOT_COPY_SECRET","PRIOR_SECRET");
            assertThat(jdbc.queryForObject("SELECT old_config::text || new_config::text || validation_result::text FROM kf_config_versions WHERE job_id=?",String.class,job.getId().toString()))
                    .doesNotContain("DO_NOT_COPY_SECRET","PRIOR_SECRET");
            assertThat(jdbc.queryForObject("SELECT fencing_token FROM kf_cluster_operation_lock WHERE change_id=?",Long.class,job.getId())).isNull();
            assertThat(jdbc.queryForObject("SELECT checksum_sha256 FROM kf_config_backup WHERE change_id=?",String.class,job.getId())).isNull();
        });
    }

    @Test void legacyDynamicBackupsStayBoundToTheActualBrokerInsteadOfEveryBrokerInTheStep() {
        rollback(() -> {
            Job job=job(JobType.CONFIG_CHANGE,false);var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setName("Apply dynamic property to brokers");step.setStepOrder(1);step.setTargetId("ALL");
            step.setPayload(json(Map.of("operation","dynamic","configKey","log.retention.hours","configValue","72","previousByBroker",Map.of("1","168","2","96"))));
            mapping.record(job,List.of(step));start(step);mapping.step(step);
            mapping.backup(step,Map.of("brokerId",1,"hostId","broker-one","role","broker","configPath","/etc/kafka/server.properties","key","log.retention.hours","value","72"),
                    Map.of("log.retention.hours",Map.of("value","168","source","DYNAMIC_BROKER_CONFIG","isSensitive",false)),"Configuration backup (7-day retention): /tmp/broker-one.bak");
            assertThat(count("kf_config_change_target_node",job)).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_backup WHERE change_id=? AND backup_uri='/tmp/broker-one.bak'",Long.class,job.getId())).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_backup b JOIN kf_config_change_node_value v ON v.target_id=b.target_id WHERE b.change_id=? AND b.backup_uri='/tmp/broker-one.bak' AND v.value_before='168'",Long.class,job.getId())).isEqualTo(1);
            mapping.health(job,step,"PRECHECK",Map.of("healthy",true),true,null);
            assertThat(jdbc.queryForObject("SELECT target_id FROM kf_health_check WHERE change_id=?",UUID.class,job.getId())).isNull();
        });
    }

    @Test void rollbackRetryRetainsTheFailedAttemptEvenWhenTheStepRetryCounterDoesNotChange() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,true);var step=steps(job,true).get(0);mapping.record(job,List.of(step));
            job.setStatus(JobStatus.ROLLING_BACK);start(step);mapping.step(step);
            step.setStatus(JobStepStatus.ROLLBACK_FAILED);step.setLogs("Recovery verification failed");step.setEndTime(Instant.now());mapping.step(step);
            job.setRetryCount(1);start(step);mapping.step(step);
            step.setStatus(JobStepStatus.ROLLED_BACK);step.setEndTime(Instant.now());mapping.step(step);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_execution_attempt WHERE change_id=? AND phase='ROLLBACK' AND attempt_number=1 AND attempt_status='ROLLBACK_FAILED'",Long.class,job.getId())).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_execution_attempt WHERE change_id=? AND phase='ROLLBACK' AND attempt_number=2 AND attempt_status='ROLLED_BACK'",Long.class,job.getId())).isEqualTo(2);
        });
    }

    @Test void legacyEmptyDesiredValueMeansRemovalAndAmbiguousEmptyBeforeValuesRemainUnknown() {
        rollback(() -> {
            Job job=job(JobType.ROLLING_CONFIG_UPDATE,true);var request=read(job.getPayload());request.put("rollingRestart",true);
            request.put("changes",List.of(Map.of("host","one","nodeId",1,"configFilePath","/etc/kafka/server.properties","properties",
                    List.of(Map.of("key","log.retention.hours","oldValue","","newValue","")))));job.setPayload(json(request));
            var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setName("WRITE_CONFIG: one");step.setStepOrder(1);step.setTargetId("one");
            mapping.record(job,List.of(step));
            var detail=jdbc.queryForMap("SELECT operation,desired_value FROM kf_config_change_detail WHERE change_id=?",job.getId());
            assertThat(detail).containsEntry("operation","REMOVE").containsEntry("desired_value",null);
            var before=jdbc.queryForMap("SELECT value_before,before_present FROM kf_config_change_node_value WHERE change_id=?",job.getId());
            assertThat(before.values()).allMatch(Objects::isNull);
        });
    }

    @Test void targetedVersionsArePerNodeIdempotentAndKeepRealCheckpointsAndLifecycle() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,false);var steps=steps(job,false);
            mapping.record(job,steps);mapping.record(job,steps);
            assertThat(versionCount(job)).isEqualTo(2);
            var row=jdbc.queryForMap("SELECT * FROM kf_config_versions WHERE job_id=? AND host_id='node-1'",job.getId().toString());
            assertThat(row).containsEntry("component","broker").containsEntry("config_file_name","/etc/kafka/server.properties")
                    .containsEntry("created_by","mapping-test").containsEntry("service_id",null).containsEntry("approval_required",false);
            assertThat(read(row.get("new_config").toString())).containsEntry("log.retention.hours","72");
            assertThat(read(row.get("validation_result").toString())).containsEntry("targetedChangeHistory",true).containsEntry("executionType","DYNAMIC");
            var step=steps.get(0);start(step);mapping.step(step);
            assertThat(versionStatus(job,"node-1")).isEqualTo("APPLYING");
            var payload=read(step.getPayload());payload.put("recoveryCheckpoint",Map.of("dynamic",Map.of("log.retention.hours",Map.of("value","192","source","DYNAMIC_BROKER_CONFIG"))));
            step.setPayload(json(payload));mapping.step(step);
            assertThat(read(jdbc.queryForObject("SELECT old_config::text FROM kf_config_versions WHERE job_id=? AND host_id='node-1'",String.class,job.getId().toString())))
                    .containsEntry("log.retention.hours","192");
            step.setStatus(JobStepStatus.SUCCESS);step.setEndTime(Instant.now());mapping.step(step);
            assertThat(versionStatus(job,"node-1")).isEqualTo("APPLIED");
            assertThat(jdbc.queryForObject("SELECT applied_at IS NOT NULL FROM kf_config_versions WHERE job_id=? AND host_id='node-1'",Boolean.class,job.getId().toString())).isTrue();
            job.setStatus(JobStatus.PARTIAL_SUCCESS);mapping.status(job);
            assertThat(versionStatus(job,"node-1")).isEqualTo("APPLIED");
            assertThat(versionStatus(job,"node-2")).isEqualTo("FAILED");
            job.setStatus(JobStatus.ROLLING_BACK);step.setStatus(JobStepStatus.ROLLED_BACK);mapping.step(step);
            assertThat(versionStatus(job,"node-1")).isEqualTo("SUPERSEDED");
            assertThat(versionCount(job)).isEqualTo(2);
        });
    }

    @Test void staticRevisionRecordsRemovalAsNullAndRetriesReuseTheSameRevision() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,true);var step=steps(job,true).get(0);
            var payload=read(step.getPayload());var changes=new LinkedHashMap<String,Object>();changes.put("queued.max.requests",null);payload.put("changes",changes);step.setPayload(json(payload));
            mapping.record(job,List.of(step));
            String metadata=jdbc.queryForObject("SELECT validation_result::text FROM kf_config_versions WHERE job_id=?",String.class,job.getId().toString());
            assertThat(read(metadata)).containsEntry("executionType","STATIC");
            assertThat(read(jdbc.queryForObject("SELECT new_config::text FROM kf_config_versions WHERE job_id=?",String.class,job.getId().toString())))
                    .containsEntry("queued.max.requests",null);
            step.setStatus(JobStepStatus.FAILED);mapping.step(step);assertThat(versionStatus(job,"node-1")).isEqualTo("FAILED");
            step.setRetryCount(1);start(step);mapping.step(step);assertThat(versionStatus(job,"node-1")).isEqualTo("APPLYING");
            assertThat(versionCount(job)).isEqualTo(1);
        });
    }

    @Test void revisionsAdvanceWithinTheExistingNamespaceAndLegacyVersionedJobsAreNotDuplicated() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,false);var step=steps(job,false).get(0);
            UUID cluster=UUID.fromString(read(job.getPayload()).get("clusterId").toString());
            jdbc.update("INSERT INTO kf_config_versions (id,cluster_id,host_id,component,config_file_name,config_version,old_config,new_config,status,created_by) VALUES (?,?,'node-1','broker','/etc/kafka/server.properties',7,'{}','{}','APPLIED','fixture')",UUID.randomUUID(),cluster);
            mapping.record(job,List.of(step));
            assertThat(jdbc.queryForObject("SELECT config_version FROM kf_config_versions WHERE job_id=?",Integer.class,job.getId().toString())).isEqualTo(8);
            Job legacy=job(JobType.CONFIG_CHANGE,true);mapping.record(legacy,steps(legacy,true));
            assertThat(versionCount(legacy)).isZero();
        });
    }

    private long versionCount(Job job){return jdbc.queryForObject("SELECT count(*) FROM kf_config_versions WHERE job_id=?",Long.class,job.getId().toString());}
    private String versionStatus(Job job,String host){return jdbc.queryForObject("SELECT status FROM kf_config_versions WHERE job_id=? AND host_id=?",String.class,job.getId().toString(),host);}

    @Test void runtimeSensitiveFlagRedactsRevisionValuesAcrossAllTargets() {
        rollback(() -> {
            Job job=job(JobType.TARGETED_CONFIG_CHANGE,false);var steps=steps(job,false);
            for(var step:steps) {
                var payload=read(step.getPayload());payload.put("changes",Map.of("custom.setting","NEW_SENSITIVE_VALUE"));payload.put("expectedValues",Map.of("custom.setting","OLD_SENSITIVE_VALUE"));step.setPayload(json(payload));
            }
            mapping.record(job,steps);var step=steps.get(0);var payload=read(step.getPayload());
            payload.put("recoveryCheckpoint",Map.of("dynamic",Map.of("custom.setting",Map.of("value","OLD_SENSITIVE_VALUE","source","DYNAMIC_BROKER_CONFIG","isSensitive",true))));
            step.setPayload(json(payload));start(step);mapping.step(step);
            assertThat(jdbc.queryForList("SELECT old_config::text || new_config::text FROM kf_config_versions WHERE job_id=?",String.class,job.getId().toString()))
                    .hasSize(2).allSatisfy(value->assertThat(value).doesNotContain("OLD_SENSITIVE_VALUE","NEW_SENSITIVE_VALUE"));
        });
    }

    @Test void externalTargetsKeepErHistoryWithoutViolatingTheExistingInternalClusterVersionForeignKey() {
        rollback(() -> {
            Job job=job(JobType.CONFIG_CHANGE,false);job.setType(JobType.TARGETED_CONFIG_CHANGE);
            var step=steps(job,false).get(0);var payload=read(step.getPayload());payload.put("hostId","external:"+UUID.randomUUID());step.setPayload(json(payload));
            // No kf_clusters row: this exercises the FK mismatch, rather than
            // masking it with an internal-cluster fixture.
            mapping.record(job,List.of(step));start(step);mapping.step(step);
            assertThat(count("kf_config_change",job)).isEqualTo(1);
            assertThat(count("kf_config_change_node_value",job)).isEqualTo(2);
            assertThat(versionCount(job)).isZero();
        });
    }

    private long count(String table,Job job){return jdbc.queryForObject("SELECT count(*) FROM "+table+" WHERE change_id=?",Long.class,job.getId());}
    private void rollback(Runnable body){transactions.executeWithoutResult(tx->{tx.setRollbackOnly();body.run();});}
    private static void start(JobStep step){step.setStatus(JobStepStatus.IN_PROGRESS);step.setStartTime(Instant.now());step.setEndTime(null);}
    private Job job(JobType type,boolean file){var job=new Job();job.setId(UUID.randomUUID());job.setType(type);job.setRequestedBy("mapping-test");
        UUID cluster=UUID.randomUUID();
        job.setPayload(json(Map.of("clusterId",cluster.toString(),"role","BROKER","scope","SELECTED","classification",file?"ROLLING_RESTART_REQUIRED":"DYNAMIC_ALLOWED")));
        if(type==JobType.TARGETED_CONFIG_CHANGE) {
            jdbc.update("INSERT INTO kf_clusters (id,cluster_name,created_by,updated_by) VALUES (?,?,?,?)",cluster,"config-mapping-test-"+cluster,"mapping-test","mapping-test");
            jdbc.update("INSERT INTO kf_jobs (id,type,status,payload,requested_by) VALUES (?,?,?,?::jsonb,?)",job.getId().toString(),type.name(),job.getStatus().name(),job.getPayload(),job.getRequestedBy());
        }
        return job;}
    private static List<JobStep> steps(Job job,boolean file){var result=new ArrayList<JobStep>();for(int i=1;i<=2;i++){
        var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setName("Config node "+i);step.setStepOrder(i);step.setTargetId(String.valueOf(i));
        var payload=new LinkedHashMap<String,Object>();payload.put("operation",file?"STATIC":"DYNAMIC");payload.put(file?"nodeId":"brokerId",i);payload.put("hostId","node-"+i);payload.put("role","broker");payload.put("configPath","/etc/kafka/server.properties");
        payload.put("changes",file?Map.of("queued.max.requests","600","socket.request.max.bytes","209715200"):Map.of("log.retention.hours","72","log.retention.bytes","2048"));
        payload.put("expectedValues",file?Map.of("queued.max.requests","500","socket.request.max.bytes","104857600"):Map.of("log.retention.hours","168","log.retention.bytes","-1"));
        step.setPayload(json(payload));result.add(step);
    }return result;}
    private static String json(Object value){try{return new ObjectMapper().writeValueAsString(value);}catch(Exception e){throw new AssertionError(e);}}
    private static Map<String,Object> read(String value){try{return new ObjectMapper().readValue(value,new com.fasterxml.jackson.core.type.TypeReference<LinkedHashMap<String,Object>>(){});}catch(Exception e){throw new AssertionError(e);}}
}
