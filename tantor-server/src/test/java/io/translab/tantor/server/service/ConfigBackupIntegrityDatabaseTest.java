package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class ConfigBackupIntegrityDatabaseTest {
    final DriverManagerDataSource source = new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),
            System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD"));
    final JdbcTemplate jdbc = new JdbcTemplate(source);
    final ObjectMapper mapper = new ObjectMapper();
    final ConfigChangeMappingService mapping = new ConfigChangeMappingService(jdbc,mapper);
    final TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(source));
    final String path="/opt/kafka/config/config-backup/controller.properties/controller.properties.bak.20261103T120000";
    final String hash="a".repeat(64);
    JobStep fixture() {
        Job job=new Job();job.setId(UUID.randomUUID());job.setType(JobType.TARGETED_CONFIG_CHANGE);
        job.setCreatedAt(Instant.now().minusSeconds(60));
        job.setPayload("{\"clusterId\":\""+UUID.randomUUID()+"\"}");
        jdbc.update("INSERT INTO kf_config_change(change_id,status) VALUES (?,'PENDING')",job.getId());
        JobStep step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setName("Backup");
        step.setPayload("{\"operation\":\"STATIC\",\"nodeId\":2,\"role\":\"controller\",\"configPath\":\"/opt/kafka/config/controller.properties\",\"changes\":{\"num.io.threads\":\"8\"}}");
        return step;
    }
    String receipt(String file,String checksum,String timestamp) {
        try { return "TANTOR_CONFIG_BACKUP_VERIFIED "+mapper.writeValueAsString(Map.of("path",file,"checksumSha256",checksum,"verifiedAt",timestamp,"method","SOURCE_COPY_MATCH")); }
        catch(Exception error) { throw new RuntimeException(error); }
    }
    String output(String evidence) { return "Rollback checkpoint (7-day retention): "+path+"\n"+evidence; }
    Map<String,Object> row(JobStep step) { return jdbc.queryForMap("SELECT * FROM kf_config_backup WHERE change_id=? AND backup_uri=?",step.getJob().getId(),path); }
    void rollback(Runnable action) { tx.executeWithoutResult(status->{status.setRollbackOnly();action.run();}); }
    @Test void validReceiptPersistsExactDigestTimeAndMethodAndReplayPreservesThem() {
        rollback(()->{
            JobStep step=fixture();String timestamp=Instant.now().minusSeconds(1).truncatedTo(java.time.temporal.ChronoUnit.MICROS).toString();
            String output=output(receipt(path,hash,timestamp));
            mapping.backup(step,Map.of(),Map.of(),output);
            var original=row(step);
            assertThat(original.get("checksum_sha256")).isEqualTo(hash);
            assertThat(original.get("verification_method")).isEqualTo("SOURCE_COPY_MATCH");
            assertThat(((java.sql.Timestamp)original.get("verified_at")).toInstant()).isEqualTo(Instant.parse(timestamp));
            mapping.backup(step,Map.of(),Map.of(),output);
            assertThat(row(step)).isEqualTo(original);
        });
    }
    @Test void legacyPathAloneDoesNotInventVerification() {
        rollback(()->{JobStep step=fixture();mapping.backup(step,Map.of(),Map.of(),output(""));
            assertThat(row(step).get("checksum_sha256")).isNull();assertThat(row(step).get("verified_at")).isNull();});
    }
    @Test void malformedUnboundAndFutureEvidenceIsIgnored() {
        rollback(()->{JobStep step=fixture();
            for(String evidence:List.of(receipt(path+".other",hash,Instant.now().toString()),receipt(path,"invalid",Instant.now().toString()),
                    receipt(path,hash,"invalid"),receipt(path,hash,Instant.now().plusSeconds(3600).toString()),"TANTOR_CONFIG_BACKUP_VERIFIED {bad}")) {
                mapping.backup(step,Map.of(),Map.of(),output(evidence));assertThat(row(step).get("verified_at")).isNull();
            }
        });
    }
    @Test void differentChecksumCannotReplaceExistingEvidence() {
        rollback(()->{JobStep step=fixture();mapping.backup(step,Map.of(),Map.of(),output(receipt(path,hash,Instant.now().toString())));
            var original=row(step);mapping.backup(step,Map.of(),Map.of(),output(receipt(path,"b".repeat(64),Instant.now().toString())));
            assertThat(row(step)).isEqualTo(original);
        });
    }
    @Test void logicalSnapshotsDoNotReceivePhysicalFileChecksums() {
        rollback(()->{JobStep step=fixture();mapping.backup(step,Map.of(),Map.of("num.io.threads",Map.of("value","8","source","STATIC_BROKER_CONFIG")),output(receipt(path,hash,Instant.now().toString())));
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_config_backup WHERE change_id=? AND backup_uri IS NULL AND checksum_sha256 IS NULL AND verified_at IS NULL",Integer.class,step.getJob().getId())).isEqualTo(1);
            assertThat(row(step).get("checksum_sha256")).isEqualTo(hash);
        });
    }
}
