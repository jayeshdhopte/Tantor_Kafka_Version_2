package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.*;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.*;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** Explicit local-only integration: all summary and audit fixture writes are rolled back. */
@EnabledIfEnvironmentVariable(named="TANTOR_LOCAL_RESTART_DB_TEST",matches="true")
class RollingRestartMappingDatabaseTest {
    @Configuration
    @EnableTransactionManagement
    @EnableJpaRepositories(basePackageClasses=AuditLogRepository.class)
    static class Config {
        @Bean DataSource dataSource() {
            String override=System.getenv("TANTOR_STATUS_AUDIT_TEST_URL");
            if(override!=null) {
                assertThat(override).contains("_check_");
                return new DriverManagerDataSource(override,System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD"));
            }
            return new DriverManagerDataSource("jdbc:postgresql://127.0.0.1:5433/tantor","tantor_dev",System.getenv("TANTOR_LOCAL_DB_PASSWORD"));
        }
        @Bean LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource source) {
            var factory=new LocalContainerEntityManagerFactoryBean(); factory.setDataSource(source);
            factory.setPackagesToScan("io.translab.tantor.server.domain");
            factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            factory.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto","none","hibernate.physical_naming_strategy","org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy")); return factory;
        }
        @Bean PlatformTransactionManager transactionManager(EntityManagerFactory factory) { return new JpaTransactionManager(factory); }
        @Bean JdbcTemplate jdbc(DataSource source) { return new JdbcTemplate(source); }
        @Bean ObjectMapper mapper() { return new ObjectMapper(); }
    }

    @Test void summaryAndAuditRowsRoundTripInsideARolledBackTransaction() throws Exception {
        try(var context=new AnnotationConfigApplicationContext(Config.class)) {
            var jdbc=context.getBean(JdbcTemplate.class);
            var pending=new ArrayList<RollingRestartMappingService.AuditEvent>();
            // Direct audit writer joins this test transaction. Production AFTER_COMMIT delivery is tested separately.
            var audit=new AuditService(context.getBean(AuditLogRepository.class),context.getBean(ObjectMapper.class));
            var mapping=new RollingRestartMappingService(jdbc,context.getBean(ObjectMapper.class),mock(ExternalClusterRepository.class),
                    event -> pending.add((RollingRestartMappingService.AuditEvent) event),audit);
            var transaction=new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            var cluster=new Cluster();cluster.setId(UUID.randomUUID());cluster.setName("restart-mapping-test");
            cluster.setMode("kraft");cluster.setEnvironment("TEST");cluster.setBootstrapServers("a:9092,b:9092");
            var job=new Job();job.setId(UUID.randomUUID());job.setType(JobType.ROLLING_RESTART);job.setRequestedBy("test-creator");
            var first=step("a","broker",1);var second=step("b","controller",2);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_audit_logs WHERE cluster_id=?",Long.class,cluster.getId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_table WHERE cluster_id=?",Long.class,cluster.getId())).isZero();
            transaction.executeWithoutResult(tx -> {
                tx.setRollbackOnly();
                jdbc.update("INSERT INTO kf_clusters(id,cluster_name,status) VALUES (?,?,'SUCCESS')",cluster.getId(),cluster.getName());
                jdbc.update("INSERT INTO kf_jobs(id,type,status,resource_key,payload,requested_by) VALUES (?,'ROLLING_RESTART','PENDING',?,'{}'::jsonb,?)",
                        job.getId().toString(),"cluster:"+cluster.getId(),job.getRequestedBy());
                mapping.record(job,cluster,List.of(first,second));
                flushAudit(mapping,pending);
                var original=jdbc.queryForMap("SELECT * FROM kf_rolling_restart_table WHERE unique_id=?",job.getId());
                assertThat(original).hasSize(19).containsEntry("user","test-creator").containsEntry("bootstrap_server_ip","a:9092,b:9092");
                assertThat(original.get("log_path")).isNull();assertThat(original.get("old_config_path")).isNull();
                assertThat(original.get("host_id").toString()).contains("a","b");
                assertThat(original.get("role_per_node").toString()).contains("controller","broker");
                mapping.record(job,cluster,List.of(first,second));flushAudit(mapping,pending);
                assertThat(audits(jdbc,cluster.getId())).isEqualTo(1);
                job.setStatus(JobStatus.FAILED);mapping.synchronizeStatus(job,null,"ROLLING_RESTART_FAILED");
                job.setRetryCount(1);job.setStatus(JobStatus.PENDING);mapping.synchronizeStatus(job,"test-retry-user","ROLLING_RESTART_RETRY_REQUESTED");
                job.setStatus(JobStatus.IN_PROGRESS);mapping.synchronizeStatus(job,null,"ROLLING_RESTART_STARTED");
                job.setStatus(JobStatus.SUCCESS);mapping.synchronizeStatus(job,null,"ROLLING_RESTART_COMPLETED");
                mapping.synchronizeStatus(job,null,"ROLLING_RESTART_COMPLETED");
                flushAudit(mapping,pending);
                var completed=jdbc.queryForMap("SELECT * FROM kf_rolling_restart_table WHERE unique_id=?",job.getId());
                assertThat(completed).containsEntry("status","SUCCESS").containsEntry("user","test-creator")
                        .containsEntry("updated_by","test-retry-user").containsEntry("created_at",original.get("created_at"));
                assertThat(audits(jdbc,cluster.getId())).isEqualTo(5);
                var outcome=jdbc.queryForMap("SELECT created_by,status,details::jsonb->>'restartStatus' AS restart_status FROM kf_audit_logs WHERE cluster_id=? AND action='ROLLING_RESTART_COMPLETED'",cluster.getId());
                assertThat(outcome).containsEntry("created_by","test-retry-user").containsEntry("status","SUCCESS").containsEntry("restart_status","SUCCESS");
                var legacy=new Job();legacy.setId(UUID.randomUUID());legacy.setType(JobType.ROLLING_RESTART);
                mapping.synchronizeStatus(legacy,null,"ROLLING_RESTART_RECOVERED");
                flushAudit(mapping,pending);
                assertThat(audits(jdbc,cluster.getId())).isEqualTo(5);
                jdbc.execute("SET CONSTRAINTS ALL IMMEDIATE");
            });
            assertThat(audits(jdbc,cluster.getId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_rolling_restart_table WHERE cluster_id=?",Long.class,cluster.getId())).isZero();
        }
    }
    private static void flushAudit(RollingRestartMappingService mapping,List<RollingRestartMappingService.AuditEvent> pending) {
        pending.forEach(mapping::recordAudit);pending.clear();
    }
    private static long audits(JdbcTemplate jdbc,UUID cluster) {
        return jdbc.queryForObject("SELECT count(*) FROM kf_audit_logs WHERE cluster_id=?",Long.class,cluster);
    }
    private static JobStep step(String host,String role,int node) throws Exception {
        var step=new JobStep();step.setId(UUID.randomUUID());step.setTargetId(host);
        step.setPayload(new ObjectMapper().writeValueAsString(Map.of("hostId",host,"role",role,"nodeId",node,"serviceName","kafka.service")));
        return step;
    }
}
