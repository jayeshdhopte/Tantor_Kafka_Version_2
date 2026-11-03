package io.translab.tantor.server.service;

import io.translab.tantor.server.repository.JobRepository;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.jpa.repository.Query;
import java.sql.*;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "TANTOR_STATUS_AUDIT_TEST_URL", matches = ".+_check_.+")
class ConfigRecoverySupersessionDatabaseTest {
    Connection connection;
    final String resource = "cluster:" + UUID.randomUUID();
    String original;

    @BeforeEach void open() throws Exception {
        connection = DriverManager.getConnection(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),
                System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"), System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD"));
        connection.setAutoCommit(false);
        original = job(resource, "ROLLBACK_FAILED", 0);
    }
    @AfterEach void close() throws Exception {
        if (connection != null) { connection.rollback(); connection.close(); }
    }
    String job(String key, String status, int seconds) throws Exception {
        String id = UUID.randomUUID().toString();
        try (var stmt = connection.prepareStatement("INSERT INTO kf_jobs(id,type,status,resource_key,created_at) VALUES (?,'TARGETED_CONFIG_CHANGE',?,?,timestamp '2026-11-01 17:25:00' + ? * interval '1 second')")) {
            stmt.setString(1,id); stmt.setString(2,status); stmt.setString(3,key); stmt.setInt(4,seconds); stmt.executeUpdate();
        }
        return id;
    }
    String newer() throws Exception {
        String query = JobRepository.class.getMethod("findNewerAppliedConfiguration", UUID.class).getAnnotation(Query.class).value();
        try (var stmt = connection.prepareStatement(query.replace(":id", "?"))) {
            stmt.setString(1,original);
            try (var rows = stmt.executeQuery()) { return rows.next() ? rows.getString("id") : null; }
        }
    }
    @Test void newerSuccessBlocksEvenIfItReappliesIdenticalValues() throws Exception {
        String applied = job(resource,"SUCCESS",1);
        assertThat(newer()).isEqualTo(applied);
    }
    @Test void excludesOtherClustersOlderVersionsAndFullyRolledBackChanges() throws Exception {
        job(resource,"SUCCESS",-1); job("cluster:"+UUID.randomUUID(),"SUCCESS",10);
        job(resource,"ROLLED_BACK",5); job(resource,"FAILED",6);
        assertThat(newer()).isNull();
    }
    @Test void partiallyAppliedNewerJobAlsoBlocksUntilItsRecoveryCompletes() throws Exception {
        String partial = job(resource,"FAILED",1);
        try (var stmt = connection.prepareStatement("INSERT INTO kf_job_steps(id,job_id,step_order,name,status,payload) VALUES (?, ?,1,'test','FAILED','{\"configMutationStarted\":true}'::jsonb)")) {
            stmt.setObject(1,UUID.randomUUID()); stmt.setString(2,partial); stmt.executeUpdate();
        }
        assertThat(newer()).isEqualTo(partial);
        try (var stmt = connection.prepareStatement("UPDATE kf_jobs SET status='ROLLED_BACK' WHERE id=?")) {
            stmt.setString(1,partial); stmt.executeUpdate();
        }
        assertThat(newer()).isNull();
    }
}
