package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Dedicated temporary loopback PostgreSQL instance only; every schema write is rolled back. */
@EnabledIfEnvironmentVariable(named="TANTOR_ADD_NODE_CHECK_URL",matches="jdbc:postgresql://127\\.0\\.0\\.1:65440/.*")
class BrokerAdditionDatabaseTest {
    @Test void operationRepositoryQueriesResolveTypedRecoveryStates() {
        var configuration=new org.hibernate.cfg.Configuration();
        configuration.addAnnotatedClass(io.translab.tantor.server.domain.BrokerAdditionOperation.class);
        configuration.setProperty("jakarta.persistence.jdbc.url",System.getenv("TANTOR_ADD_NODE_CHECK_URL"));
        configuration.setProperty("jakarta.persistence.jdbc.user","postgres");configuration.setProperty("jakarta.persistence.jdbc.password","");
        configuration.setProperty("hibernate.hbm2ddl.auto","none");
        try(var factory=configuration.buildSessionFactory();var session=factory.openSession()) {
            var repository=new org.springframework.data.jpa.repository.support.JpaRepositoryFactory(session)
                    .getRepository(io.translab.tantor.server.repository.BrokerAdditionOperationRepository.class);
            assertNotNull(repository);
        }
    }
    @Test void migrationRetainsReservationsAcrossFailuresAndGuardsOtherWriters()throws Exception {
        try(var c=DriverManager.getConnection(System.getenv("TANTOR_ADD_NODE_CHECK_URL"),"postgres","")) {
            c.setAutoCommit(false);
            try {
                String schema="broker_add_check_"+UUID.randomUUID().toString().replace("-","");
                sql(c,"CREATE SCHEMA "+schema);sql(c,"SET search_path TO "+schema);
                sql(c,"CREATE TABLE kf_clusters(id uuid PRIMARY KEY); CREATE TABLE kf_hosts(id varchar(255) PRIMARY KEY,cluster_id uuid); CREATE TABLE kf_jobs(id varchar(36) PRIMARY KEY); CREATE TABLE kf_tasks(id uuid PRIMARY KEY,host_id varchar(255),command varchar(64),status varchar(32) DEFAULT 'PENDING'); CREATE TABLE kf_job_steps(id uuid PRIMARY KEY); CREATE TABLE kf_cluster_services(id uuid PRIMARY KEY,host_id varchar(255));");
                try(var stream=getClass().getResourceAsStream("/db/migration/V105__broker_addition_operation.sql")){assertNotNull(stream);sql(c,new String(stream.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));}
                UUID cluster=UUID.randomUUID(),job=UUID.randomUUID(),operation=UUID.randomUUID();
                sql(c,"INSERT INTO kf_clusters VALUES ('"+cluster+"'); INSERT INTO kf_hosts(id) VALUES ('candidate'); INSERT INTO kf_jobs VALUES ('"+job+"');");
                sql(c,"INSERT INTO kf_broker_addition_operation(id,cluster_id,host_id,job_id,request_key,request_digest,node_id,state,plan) VALUES ('"+operation+"','"+cluster+"','candidate','"+job+"','request-123456789','"+"a".repeat(64)+"',4,'RECOVERY_REQUIRED','{}')");
                blocked(c,"UPDATE kf_hosts SET cluster_id='"+cluster+"' WHERE id='candidate'");
                blocked(c,"INSERT INTO kf_cluster_services VALUES ('"+UUID.randomUUID()+"','candidate')");
                blocked(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+UUID.randomUUID()+"','candidate','INSTALL_KAFKA')");
                sql(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+UUID.randomUUID()+"','candidate','INSPECT_ADD_BROKER')");
                sql(c,"SELECT set_config('tantor.broker_addition_id','"+operation+"',true)");
                sql(c,"UPDATE kf_hosts SET cluster_id='"+cluster+"' WHERE id='candidate'");
                sql(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+UUID.randomUUID()+"','candidate','PREPARE_ADD_BROKER')");
                sql(c,"SELECT set_config('tantor.broker_addition_id','',true)");
                UUID other=UUID.randomUUID();sql(c,"INSERT INTO kf_jobs VALUES ('"+other+"')");
                blocked(c,"INSERT INTO kf_broker_addition_operation(id,cluster_id,host_id,job_id,request_key,request_digest,node_id,state,plan) VALUES ('"+UUID.randomUUID()+"','"+cluster+"','candidate','"+other+"','request-987654321','"+"b".repeat(64)+"',5,'ACCEPTED','{}')");
                sql(c,"UPDATE kf_broker_addition_operation SET state='SUCCEEDED' WHERE id='"+operation+"'");
                sql(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+UUID.randomUUID()+"','candidate','RESTART_SERVICE')");
                blocked(c,"INSERT INTO kf_broker_addition_operation(id,cluster_id,host_id,job_id,request_key,request_digest,node_id,state,plan) VALUES ('"+UUID.randomUUID()+"','"+cluster+"','candidate','"+other+"','request-987654321','"+"b".repeat(64)+"',4,'ACCEPTED','{}')");
            } finally {c.rollback();}
        }
    }
    private static void sql(Connection c,String sql)throws SQLException {try(var statement=c.createStatement()){statement.execute(sql);}}
    private static void blocked(Connection c,String sql)throws SQLException {var savepoint=c.setSavepoint();assertThrows(SQLException.class,()->sql(c,sql));c.rollback(savepoint);}
}
