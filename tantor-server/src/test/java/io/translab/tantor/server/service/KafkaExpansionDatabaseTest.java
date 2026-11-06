package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** All writes roll back in a dedicated temporary loopback database. */
@EnabledIfEnvironmentVariable(named="TANTOR_ADD_NODE_CHECK_URL",matches="jdbc:postgresql://127\\.0\\.0\\.1:65440/.*")
class KafkaExpansionDatabaseTest {
    @Test void sharedOwnershipPreventsBrokerExpansionRacesAndSurvivesRecovery()throws Exception{
        try(var c=connection()) {c.setAutoCommit(false);try{
            schema(c);UUID cluster=cluster(c),otherCluster=cluster(c);UUID expansion=UUID.randomUUID();insertExpansion(c,expansion,cluster,"candidate",102,2);
            assertEquals(6,count(c,"SELECT count(*) FROM kf_add_node_resource_reservation WHERE operation_id='"+expansion+"'"));
            blocked(c,()->insertBroker(c,UUID.randomUUID(),cluster,"other-host",3));
            blocked(c,()->insertBroker(c,UUID.randomUUID(),otherCluster,"participant",3));
            blocked(c,()->sql(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+UUID.randomUUID()+"','participant','RESTART_SERVICE')"));
            blocked(c,()->sql(c,"INSERT INTO kf_cluster_services VALUES ('"+UUID.randomUUID()+"','candidate')"));
            sql(c,"UPDATE kf_quorum_expansion_operation SET state='RECOVERY_REQUIRED' WHERE id='"+expansion+"'");
            assertEquals(6,count(c,"SELECT count(*) FROM kf_add_node_resource_reservation WHERE operation_id='"+expansion+"'"));
            blocked(c,()->sql(c,"UPDATE kf_quorum_expansion_operation SET controller_id=103 WHERE id='"+expansion+"'"));
            blocked(c,()->sql(c,"UPDATE kf_quorum_expansion_operation SET plan='changed' WHERE id='"+expansion+"'"));
            sql(c,"UPDATE kf_quorum_expansion_operation SET state='SUCCEEDED' WHERE id='"+expansion+"'");
            assertEquals(2,count(c,"SELECT count(*) FROM kf_add_node_resource_reservation WHERE operation_id='"+expansion+"'"));
            blocked(c,()->insertBroker(c,UUID.randomUUID(),cluster,"other-host",2));
            insertBroker(c,UUID.randomUUID(),cluster,"other-host",3);
            sql(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+UUID.randomUUID()+"','participant','RESTART_SERVICE')");
            blocked(c,()->sql(c,"UPDATE kf_quorum_expansion_operation SET state='ACCEPTED' WHERE id='"+expansion+"'"));
        }finally{c.rollback();}}
    }
    @Test void existingBrokerReservationsAreBackfilledAndSharedLeasesReleaseOnlyWhenSafe()throws Exception{
        try(var c=connection()){c.setAutoCommit(false);try{
            bootstrap(c);migration(c,"V105__broker_addition_operation.sql");UUID cluster=cluster(c),broker=UUID.randomUUID();insertBroker(c,broker,cluster,"candidate",2);migration(c,"V106__quorum_expansion_ownership.sql");
            assertEquals(3,count(c,"SELECT count(*) FROM kf_add_node_resource_reservation WHERE operation_id='"+broker+"'"));
            blocked(c,()->insertExpansion(c,UUID.randomUUID(),cluster,"other-host",102,3));
            sql(c,"UPDATE kf_broker_addition_operation SET state='CANCELLED_SAFE' WHERE id='"+broker+"'");
            assertEquals(0,count(c,"SELECT count(*) FROM kf_add_node_resource_reservation WHERE operation_id='"+broker+"'"));
            UUID expansion=UUID.randomUUID();insertExpansion(c,expansion,cluster,"other-host",102,2);sql(c,"UPDATE kf_quorum_expansion_operation SET state='CANCELLED_SAFE' WHERE id='"+expansion+"'");
            assertEquals(0,count(c,"SELECT count(*) FROM kf_add_node_resource_reservation WHERE operation_id='"+expansion+"'"));
            insertBroker(c,UUID.randomUUID(),cluster,"candidate",2);
        }finally{c.rollback();}}
    }
    @Test void ownedTaskCallbacksAreAllowedButMutationPreventsSafeCancellation()throws Exception{
        try(var c=connection()){c.setAutoCommit(false);try{
            schema(c);UUID cluster=cluster(c),operation=UUID.randomUUID();insertExpansion(c,operation,cluster,"candidate",102,null);UUID task=UUID.randomUUID(),step=UUID.randomUUID();
            sql(c,"SELECT set_config('tantor.quorum_expansion_id','"+operation+"',true)");
            sql(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+task+"','candidate','EXECUTE_QUORUM_CANDIDATE');INSERT INTO kf_job_steps VALUES ('"+step+"')");
            sql(c,"INSERT INTO kf_quorum_expansion_action(task_id,operation_id,step_id,workflow_step,command,action,mutation) VALUES ('"+task+"','"+operation+"','"+step+"','step','EXECUTE_QUORUM_CANDIDATE','PREPARE',true)");
            sql(c,"SELECT set_config('tantor.quorum_expansion_id','',true)");
            sql(c,"UPDATE kf_tasks SET status='IN_PROGRESS' WHERE id='"+task+"'");
            blocked(c,()->sql(c,"UPDATE kf_quorum_expansion_operation SET state='CANCELLED_SAFE' WHERE id='"+operation+"'"));
            sql(c,"UPDATE kf_tasks SET status='SUCCESS' WHERE id='"+task+"'");
            blocked(c,()->sql(c,"INSERT INTO kf_tasks(id,host_id,command) VALUES ('"+UUID.randomUUID()+"','participant','INSTALL_SCHEMA')"));
        }finally{c.rollback();}}
    }
    private Connection connection()throws SQLException{return DriverManager.getConnection(System.getenv("TANTOR_ADD_NODE_CHECK_URL"),"postgres","");}
    private void schema(Connection c)throws Exception{bootstrap(c);migration(c,"V105__broker_addition_operation.sql");migration(c,"V106__quorum_expansion_ownership.sql");}
    private void bootstrap(Connection c)throws SQLException{String schema="expansion_check_"+UUID.randomUUID().toString().replace("-","");sql(c,"CREATE SCHEMA "+schema);sql(c,"SET search_path TO "+schema);sql(c,"CREATE TABLE kf_clusters(id uuid PRIMARY KEY);CREATE TABLE kf_hosts(id varchar(255) PRIMARY KEY,cluster_id uuid);CREATE TABLE kf_jobs(id varchar(36) PRIMARY KEY);CREATE TABLE kf_tasks(id uuid PRIMARY KEY,host_id varchar(255),command varchar(64),status varchar(32) DEFAULT 'PENDING');CREATE TABLE kf_job_steps(id uuid PRIMARY KEY);CREATE TABLE kf_cluster_services(id uuid PRIMARY KEY,host_id varchar(255));INSERT INTO kf_hosts(id) VALUES ('candidate'),('participant'),('other-host');");}
    private void migration(Connection c,String file)throws Exception{try(var stream=getClass().getResourceAsStream("/db/migration/"+file)){assertNotNull(stream);sql(c,new String(stream.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));}}
    private UUID cluster(Connection c)throws SQLException{UUID id=UUID.randomUUID();sql(c,"INSERT INTO kf_clusters VALUES ('"+id+"')");return id;}
    private void insertExpansion(Connection c,UUID id,UUID cluster,String host,int controller,Integer broker)throws SQLException{UUID job=UUID.randomUUID();sql(c,"INSERT INTO kf_jobs VALUES ('"+job+"')");sql(c,"INSERT INTO kf_quorum_expansion_operation(id,cluster_id,host_id,participant_hosts,controller_id,broker_id,job_id,request_key,request_digest,plan,plan_digest) VALUES ('"+id+"','"+cluster+"','"+host+"',ARRAY['participant','other-participant'],"+controller+","+broker+",'"+job+"','request-"+id+"','"+"a".repeat(64)+"','{}','"+"b".repeat(64)+"')");}
    private void insertBroker(Connection c,UUID id,UUID cluster,String host,int node)throws SQLException{UUID job=UUID.randomUUID();sql(c,"INSERT INTO kf_jobs VALUES ('"+job+"')");sql(c,"INSERT INTO kf_broker_addition_operation(id,cluster_id,host_id,job_id,request_key,request_digest,node_id,state,plan) VALUES ('"+id+"','"+cluster+"','"+host+"','"+job+"','request-"+id+"','"+"a".repeat(64)+"',"+node+",'ACCEPTED','{}')");}
    private static void sql(Connection c,String text)throws SQLException{try(var s=c.createStatement()){s.execute(text);}}
    private static int count(Connection c,String text)throws SQLException{try(var s=c.createStatement();var r=s.executeQuery(text)){assertTrue(r.next());return r.getInt(1);}}
    private interface SQLAction{void run()throws SQLException;}
    private static void blocked(Connection c,SQLAction action)throws SQLException{var sp=c.setSavepoint();assertThrows(SQLException.class,action::run);c.rollback(sp);}
}
