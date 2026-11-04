package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class DeletedClusterInventoryDatabaseTest {
    final JdbcTemplate jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD")));
    final TransactionTemplate tx=new TransactionTemplate(new DataSourceTransactionManager(jdbc.getDataSource()));
    record Fixture(UUID cluster,String host,UUID service,UUID node,UUID connection){}
    Fixture fixture() {
        UUID cluster=UUID.randomUUID(),service=UUID.randomUUID(),node=UUID.randomUUID(),connection=UUID.randomUUID();String host="deletion-check-"+UUID.randomUUID();
        jdbc.update("INSERT INTO kf_clusters(id,cluster_name,status,updated_by) VALUES (?,?,'SUCCESS','delete-requester')",cluster,"cleanup-"+cluster);
        jdbc.update("INSERT INTO kf_hosts(id,hostname,status,cluster_id) VALUES (?,?,'OCCUPIED',?)",host,host,cluster);
        jdbc.update("INSERT INTO kf_cluster_services(id,cluster_id,host_id,role,status,config_json) VALUES (?,?,?,'kafka_connect','ONLINE','{\"rest_port\":8083,\"host_name\":\"192.168.3.229\"}')",service,cluster,host);
        jdbc.update("INSERT INTO kf_nodes(id,cluster_id,host_id,status,role) VALUES (?,?,?,'OCCUPIED','broker')",node,cluster,host);
        jdbc.update("INSERT INTO kf_add_service(add_service_id,cluster_id,status) VALUES (?,?,'ONLINE')",service,cluster);
        jdbc.update("INSERT INTO kf_data_service_connections(id,cluster_id,service_type,host,port,status,is_active,is_default) VALUES (?,?,'KAFKA_CONNECT','192.168.3.229',8083,'ONLINE',true,true)",connection,cluster);
        return new Fixture(cluster,host,service,node,connection);
    }
    void rollback(Runnable body){tx.executeWithoutResult(status->{status.setRollbackOnly();body.run();jdbc.execute("SET CONSTRAINTS ALL IMMEDIATE");});}
    void retired(Fixture f) {
        for(String table:List.of("kf_cluster_services","kf_nodes","kf_add_service"))
            assertThat(jdbc.queryForObject("SELECT count(*) FROM "+table+" WHERE cluster_id=? AND status IS DISTINCT FROM 'DELETED'",Integer.class,f.cluster())).isZero();
        assertThat(jdbc.queryForMap("SELECT status,is_active,is_default FROM kf_data_service_connections WHERE id=?",f.connection())).containsEntry("status","DELETED").containsEntry("is_active",false).containsEntry("is_default",false);
        assertThat(jdbc.queryForMap("SELECT status,cluster_id FROM kf_hosts WHERE id=?",f.host())).containsEntry("status","ONLINE").containsEntry("cluster_id",null);
    }
    @Test void realReservationRepositoryQueriesExcludeDeletedParents() {
        // Committed fixtures remain only in the disposable check database. The JPA config also validates all repository queries.
        var retired=fixture();var active=fixture();jdbc.update("UPDATE kf_clusters SET status='DELETED' WHERE id=?",retired.cluster());
        try(var context=new org.springframework.context.annotation.AnnotationConfigApplicationContext(RollingRestartMappingDatabaseTest.Config.class)) {
            var emf=context.getBean(jakarta.persistence.EntityManagerFactory.class);
            var repositories=new org.springframework.data.jpa.repository.support.JpaRepositoryFactory(org.springframework.orm.jpa.SharedEntityManagerCreator.createSharedEntityManager(emf));
            var assignments=repositories.getRepository(io.translab.tantor.server.repository.ClusterServiceAssignmentRepository.class).findReservationsForLiveClusters();
            assertThat(assignments.stream().map(io.translab.tantor.server.domain.ClusterServiceAssignment::getId)).contains(active.service()).doesNotContain(retired.service());
            assertThat(repositories.getRepository(io.translab.tantor.server.repository.ExternalDataServiceRepository.class).findLiveReservationsByAgentId("unused-check-agent")).isEmpty();
        }
    }
    @Test void finalDeletionRetiresAllInventoryButKeepsTheAgentConnectedAndHistoryPresent(){rollback(()->{
        var target=fixture();var other=fixture();jdbc.update("UPDATE kf_clusters SET status='DELETED' WHERE id=?",target.cluster());retired(target);
        assertThat(jdbc.queryForObject("SELECT status FROM kf_cluster_services WHERE id=?",String.class,other.service())).isEqualTo("ONLINE");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_nodes WHERE id=?",Integer.class,target.node())).isEqualTo(1);
    });}
    @Test void pendingOrFailedCleanupKeepsReservationsUntilSuccessfulDeletion(){rollback(()->{
        var f=fixture();for(String status:List.of("DELETING","DELETE_FAILED")){
            jdbc.update("UPDATE kf_clusters SET status=? WHERE id=?",status,f.cluster());
            assertThat(jdbc.queryForObject("SELECT is_active FROM kf_data_service_connections WHERE id=?",Boolean.class,f.connection())).isTrue();
        }
        jdbc.update("UPDATE kf_clusters SET status='DELETED' WHERE id=?",f.cluster());retired(f);
    });}
    @Test void lateCallbacksAndHeartbeatsCannotResurrectDeletedInventory(){rollback(()->{
        var f=fixture();jdbc.update("UPDATE kf_clusters SET status='DELETED' WHERE id=?",f.cluster());
        jdbc.update("UPDATE kf_cluster_services SET status='ONLINE' WHERE id=?",f.service());
        jdbc.update("UPDATE kf_nodes SET status='ONLINE' WHERE id=?",f.node());
        jdbc.update("UPDATE kf_add_service SET status='ONLINE' WHERE add_service_id=?",f.service());
        jdbc.update("UPDATE kf_data_service_connections SET status='ONLINE',is_active=true,is_default=true WHERE id=?",f.connection());
        jdbc.update("UPDATE kf_hosts SET status='OCCUPIED',cluster_id=? WHERE id=?",f.cluster(),f.host());retired(f);
    });}
    @Test void repairedInventoryDoesNotReservePortsAndActiveParentsStillDo(){rollback(()->{
        var f=fixture();jdbc.update("UPDATE kf_clusters SET status='DELETED' WHERE id=?",f.cluster());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_cluster_services s JOIN kf_clusters c ON c.id=s.cluster_id WHERE s.id=? AND c.status IS DISTINCT FROM 'DELETED'",Integer.class,f.service())).isZero();
        jdbc.queryForList("SELECT retire_deleted_cluster_inventory(?)",f.cluster());retired(f);
    });}
    @Test void retiredConnectionsCannotBeReactivatedThroughNewInsert(){rollback(()->{
        var f=fixture();jdbc.update("UPDATE kf_clusters SET status='DELETED' WHERE id=?",f.cluster());
        jdbc.update("INSERT INTO kf_data_service_connections(id,cluster_id,service_type,host,port,status,is_active) VALUES (?,?,'SCHEMA_REGISTRY','192.168.3.229',8081,'ONLINE',true)",UUID.randomUUID(),f.cluster());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_data_service_connections WHERE cluster_id=? AND is_active",Integer.class,f.cluster())).isZero();
    });}
}
