package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static io.translab.tantor.server.service.KafkaExpansionWorkflow.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** The actual publication method executes through a Spring transaction into PostgreSQL. */
@EnabledIfEnvironmentVariable(named="TANTOR_ADD_NODE_CHECK_URL",matches="jdbc:postgresql://127\\.0\\.0\\.1:65440/.*")
class KafkaExpansionPublicationDatabaseTest {
    @Test void failureAfterBothRoleInsertsRollsBackMembershipHostAndReservations()throws Exception{publication(true);}
    @Test void successfulPublicationCommitsBothRolesHostAndPermanentIdsTogether()throws Exception{publication(false);}
    void publication(boolean fail)throws Exception{
        String url=System.getenv("TANTOR_ADD_NODE_CHECK_URL"),schema="expansion_publish_"+UUID.randomUUID().toString().replace("-","");var admin=new JdbcTemplate(new DriverManagerDataSource(url,"postgres",""));admin.execute("CREATE SCHEMA "+schema);
        try{
            var ds=new DriverManagerDataSource(url+(url.contains("?")?"&":"?")+"currentSchema="+schema,"postgres","");var jdbc=new JdbcTemplate(ds);
            jdbc.execute("CREATE TABLE kf_clusters(id uuid PRIMARY KEY,config_json text);CREATE TABLE kf_hosts(id varchar(255) PRIMARY KEY,cluster_id uuid);CREATE TABLE kf_jobs(id varchar(36) PRIMARY KEY);CREATE TABLE kf_tasks(id uuid PRIMARY KEY,host_id varchar(255),command varchar(64),status varchar(32) DEFAULT 'PENDING');CREATE TABLE kf_job_steps(id uuid PRIMARY KEY);CREATE TABLE kf_cluster_services(id uuid PRIMARY KEY,host_id varchar(255),role text,node_id integer)");
            for(String file:List.of("V105__broker_addition_operation.sql","V106__quorum_expansion_ownership.sql"))try(var stream=getClass().getResourceAsStream("/db/migration/"+file)){assertNotNull(stream);jdbc.execute(new String(stream.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8));}
            var mapper=new ObjectMapper().findAndRegisterModules();var frozen=new KafkaExpansionPlanTest().plan().freeze(mapper);var p=frozen.plan();UUID jobId=UUID.randomUUID();jdbc.update("INSERT INTO kf_clusters VALUES (?,?)",p.clusterId(),"{}");jdbc.update("INSERT INTO kf_hosts VALUES (?,null)",p.candidateHostId());jdbc.update("INSERT INTO kf_jobs VALUES (?)",jobId.toString());jdbc.update("INSERT INTO kf_quorum_expansion_operation(id,cluster_id,host_id,participant_hosts,controller_id,broker_id,job_id,request_key,request_digest,plan,plan_digest,state) VALUES (?,?,?,ARRAY['existing-broker','existing-controller'],102,2,?,'publish-request',?,?,?,'EXECUTING')",p.operationId(),p.clusterId(),p.candidateHostId(),jobId.toString(),"a".repeat(64),frozen.raw(),frozen.digest());
            var ops=mock(KafkaExpansionOperationRepository.class);var clusters=mock(ClusterRepository.class);var hosts=mock(HostRepository.class);var assignments=mock(ClusterServiceAssignmentRepository.class);var jobs=mock(JobService.class);
            var op=new KafkaExpansionOperation();op.setId(p.operationId());op.setClusterId(p.clusterId());op.setHostId(p.candidateHostId());op.setJobId(jobId);op.setPlan(frozen.raw());op.setPlanDigest(frozen.digest());op.setState(KafkaExpansionState.EXECUTING);
            var checkpoints=new Checkpoints(frozen.digest(),frozen.steps(),Map.of());for(var step:frozen.steps()){if(step.kind()==Kind.PUBLISH)break;checkpoints.intent(step.id());checkpoints.confirm(new Evidence(frozen.digest(),step.id(),step.target(),Instant.now(),step.kind().requiredFacts()),Instant.now());}op.setCheckpoints(mapper.writeValueAsString(checkpoints.snapshot()));
            when(ops.findById(op.getId())).thenReturn(Optional.of(op));when(ops.findForUpdate(op.getId())).thenReturn(Optional.of(op));
            var cluster=new Cluster();cluster.setId(p.clusterId());cluster.setKafkaVersion(p.kafkaVersion());cluster.setKafkaClusterId(p.kafkaClusterId());cluster.setConfigJson("{}");cluster.setNodeIds(List.of(1,101));cluster.setBootstrapServers("192.0.2.1:9092");when(clusters.findWithServicesById(p.clusterId())).thenReturn(Optional.of(cluster));
            var host=new Host();host.setId(p.candidateHostId());when(hosts.findById(host.getId())).thenReturn(Optional.of(host));var inserted=new AtomicInteger();
            when(assignments.saveAndFlush(any())).thenAnswer(call->{ClusterServiceAssignment assignment=call.getArgument(0);jdbc.update("INSERT INTO kf_cluster_services VALUES (?,?,?,?)",UUID.randomUUID(),assignment.getHostId(),assignment.getRole(),assignment.getNodeId());if(inserted.incrementAndGet()==2&&fail)throw new IllegalStateException("injected publication interruption");return assignment;});
            when(hosts.saveAndFlush(any())).thenAnswer(call->{Host value=call.getArgument(0);jdbc.update("UPDATE kf_hosts SET cluster_id=? WHERE id=?",value.getClusterId(),value.getId());return value;});
            when(clusters.saveAndFlush(any())).thenAnswer(call->{Cluster value=call.getArgument(0);jdbc.update("UPDATE kf_clusters SET config_json=? WHERE id=?",value.getConfigJson(),value.getId());return value;});
            when(ops.saveAndFlush(any())).thenAnswer(call->{KafkaExpansionOperation value=call.getArgument(0);jdbc.update("UPDATE kf_quorum_expansion_operation SET state=? WHERE id=?",value.getState().name(),value.getId());return value;});
            var target=new KafkaExpansionLedger(ops,clusters,hosts,assignments,mock(JobStepRepository.class),mock(TaskRepository.class),jobs,jdbc,mapper,new AddNodeInspectionProperties());var factory=new ProxyFactory(target);factory.setProxyTargetClass(true);factory.addAdvice(new TransactionInterceptor(new DataSourceTransactionManager(ds),new AnnotationTransactionAttributeSource()));var ledger=(KafkaExpansionLedger)factory.getProxy();var publish=checkpoints.next().orElseThrow();var proof=new Evidence(frozen.digest(),publish.id(),publish.target(),Instant.now(),Set.of("ALL_PLANNED_ROLES_VERIFIED"));
            if(fail)assertThrows(IllegalStateException.class,()->ledger.publish(op.getId(),proof));else ledger.publish(op.getId(),proof);
            assertEquals(fail?0:2,jdbc.queryForObject("SELECT count(*) FROM kf_cluster_services",Integer.class));assertEquals(fail?0:1,jdbc.queryForObject("SELECT count(*) FROM kf_hosts WHERE cluster_id IS NOT NULL",Integer.class));assertEquals(fail?"EXECUTING":"SUCCEEDED",jdbc.queryForObject("SELECT state FROM kf_quorum_expansion_operation",String.class));assertEquals(fail?6:2,jdbc.queryForObject("SELECT count(*) FROM kf_add_node_resource_reservation",Integer.class));
            if(fail)assertEquals("{}",jdbc.queryForObject("SELECT config_json FROM kf_clusters",String.class));else assertTrue(jdbc.queryForObject("SELECT config_json FROM kf_clusters",String.class).contains(p.candidateIp()));
        }finally{admin.execute("DROP SCHEMA "+schema+" CASCADE");}
    }
}
