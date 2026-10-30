package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfigChangeMappingServiceTest {
    final JdbcTemplate jdbc=mock(JdbcTemplate.class);
    final ConfigChangeMappingService mapping=new ConfigChangeMappingService(jdbc,new ObjectMapper());

    Job job(JobType type){var job=new Job();job.setId(UUID.randomUUID());job.setType(type);job.setRequestedBy("operator");job.setPayload("{\"clusterId\":\""+UUID.randomUUID()+"\",\"role\":\"BROKER\",\"classification\":\"DYNAMIC_ALLOWED\"}");return job;}
    JobStep step(Job job,String key){var step=new JobStep();step.setId(UUID.randomUUID());step.setJob(job);step.setName("Apply "+key);step.setStepOrder(1);step.setPayload("{\"operation\":\"DYNAMIC\",\"brokerId\":1,\"hostId\":\"host-a\",\"role\":\"broker_controller\",\"configPath\":\"/etc/kafka/server.properties\",\"key\":\""+key+"\",\"value\":\"NEVER_COPY_IF_SECRET\",\"expectedOldValue\":\"OLD_SECRET\"}");return step;}
    void catalog(){when(jdbc.queryForObject(anyString(),eq(Long.class),any(Object[].class))).thenReturn(10L);}

    @Test void inventoryUuidIsUsedInsteadOfTheNumericKafkaNodeId() {
        catalog();UUID inventory=UUID.randomUUID();
        when(jdbc.queryForList(anyString(),any(Object[].class))).thenAnswer(call -> ((String)call.getArgument(0)).contains("FROM kf_nodes")?List.of(Map.of("id",inventory,"role","broker_controller")):List.of());
        var job=job(JobType.TARGETED_CONFIG_CHANGE);mapping.record(job,List.of(step(job,"log.retention.hours")));
        var target=inserted("kf_config_change_target_node");
        assertThat(target.get("node_id")).isEqualTo(inventory);
        assertThat(target.get("frozen_broker_role")).isEqualTo(true);
        assertThat(target.get("frozen_controller_role")).isEqualTo(true);
    }
    @Test void noInventoryMatchLeavesTheUuidEmptyWithoutInventingAnIdentity() {
        catalog();var job=job(JobType.TARGETED_CONFIG_CHANGE);mapping.record(job,List.of(step(job,"log.retention.hours")));
        assertThat(inserted("kf_config_change_target_node").get("node_id")).isNull();
    }
    @Test void secretKeysAreRedactedInEveryCopiedInsertArgument() {
        catalog();when(jdbc.queryForObject(anyString(),eq(Boolean.class),any(Object[].class))).thenAnswer(call -> !((String)call.getArgument(0)).contains("SELECT EXISTS(SELECT 1 FROM kf_config_versions"));
        var job=job(JobType.TARGETED_CONFIG_CHANGE);var step=step(job,"sasl.jaas.config");mapping.record(job,List.of(step));mapping.step(step);
        for(var invocation:mockingDetails(jdbc).getInvocations()) for(Object value:invocation.getArguments()) {
            if(value instanceof Object[] array) for(Object item:array) assertThat(Objects.toString(item,"")).doesNotContain("NEVER_COPY_IF_SECRET","OLD_SECRET");
            else assertThat(Objects.toString(value,"")).doesNotContain("NEVER_COPY_IF_SECRET","OLD_SECRET");
        }
    }
    @Test void unrelatedJobTypesDoNotReadOrWriteConfigurationProjections() {
        var job=job(JobType.ROLLING_RESTART);var step=step(job,"log.retention.hours");
        mapping.record(job,List.of(step));mapping.status(job);mapping.step(step);mapping.verified(step,"log.retention.hours","72",null);
        verifyNoInteractions(jdbc);
    }
    @Test void historicalUnmappedStepsAreNotBackfilled() {
        var job=job(JobType.CONFIG_CHANGE);mapping.step(step(job,"log.retention.hours"));
        for(var invocation:mockingDetails(jdbc).getInvocations()) assertThat((String)invocation.getArgument(0)).doesNotContain("INSERT","UPDATE");
    }
    @Test void projectionWritesAreConfinedToConfigurationHistoryTables() {
        catalog();when(jdbc.queryForObject(anyString(),eq(Boolean.class),any(Object[].class))).thenAnswer(call -> !((String)call.getArgument(0)).contains("SELECT EXISTS(SELECT 1 FROM kf_config_versions"));
        var job=job(JobType.TARGETED_CONFIG_CHANGE);var step=step(job,"log.retention.hours");mapping.record(job,List.of(step));mapping.step(step);mapping.status(job);
        Set<String> tables=Set.of("kf_config_versions","kf_config_catalog","kf_config_change","kf_config_change_detail","kf_config_change_target_node","kf_config_change_node_value","kf_cluster_operation_lock","kf_health_check","kf_config_execution_attempt","kf_config_backup","kf_rolling_restart_operation","kf_rolling_restart_node_step");
        for(var invocation:mockingDetails(jdbc).getInvocations()) {
            String sql=(String)invocation.getArgument(0);
            if(sql.startsWith("INSERT INTO ")||sql.startsWith("UPDATE ")) assertThat(tables).contains(sql.split(" ")[sql.startsWith("UPDATE ")?1:2]);
        }
    }
    private Map<String,Object> inserted(String table){
        for(var invocation:mockingDetails(jdbc).getInvocations()) {
            String sql=(String)invocation.getArgument(0);
            if(!sql.startsWith("INSERT INTO "+table+" (")||!invocation.getMethod().getName().equals("update"))continue;
            String[] columns=sql.substring(sql.indexOf('(')+1,sql.indexOf(')')).split(",");Object[] values=(Object[])invocation.getRawArguments()[1];
            var result=new LinkedHashMap<String,Object>();for(int i=0;i<columns.length;i++)result.put(columns[i],values[i]);return result;
        }return Map.of();
    }
}
