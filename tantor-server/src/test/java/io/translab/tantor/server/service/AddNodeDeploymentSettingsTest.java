package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class AddNodeDeploymentSettingsTest {
    final ObjectMapper mapper=new ObjectMapper().findAndRegisterModules();
    AddNodeDeploymentSettings settings(){return new AddNodeDeploymentSettings("/opt/tantor/kafka","/opt/tantor/kafka/config","/usr/lib/jvm/jdk-17","kafka",List.of("/data/new-broker/data"),"","/var/log/new-broker","512M",List.of("/data/new-controller/data"),"/data/new-controller/metadata","/var/log/new-controller","512M");}
    @Test void standardExpansionPathsAndDistinctProcessesSurviveFrozenRoundTrip()throws Exception{
        var fixture=new KafkaExpansionPlanTest();ObjectNode raw=(ObjectNode)mapper.readTree(fixture.fixture());
        var settings=settings();settings.validate();raw.put("schemaVersion","add-quorum-expansion/v2");raw.set("deploymentSettings",mapper.valueToTree(settings));raw.put("javaHome",settings.javaHome());
        for(var row:raw.path("processes")){
            ObjectNode p=(ObjectNode)row;boolean c=p.path("role").asText().equals("controller");
            p.set("dataPaths",mapper.valueToTree(c?settings.controllerDataPaths():settings.brokerDataPaths()));p.put("metadataPath",c?settings.controllerMetadataPath():settings.brokerMetadataPath());p.put("logPath",c?settings.controllerLogPath():settings.brokerLogPath());p.put("heapXmx","512M");
        }
        var p=mapper.treeToValue(raw,KafkaExpansionPlan.class);p.validate();var frozen=p.freeze(mapper);var loaded=KafkaExpansionPlan.read(mapper,frozen.raw(),frozen.digest()).plan();
        assertEquals("broker.service",loaded.unit("broker"));assertEquals("controller.service",loaded.unit("controller"));assertEquals("/opt/tantor/kafka/config/broker.properties",loaded.configPath("broker"));assertEquals(settings,loaded.deploymentSettings());
        ((ObjectNode)raw.path("processes").get(1)).put("heapXmx","2G");assertThrows(IllegalArgumentException.class,()->mapper.treeToValue(raw,KafkaExpansionPlan.class).validate());
    }
    @Test void traversalProtectedRootsAndOverlappingExecutableTreesAreRejected()throws Exception{
        for(var entry:List.of(Map.entry("configDirectory","/etc/kafka"),Map.entry("installPath","/opt/tantor/../etc"),Map.entry("configDirectory","/opt/tantor/kafka/libs"),Map.entry("brokerLogPath","/data/new-broker/data"),Map.entry("runtimeUser","kafka;id"),Map.entry("brokerHeapXmx","1G -javaagent:evil"))){
            ObjectNode raw=mapper.valueToTree(settings());raw.put(entry.getKey(),entry.getValue());assertThrows(IllegalArgumentException.class,()->mapper.treeToValue(raw,AddNodeDeploymentSettings.class).validate(),entry.toString());
        }
    }
    @Test void legacyPlanSerializationDoesNotAddANullFieldOrChangePaths()throws Exception{
        var p=new KafkaExpansionPlanTest().plan();String raw=mapper.writeValueAsString(p);assertFalse(raw.contains("deploymentSettings"));assertTrue(p.configPath("broker").startsWith("/etc/tantor-kafka/add-expansion/"));assertTrue(p.unit("broker").startsWith("tantor-kafka-broker-"));
    }
}
