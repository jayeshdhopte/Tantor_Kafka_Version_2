package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** Standalone read-only gateway for isolated QA; uses the production typed verifier. */
public final class Phase7QuorumGateway {
    public static void main(String[] args)throws Exception{
        var mapper=new ObjectMapper().findAndRegisterModules();var input=mapper.readTree(Files.readString(Path.of(args[0])));
        var endpoints=new ArrayList<String>();input.path("endpoints").forEach(e->endpoints.add(e.asText()));
        for(String endpoint:endpoints)if(!Set.of("192.168.3.191:19093","192.168.3.229:29093","192.168.3.229:19093").contains(endpoint))throw new IllegalArgumentException("Endpoint outside isolated QA ports.");
        var quorum=new KafkaExpansionQuorum(new AddNodeInspectionProperties());var live=quorum.observe(endpoints);
        Object result=live;
        if(input.has("brokerPlan")){
            var p=mapper.treeToValue(input.path("brokerPlan"),BrokerAdditionPlan.class);p.validate();
            if(p.nodeId()!=7002||!p.hostIp().equals("192.168.3.229")||!p.kafkaClusterId().equals(live.clusterId()))throw new IllegalArgumentException("Broker inheritance outside isolated QA identity.");
            var config=new Properties();config.put("bootstrap.servers","192.168.3.191:19092");config.put("request.timeout.ms","10000");config.put("default.api.timeout.ms","15000");
            try(var admin=org.apache.kafka.clients.admin.AdminClient.create(config)){
                var kafka=org.mockito.Mockito.mock(KafkaAdminService.class);var cluster=new io.translab.tantor.server.domain.Cluster();cluster.setId(UUID.randomUUID());var assignment=new io.translab.tantor.server.domain.ClusterServiceAssignment();assignment.setRole("broker");assignment.setNodeId(7001);cluster.setServices(new ArrayList<>(List.of(assignment)));org.mockito.Mockito.when(kafka.getAdminClient(cluster.getId())).thenReturn(admin);
                result=Map.of("properties",new BrokerAdditionKafka(kafka,null).properties(cluster,p));
            }
        }
        if(input.has("expectedBrokers")){
            if(!live.clusterId().equals(input.path("expectedClusterId").asText()))throw new IllegalStateException("QA cluster identity differs.");
            var expected=new HashSet<Integer>();input.path("expectedBrokers").forEach(n->expected.add(n.asInt()));for(int id:expected)if(id<7000||id>7999)throw new IllegalArgumentException("Broker outside isolated QA identities.");
            var properties=new Properties();properties.put("bootstrap.servers","192.168.3.191:19092");properties.put("default.api.timeout.ms","15000");properties.put("request.timeout.ms","10000");
            try(var admin=org.apache.kafka.clients.admin.Admin.create(properties)){
                var description=admin.describeCluster();if(!live.clusterId().equals(description.clusterId().get(20,java.util.concurrent.TimeUnit.SECONDS)))throw new IllegalStateException("Broker cluster identity differs.");var nodes=description.nodes().get(20,java.util.concurrent.TimeUnit.SECONDS);var actual=new HashSet<Integer>();nodes.forEach(n->actual.add(n.id()));if(!actual.equals(expected))throw new IllegalStateException("QA brokers are not all registered and unfenced: "+actual);
                var node=nodes.stream().filter(n->n.id()==7002).findFirst().orElseThrow();if(!node.host().equals("192.168.3.229")||node.port()!=input.path("expectedBrokerPort").asInt(29092)||!Objects.toString(node.rack(),"").equals("qa-rack"))throw new IllegalStateException("Broker endpoint/rack differs.");
                if(input.has("expectedNetworkThreads")){var resource=new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.BROKER,"7002");var config=admin.describeConfigs(List.of(resource)).all().get(20,java.util.concurrent.TimeUnit.SECONDS).get(resource);if(!input.path("expectedNetworkThreads").asText().equals(config.get("num.network.threads").value()))throw new IllegalStateException("Restart did not load the requested broker setting.");}
            }
        }
        if(input.has("plan")){
            String raw=input.path("plan").asText();var frozen=KafkaExpansionPlan.read(mapper,raw,input.path("digest").asText());var p=frozen.plan();
            var voters=new HashMap<Integer,String>();p.voters().forEach(v->voters.put(v.nodeId(),v.directoryId()));var known=new HashSet<Integer>();p.participants().forEach(m->known.add(m.nodeId()));if(input.path("brokerExpected").asBoolean()&&p.layout()==KafkaExpansionWorkflow.Layout.SEPARATE)known.add(p.process("broker").nodeId());
            var baseline=new KafkaExpansionQuorum.Baseline(p.kafkaClusterId(),p.features(),voters,known,p.quorumMode()==KafkaExpansionWorkflow.Mode.DYNAMIC);
            var controller=p.process("controller");var candidate=new KafkaExpansionQuorum.Candidate(controller.nodeId(),input.path("directoryId").asText(),p.candidateIp()+":"+controller.port());
            boolean member=input.path("member").asBoolean();var verified=member?quorum.member(baseline,live,candidate,10,p.quorumMode()==KafkaExpansionWorkflow.Mode.STATIC,Instant.now()):quorum.caughtUp(baseline,live,candidate,10,Instant.now());
            if(input.path("brokerExpected").asBoolean()){
                var config=new Properties();config.put("bootstrap.servers","192.168.3.191:19092");config.put("default.api.timeout.ms","15000");config.put("request.timeout.ms","10000");
                try(var admin=org.apache.kafka.clients.admin.Admin.create(config)){
                    var expected=new HashSet<Integer>();p.participants().stream().filter(m->m.role().contains("broker")).forEach(m->expected.add(m.nodeId()));if(p.layout()==KafkaExpansionWorkflow.Layout.SEPARATE)expected.add(p.process("broker").nodeId());
                    var actual=new HashSet<Integer>();admin.describeCluster().nodes().get(20,java.util.concurrent.TimeUnit.SECONDS).forEach(n->actual.add(n.id()));if(!actual.equals(expected))throw new IllegalStateException("Expected unfenced broker set differs: "+actual);
                    if(p.layout()==KafkaExpansionWorkflow.Layout.SEPARATE){var resource=new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.BROKER,String.valueOf(p.process("broker").nodeId()));var values=admin.describeConfigs(List.of(resource)).all().get(20,java.util.concurrent.TimeUnit.SECONDS).get(resource);if(!p.process("broker").brokerRack().equals(values.get("broker.rack").value()))throw new IllegalStateException("New broker rack differs.");}
                }
            }
            result=new KafkaExpansionLiveVerifier.ControllerProof(UUID.randomUUID(),p.operationId(),frozen.digest(),controller.nodeId(),candidate.directoryId(),p.voterDigest(),verified.observedAt(),!verified.voter(),verified.voter());
        }
        System.out.println("TANTOR_PHASE7_RESULT="+mapper.writeValueAsString(result));
    }
}
