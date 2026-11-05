package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Kafka 4.1/4.3 PLAINTEXT broker adapter. Every unsupported observation fails closed. */
@Service @RequiredArgsConstructor
public class BrokerAdditionKafka {
    public static final class RegistrationPending extends IllegalStateException {
        public RegistrationPending(){super("Candidate broker is not yet registered and unfenced.");}
    }
    private final KafkaAdminService kafka;
    private final AddNodeKafkaReadiness readiness;
    public String voterBaseline(Cluster cluster) {
        try {
            var quorum=kafka.getAdminClient(cluster.getId()).describeMetadataQuorum().quorumInfo().get(30,TimeUnit.SECONDS);
            if(quorum.voters().isEmpty())throw new IllegalStateException("No live controller voters.");
            return quorum.voters().stream().map(v->v.replicaId()+":"+v.replicaDirectoryId()).sorted().collect(java.util.stream.Collectors.joining(","));
        } catch(Exception e){throw new IllegalStateException("Cannot establish exact voter identity baseline.",e);}
    }

    public String properties(Cluster cluster,BrokerAdditionPlan p) {
        try {
            var resources=cluster.getServices().stream().filter(s->s.getRole().contains("broker")).map(s->new ConfigResource(ConfigResource.Type.BROKER,String.valueOf(s.getNodeId()))).distinct().toList();
            if(resources.isEmpty())throw new IllegalStateException("Existing broker configuration is required.");
            var configs=kafka.getAdminClient(cluster.getId()).describeConfigs(resources).all().get(30,TimeUnit.SECONDS);
            Map<String,String> inherited=null;
            boolean rackAware=false,withoutRack=false;
            var replaced=Set.of("broker.id","node.id","process.roles","listeners","advertised.listeners","log.dirs","log.dir","metadata.log.dir","controller.quorum.voters","controller.quorum.bootstrap.servers","controller.quorum.auto.join.enable","broker.rack");
            for(var resource:resources) {
                var config=configs.get(resource);if(config==null)throw new IllegalStateException("Missing broker configuration.");
                var selected=new TreeMap<String,String>();
                for(var entry:config.entries()) {
                    if(replaced.contains(entry.name())||entry.isDefault())continue;
                    if(entry.isSensitive() && entry.value()!=null)throw new IllegalStateException("Sensitive inherited broker settings need a supported secret adapter.");
                    if(entry.value()!=null)selected.put(entry.name(),entry.value());
                }
                var rack=config.get("broker.rack");if(rack==null)throw new IllegalStateException("Existing broker rack observation unavailable.");
                if(rack.value()==null||rack.value().isBlank())withoutRack=true;else rackAware=true;
                String listener=config.get("inter.broker.listener.name")==null?null:config.get("inter.broker.listener.name").value();
                if(listener!=null&&!listener.isBlank()&&!"PLAINTEXT".equals(listener))throw new IllegalStateException("Nonstandard inter-broker listener requires an approved adapter.");
                if(inherited!=null&&!inherited.equals(selected))throw new IllegalStateException("Existing brokers have different effective custom settings; resolve the inheritance policy before adding a broker.");
                inherited=selected;
            }
            if((rackAware&&withoutRack)||rackAware==p.brokerRack().isBlank())throw new IllegalStateException("Explicit candidate rack must match whether the existing cluster uses rack awareness; resolve mixed existing rack settings.");
            var result=new TreeMap<>(Objects.requireNonNull(inherited));
            if(!p.brokerRack().isBlank())result.put("broker.rack",p.brokerRack());
            result.put("process.roles","broker");result.put("node.id",String.valueOf(p.nodeId()));
            String endpoint=(p.hostIp().contains(":")?"["+p.hostIp()+"]":p.hostIp())+":"+p.brokerPort();
            result.put("listeners","PLAINTEXT://"+endpoint);result.put("advertised.listeners","PLAINTEXT://"+endpoint);
            result.put("listener.security.protocol.map","PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT");result.put("inter.broker.listener.name","PLAINTEXT");result.put("controller.listener.names","CONTROLLER");
            result.put("log.dirs",String.join(",",p.dataPaths()));if(!p.metadataPath().isBlank())result.put("metadata.log.dir",p.metadataPath());
            if("DYNAMIC".equals(p.quorumMode()))result.put("controller.quorum.bootstrap.servers",String.join(",",p.controllerEndpoints()));else result.put("controller.quorum.voters",p.quorumVoters());
            StringBuilder text=new StringBuilder();
            for(var entry:result.entrySet()) {
                if(!entry.getKey().matches("[A-Za-z0-9._-]+")||entry.getValue().contains("\\")||entry.getValue().contains("\n")||entry.getValue().contains("\r")||entry.getValue().contains("\0")||!entry.getValue().equals(entry.getValue().strip()))throw new IllegalStateException("Inherited property requires a supported escaping adapter.");
                text.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
            }
            return text.toString();
        } catch(Exception e){throw new IllegalStateException("Cannot freeze inherited broker configuration: "+e.getMessage(),e);}
    }

    public String verify(Cluster cluster,BrokerAdditionLedger.Payload payload,Map<String,Object> config) {
        try {
            var p=payload.plan();var admin=kafka.getAdminClient(cluster.getId());
            var description=admin.describeCluster(new DescribeClusterOptions().includeFencedBrokers(false).timeoutMs(30000));
            if(!p.kafkaClusterId().equals(description.clusterId().get(30,TimeUnit.SECONDS)))throw new IllegalStateException("Live Kafka identity changed.");
            var brokers=description.nodes().get(30,TimeUnit.SECONDS);
            var expected=cluster.getServices().stream().filter(s->s.getRole().contains("broker")).map(s->s.getNodeId()).collect(java.util.stream.Collectors.toSet());expected.add(p.nodeId());
            var observed=brokers.stream().map(n->n.id()).collect(java.util.stream.Collectors.toSet());
            if(!expected.equals(observed)) {
                var existing=new HashSet<>(expected);existing.remove(p.nodeId());
                if(existing.equals(observed))throw new RegistrationPending();
                throw new IllegalStateException("Unfenced broker set does not match existing plus candidate.");
            }
            var candidate=brokers.stream().filter(n->n.id()==p.nodeId()).findFirst().orElseThrow();
            if(!candidate.host().equals(p.hostIp())||candidate.port()!=p.brokerPort())throw new IllegalStateException("Candidate advertised endpoint mismatch.");
            if(!Objects.toString(candidate.rack(),"").equals(p.brokerRack()))throw new IllegalStateException("Registered broker rack differs from the frozen candidate rack.");
            if(!payload.voterBaseline().equals(voterBaseline(cluster)))throw new IllegalStateException("Controller voter identities changed during broker addition.");
            var health=readiness.observe(cluster,config);
            var required=Set.of("C01","C03","C06","C07","C08","B09");
            var checked=health.checks().stream().filter(c->required.contains(c.code())).toList();
            if(checked.size()!=required.size()||checked.stream().anyMatch(c->!"PASS".equals(c.status())))throw new IllegalStateException("Stable cluster health is not proven.");
            return "Verified Kafka ID, unfenced broker "+p.nodeId()+", advertised endpoint, unchanged voter identities and live cluster health.";
        } catch(RegistrationPending e){throw e;}
        catch(Exception e){throw new IllegalStateException("Broker verification incomplete: "+e.getMessage(),e);}
    }
}
