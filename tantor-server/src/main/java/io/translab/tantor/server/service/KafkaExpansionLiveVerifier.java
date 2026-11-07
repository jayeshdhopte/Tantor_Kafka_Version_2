package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.*;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Independent typed Kafka observations; native receipts cannot assert voter admission. */
@Service @RequiredArgsConstructor
public class KafkaExpansionLiveVerifier {
    private final KafkaExpansionQuorum quorum;
    private final KafkaAdminService kafka;
    private final AddNodeKafkaReadiness readiness;
    private final ClusterRepository clusters;
    private final HostRepository hosts;
    private final AddNodeReadinessProperties policy;
    private final AddNodeInspectionProperties scopePolicy;
    private final ObjectMapper mapper;
    public record ControllerProof(UUID observationId,UUID operationId,String planDigest,int nodeId,String directoryId,
                                  String baselineDigest,@com.fasterxml.jackson.annotation.JsonFormat(shape=com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING) Instant observedAt,boolean observerCaughtUp,boolean voterVerified) {}
    public void baseline(KafkaExpansionPlan plan){allowed(plan);quorum.healthyBaseline(baselineOf(plan),quorum.observe(endpoints(plan,false)),policy.getMaxQuorumLag(),Instant.now());brokerSet(plan,false);partitionHealth(plan);}
    public ControllerProof controller(KafkaExpansionPlan.Frozen frozen,String directoryId,boolean membership,boolean staticConfigsVerified,boolean brokerExpected){
        var plan=frozen.plan();allowed(plan);var live=quorum.observe(endpoints(plan,true));var process=plan.process("controller");var candidate=new KafkaExpansionQuorum.Candidate(process.nodeId(),directoryId,endpoint(plan.candidateIp(),process.port()));
        var baseline=baselineOf(plan);if(brokerExpected&&plan.layout()==KafkaExpansionWorkflow.Layout.SEPARATE){var known=new HashSet<>(baseline.knownNodeIds());known.add(plan.process("broker").nodeId());baseline=new KafkaExpansionQuorum.Baseline(baseline.clusterId(),baseline.features(),baseline.voters(),known,baseline.dynamic());}
        var verified=membership?quorum.member(baseline,live,candidate,policy.getMaxQuorumLag(),staticConfigsVerified,Instant.now()):quorum.caughtUp(baseline,live,candidate,policy.getMaxQuorumLag(),Instant.now());
        return new ControllerProof(UUID.randomUUID(),plan.operationId(),frozen.digest(),process.nodeId(),directoryId,plan.voterDigest(),verified.observedAt(),!verified.voter(),verified.voter());
    }
    public void broker(KafkaExpansionPlan plan){allowed(plan);brokerSet(plan,plan.layout()==KafkaExpansionWorkflow.Layout.SEPARATE);}
    public void health(KafkaExpansionPlan plan){allowed(plan);brokerSet(plan,plan.layout()==KafkaExpansionWorkflow.Layout.SEPARATE);partitionHealth(plan);try{kafka.getAdminClient(plan.clusterId()).listTopics(new ListTopicsOptions().listInternal(true).timeoutMs(15000)).names().get(20,TimeUnit.SECONDS);}catch(Exception e){throw new IllegalStateException("Script-aligned topic-list smoke check is incomplete.",e);}}
    private void brokerSet(KafkaExpansionPlan plan,boolean expanded){
        try{var description=kafka.getAdminClient(plan.clusterId()).describeCluster(new DescribeClusterOptions().includeFencedBrokers(false).timeoutMs(15000));if(!plan.kafkaClusterId().equals(description.clusterId().get(20,TimeUnit.SECONDS)))throw new IllegalStateException("Kafka cluster identity changed.");var nodes=description.nodes().get(20,TimeUnit.SECONDS);var expected=plan.participants().stream().filter(p->p.role().contains("broker")).map(KafkaExpansionPlan.Participant::nodeId).collect(java.util.stream.Collectors.toSet());if(expanded)expected.add(plan.process("broker").nodeId());var actual=nodes.stream().map(org.apache.kafka.common.Node::id).collect(java.util.stream.Collectors.toSet());if(!actual.equals(expected))throw new KafkaExpansionQuorum.NotReady("Expected broker membership is not fully registered and unfenced.");
            if(expanded){var broker=plan.process("broker");var node=nodes.stream().filter(n->n.id()==broker.nodeId()).findFirst().orElseThrow();if(!node.host().equals(plan.candidateIp())||node.port()!=broker.port()||!Objects.toString(node.rack(),"").equals(broker.brokerRack()))throw new IllegalStateException("Candidate broker endpoint or rack differs from the frozen plan.");}
        }catch(KafkaExpansionQuorum.NotReady e){throw e;}catch(Exception e){throw new IllegalStateException("Broker identity verification is incomplete.",e);}
    }
    private void partitionHealth(KafkaExpansionPlan plan){var cluster=cluster(plan);Map<String,Object> config;try{config=mapper.readValue(Objects.toString(cluster.getConfigJson(),"{}"),new TypeReference<>(){});}catch(Exception e){throw new IllegalStateException("Cluster configuration is unreadable.",e);}var observations=readiness.observe(cluster,config);for(String code:List.of("C06","C08"))if(observations.checks().stream().noneMatch(c->code.equals(c.code())&&"PASS".equals(c.status())))throw new KafkaExpansionQuorum.NotReady("Zero URP and partition/ISR health are not proven yet.");}
    private void allowed(KafkaExpansionPlan plan){var cluster=cluster(plan);AddNodeProtectedHostGuard.cluster(cluster,hosts,scopePolicy,mapper);if(scopePolicy.excludes(plan.candidateHostId(),plan.candidateIp())||plan.participants().stream().anyMatch(p->scopePolicy.excludes(p.hostId(),p.hostIp())))throw new IllegalStateException("Expansion scope includes an excluded host.");}
    private Cluster cluster(KafkaExpansionPlan plan){var cluster=clusters.findWithServicesById(plan.clusterId()).orElseThrow();if(!Objects.equals(cluster.getKafkaClusterId(),plan.kafkaClusterId())||!Objects.equals(cluster.getKafkaVersion(),plan.kafkaVersion())||"DELETED".equals(cluster.getStatus()))throw new IllegalStateException("Frozen cluster identity changed.");return cluster;}
    private KafkaExpansionQuorum.Baseline baselineOf(KafkaExpansionPlan plan){var voters=new TreeMap<Integer,String>();plan.voters().forEach(v->voters.put(v.nodeId(),v.directoryId()));return new KafkaExpansionQuorum.Baseline(plan.kafkaClusterId(),plan.features(),voters,plan.participants().stream().map(KafkaExpansionPlan.Participant::nodeId).collect(java.util.stream.Collectors.toSet()),plan.quorumMode()==KafkaExpansionWorkflow.Mode.DYNAMIC);}
    private List<String> endpoints(KafkaExpansionPlan plan,boolean candidate){var out=new TreeSet<String>();plan.voters().forEach(v->out.add(endpoint(v.hostIp(),v.port())));if(candidate)out.add(endpoint(plan.candidateIp(),plan.process("controller").port()));return List.copyOf(out);}
    private static String endpoint(String host,int port){return (host.contains(":")?"["+host+"]":host)+":"+port;}
}
