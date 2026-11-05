package io.translab.tantor.server.service;

import io.translab.tantor.server.config.AddNodeReadinessProperties;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.dto.AddNodeReadiness.Check;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Only Kafka describe/list operations; no configuration, membership or topic mutations. */
@Service @RequiredArgsConstructor
public class AddNodeKafkaReadiness {
    private final KafkaAdminService kafka;
    private final AddNodeReadinessProperties policy;
    public record Observation(List<Check> checks, Set<Integer> nodeIds, List<String> brokerEndpoints,
                              List<String> controllerEndpoints, String quorumMode,Set<String> brokerRacks,boolean rackObserved) {
        public Observation(List<Check> checks,Set<Integer> ids,List<String> brokers,List<String> controllers,String mode){this(checks,ids,brokers,controllers,mode,Set.of(),true);}
    }

    public Observation observe(Cluster cluster, Map<String, Object> config) {
        var checks = new ArrayList<Check>();
        var ids = new TreeSet<Integer>();
        var brokers = new ArrayList<String>();
        var controllers = new ArrayList<String>();
        String mode = "UNKNOWN";
        var racks=new HashSet<String>();boolean rackObserved=false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(policy.getTimeoutSeconds());
        AdminClient admin;
        try { admin = kafka.getAdminClient(cluster.getId()); }
        catch (Exception e) {
            for (String code : List.of("C01", "C02", "C03", "C05", "C06", "C07", "C08", "B09")) checks.add(unknown(code, "Kafka observation", e));
            return new Observation(checks, ids, brokers, controllers, mode,Set.of(),false);
        }
        try {
            var description = admin.describeCluster();
            String actualId = await(description.clusterId(), deadline);
            checks.add(check("C01", "Live Kafka identity", Objects.equals(actualId, cluster.getKafkaClusterId())
                    && actualId != null && !actualId.isBlank(), cluster.getKafkaClusterId(), actualId));
            var nodes = await(description.nodes(), deadline);
            for (var node : nodes) { ids.add(node.id()); brokers.add(endpoint(node.host(), node.port())); }
            var expected = cluster.getServices().stream().filter(s -> s.getRole() != null && s.getRole().contains("broker"))
                    .map(s -> s.getNodeId()).filter(Objects::nonNull).collect(java.util.stream.Collectors.toSet());
            checks.add(check("C05", "Expected brokers available", !expected.isEmpty() && ids.equals(expected), expected, Set.copyOf(ids)));
        } catch (Exception e) { addUnknown(checks, List.of("C01", "C05"), e); }

        try {
            var features = await(admin.describeFeatures().featureMetadata(), deadline);
            var kraft = features.finalizedFeatures().get("kraft.version");
            mode = kraft != null && kraft.maxVersionLevel() >= 1 ? "DYNAMIC" : "STATIC";
            checks.add(check("C03", "Finalized feature and quorum state", features.finalizedFeatures().containsKey("metadata.version") && policy.getSupportedKafkaVersions().contains(cluster.getKafkaVersion()),
                    "Queryable metadata.version and a package version admitted by readiness policy", features.toString()));
        } catch (Exception e) { checks.add(unknown("C03", "Finalized feature and quorum state", e)); }

        try {
            // Registered fenced brokers also own IDs, even though they cannot satisfy C05 availability.
            await(admin.describeCluster(new DescribeClusterOptions().includeFencedBrokers(true)).nodes(),deadline).forEach(n->ids.add(n.id()));
            var quorum = await(admin.describeMetadataQuorum().quorumInfo(), deadline);
            var voters = quorum.voters();
            voters.forEach(v -> ids.add(v.replicaId()));
            quorum.observers().forEach(v -> ids.add(v.replicaId()));
            quorum.nodes().values().forEach(node -> node.endpoints().forEach(ep -> controllers.add(endpoint(ep.host(), ep.port()))));
            var expectedIds = cluster.getServices().stream().filter(s -> s.getRole() != null && (s.getRole().contains("broker") || s.getRole().contains("controller")))
                    .map(s -> s.getNodeId()).filter(Objects::nonNull).collect(java.util.stream.Collectors.toSet());
            checks.add(check("C02", "Broker and controller identities", !voters.isEmpty() && !expectedIds.isEmpty() && ids.equals(expectedIds),
                    expectedIds, Map.of("nodeIds", Set.copyOf(ids), "voterCount", voters.size())));
            long now = System.currentTimeMillis();
            long healthy = voters.stream().filter(v -> v.replicaId() == quorum.leaderId()
                    || (v.lastFetchTimestamp().isPresent() && now - v.lastFetchTimestamp().getAsLong() <= 30_000
                    && quorum.highWatermark() - v.logEndOffset() <= policy.getMaxQuorumLag())).count();
            checks.add(check("C07", "Live quorum health", quorum.leaderId() >= 0 && healthy > voters.size() / 2
                            && healthy == voters.size(), "Leader and all current voters responsive; bounded metadata lag",
                    Map.of("leaderId", quorum.leaderId(), "voters", voters.size(), "responsiveVoters", healthy)));
            String votersConfig = Objects.toString(config.getOrDefault("controller_quorum_voters", config.getOrDefault("quorum_voters", "")), "");
            Set<Integer> configuredIds = new TreeSet<>();
            if (!votersConfig.isBlank()) {
                for (String voter : votersConfig.split(",")) {
                    var parts = voter.trim().split("@", 2);
                    configuredIds.add(Integer.parseInt(parts[0]));
                }
                if (controllers.isEmpty()) for (String voter : votersConfig.split(",")) controllers.add(voter.trim().split("@", 2)[1]);
            }
            Set<Integer> liveVoters = voters.stream().map(QuorumInfo.ReplicaState::replicaId).collect(java.util.stream.Collectors.toSet());
            boolean valid = !controllers.isEmpty() && !"UNKNOWN".equals(mode)
                    && ("DYNAMIC".equals(mode) || configuredIds.equals(liveVoters));
            checks.add(check("B09", "Inherited controller discovery", valid, "Existing quorum only; broker never becomes a voter",
                    Map.of("quorumMode", mode, "controllerEndpoints", List.copyOf(controllers), "voterIds", liveVoters)));
        } catch (Exception e) { addUnknown(checks, List.of("C02", "C07", "B09"), e); }

        try {
            Set<String> topics = await(admin.listTopics(new ListTopicsOptions().listInternal(true)).names(), deadline);
            if (topics.size() > policy.getMaxTopics()) throw new IllegalStateException("Topic inspection limit exceeded");
            var descriptions = topics.isEmpty() ? Map.<String, TopicDescription>of() : await(admin.describeTopics(topics).allTopicNames(), deadline);
            var resources = topics.stream().map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).toList();
            var configs = resources.isEmpty() ? Map.<ConfigResource, Config>of() : await(admin.describeConfigs(resources).all(), deadline);
            int underReplicated = 0, offline = 0, belowMinIsr = 0;
            for (var entry : descriptions.entrySet()) {
                var effective = configs.get(new ConfigResource(ConfigResource.Type.TOPIC, entry.getKey()));
                var min = effective == null ? null : effective.get("min.insync.replicas");
                if (min == null || min.value() == null) throw new IllegalStateException("Missing effective min ISR");
                int minIsr = Integer.parseInt(min.value());
                for (var partition : entry.getValue().partitions()) {
                    if (partition.leader() == null || partition.leader().id() < 0) offline++;
                    if (partition.isr().size() < partition.replicas().size()) underReplicated++;
                    if (partition.isr().size() < minIsr) belowMinIsr++;
                }
            }
            checks.add(check("C06", "Partition and internal-topic health", underReplicated == 0 && offline == 0 && belowMinIsr == 0,
                    "URP=0; offline=0; below min ISR=0 (including internal topics)", Map.of("topics", topics.size(), "urp", underReplicated, "offline", offline, "belowMinIsr", belowMinIsr)));
        } catch (Exception e) { checks.add(unknown("C06", "Partition and internal-topic health", e)); }

        try {
            var brokerIds = brokers.isEmpty() ? Set.<Integer>of() : await(admin.describeCluster().nodes(), deadline).stream().map(n -> n.id()).collect(java.util.stream.Collectors.toSet());
            if (brokerIds.isEmpty()) throw new IllegalStateException("No brokers observed");
            var dirs = await(admin.describeLogDirs(brokerIds).allDescriptions(), deadline);
            long minimum = (long) policy.getMinDiskGb() * 1024 * 1024 * 1024;
            boolean healthy = true;
            for (var broker : brokerIds) {
                var logs = dirs.get(broker);
                if (logs == null || logs.isEmpty()) throw new IllegalStateException("Missing broker log-directory measurements");
                for (var log : logs.values()) {
                    if (log.error() != null) healthy = false;
                    if (log.usableBytes().isEmpty()) throw new IllegalStateException("Broker disk headroom unavailable for this Kafka release");
                    if (log.usableBytes().getAsLong() < minimum) healthy = false;
                }
            }
            checks.add(check("C08", "Existing broker storage headroom", healthy, "No Kafka log-directory errors; free bytes >= " + minimum, "Inspected " + dirs.size() + " broker(s) over live Kafka API"));
        } catch (Exception e) { checks.add(unknown("C08", "Existing broker storage headroom", e)); }
        try {
            var resources=cluster.getServices().stream().filter(s->s.getRole()!=null&&s.getRole().contains("broker")&&s.getNodeId()!=null).map(s->new ConfigResource(ConfigResource.Type.BROKER,String.valueOf(s.getNodeId()))).distinct().toList();
            if(resources.isEmpty())throw new IllegalStateException("Existing brokers are required for rack observation.");
            var configs=await(admin.describeConfigs(resources).all(),deadline);
            for(var resource:resources){var c=configs.get(resource);if(c==null||c.get("broker.rack")==null)throw new IllegalStateException("Broker rack observation missing.");racks.add(Objects.toString(c.get("broker.rack").value(),""));}
            rackObserved=true;
        } catch(Exception unavailable) {racks.clear();}
        return new Observation(checks, ids, brokers, controllers.stream().distinct().toList(), mode,Set.copyOf(racks),rackObserved);
    }

    private static String endpoint(String host, int port) { return (host.contains(":") ? "[" + host + "]" : host) + ":" + port; }
    private static <T> T await(KafkaFuture<T> future, long deadline) throws Exception {
        return future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    }
    static Check check(String code, String name, boolean pass, Object expected, Object observed) {
        return new Check(code, name, code.startsWith("B") ? "BROKER" : "COMMON", pass ? "PASS" : "FAIL", expected, observed,
                pass ? "" : "Resolve the expected/observed difference and run fresh checks.", Instant.now(), "KAFKA_API");
    }
    static Check unknown(String code, String name, Exception e) {
        return new Check(code, name, code.startsWith("B") ? "BROKER" : "COMMON", "UNKNOWN", "Successful live observation", e.getClass().getSimpleName(),
                "Check Kafka API support, connectivity and permissions; an unavailable measurement cannot pass.", Instant.now(), "KAFKA_API");
    }
    private static void addUnknown(List<Check> checks, List<String> codes, Exception e) {
        for (String code : codes) if (checks.stream().noneMatch(check -> check.code().equals(code))) checks.add(unknown(code, "Kafka observation", e));
    }
}
