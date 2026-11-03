package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class RollingRestartHealthService {
    @org.springframework.beans.factory.annotation.Autowired private ExternalKafkaManagement externalKafka;
    private final KafkaAdminService kafkaAdminService;
    private final HostRepository hostRepository;

    @Value("${tantor.rolling-restart.max-disk-used-percent:85}")
    private double maxDiskUsedPercent;
    @Value("${tantor.rolling-restart.max-consumer-lag:1000000}")
    private long maxConsumerLag;
    @Value("${tantor.hosts.heartbeat-timeout-seconds:90}")
    private long heartbeatTimeoutSeconds;

    public Map<String,Object> policySettings() {
        return Map.of("maxDiskUsedPercent",maxDiskUsedPercent,"maxConsumerLag",maxConsumerLag,
                "heartbeatTimeoutSeconds",heartbeatTimeoutSeconds,"quorumFreshnessMillis",30000,
                "missingDiskMetrics","NON_BLOCKING_UNKNOWN");
    }

    public HealthSnapshot inspect(UUID clusterId, int expectedBrokerCount, Set<String> requiredHostIds) {
        Map<String, String> failures = new LinkedHashMap<>();
        Map<String,RollingRestartEvidenceService.Check> checks=new LinkedHashMap<>();
        int visibleBrokers = 0;
        int offlinePartitions = 0;
        int underReplicatedPartitions = 0;
        int minIsrViolations = 0;
        long consumerLag = 0;
        Integer controllerId = null;

        try {
            AdminClient client = kafkaAdminService.getAdminClient(clusterId);
            DescribeClusterResult cluster = client.describeCluster();
            Collection<org.apache.kafka.common.Node> nodes = cluster.nodes().get();
            org.apache.kafka.common.Node controller = cluster.controller().get();
            visibleBrokers = nodes.size();
            controllerId = controller == null ? null : controller.id();
            if (visibleBrokers < expectedBrokerCount) {
                failures.put("brokerReachability", "Only " + visibleBrokers + " of " + expectedBrokerCount + " brokers are reachable");
            }
            if (controllerId == null || controllerId < 0) failures.put("controller", "Kafka has no active controller");

            outcome(checks,"brokerReachability",visibleBrokers>=expectedBrokerCount,Map.of("visibleBrokers",visibleBrokers,"expectedBrokerCount",expectedBrokerCount),failures);
            outcome(checks,"controller",controllerId!=null && controllerId>=0,Map.of("controllerId",Objects.toString(controllerId,"unknown")),failures);

            Set<String> topicNames = client.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
            Map<String, TopicDescription> topics = topicNames.isEmpty()
                    ? Map.of() : client.describeTopics(topicNames).allTopicNames().get();
            Map<String, Integer> minimumIsr = loadMinimumIsr(client, topicNames);
            for (TopicDescription topic : topics.values()) {
                int requiredIsr = minimumIsr.getOrDefault(topic.name(), 1);
                for (var partition : topic.partitions()) {
                    if (partition.leader() == null || partition.leader().id() < 0) offlinePartitions++;
                    if (partition.isr().size() < partition.replicas().size()) underReplicatedPartitions++;
                    if (partition.isr().size() < requiredIsr) minIsrViolations++;
                }
            }
            if (offlinePartitions > 0) failures.put("offlinePartitions", offlinePartitions + " partition(s) have no leader");
            if (underReplicatedPartitions > 0) failures.put("underReplicatedPartitions", underReplicatedPartitions + " partition(s) are under-replicated");
            if (minIsrViolations > 0) failures.put("minimumIsr", minIsrViolations + " partition(s) are below min.insync.replicas");

            outcome(checks,"offlinePartitions",offlinePartitions==0,Map.of("count",offlinePartitions),failures);
            outcome(checks,"underReplicatedPartitions",underReplicatedPartitions==0,Map.of("count",underReplicatedPartitions),failures);
            outcome(checks,"minimumIsr",minIsrViolations==0,Map.of("count",minIsrViolations),failures);
            consumerLag = totalConsumerLag(client);
            if (consumerLag > maxConsumerLag) {
                failures.put("consumerLag", "Total consumer lag " + consumerLag + " exceeds limit " + maxConsumerLag);
            }
            outcome(checks,"consumerLag",consumerLag<=maxConsumerLag,Map.of("totalLag",consumerLag,"limit",maxConsumerLag),failures);
            outcome(checks,"kafkaInspection",true,Map.of(),failures);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failures.put("kafka", "Health inspection was interrupted");
        } catch (Exception e) {
            log.warn("Rolling restart health inspection failed for cluster {}", clusterId, e);
            kafkaAdminService.refreshAdminClient(clusterId);
            failures.put("kafka", "Kafka health inspection failed: " + rootMessage(e));
        }

        if(failures.containsKey("kafka")) checks.put("kafkaInspection",new RollingRestartEvidenceService.Check("ERROR",Map.of(),failures.get("kafka")));
        for(String key:List.of("brokerReachability","controller","offlinePartitions","underReplicatedPartitions","minimumIsr","consumerLag"))
            checks.putIfAbsent(key,new RollingRestartEvidenceService.Check("UNKNOWN",Map.of(),"Kafka inspection did not reach this check"));
        inspectHosts(requiredHostIds, failures);
        boolean hostFailure=failures.keySet().stream().anyMatch(k->k.equals("hostReachability") || k.startsWith("agent:"));
        boolean diskFailure=failures.keySet().stream().anyMatch(k->k.equals("disk") || k.startsWith("disk:"));
        outcome(checks,"hostReachability",!hostFailure,Map.of("requiredHostIds",requiredHostIds,"heartbeatTimeoutSeconds",heartbeatTimeoutSeconds),failures);
        Map<String,Object> diskMetrics=new LinkedHashMap<>();
        for(String hostId:requiredHostIds) {
            if(externalKafka!=null && externalKafka.target(hostId)){diskMetrics.put(hostId,externalKafka.diskMetrics(hostId));continue;}
            Host host=hostRepository.findById(hostId).orElse(null);
            diskMetrics.put(hostId,host!=null && host.getDiskTotalGb()!=null && host.getDiskTotalGb()>0 && host.getDiskUsedGb()!=null
                    ?Map.of("totalGb",host.getDiskTotalGb(),"usedGb",host.getDiskUsedGb()):Map.of());
        }
        boolean completeDiskMetrics=diskMetrics.values().stream().allMatch(value->value instanceof Map<?,?> values && !values.isEmpty());
        checks.put("disk",new RollingRestartEvidenceService.Check(diskFailure?"FAIL":(!hostFailure && completeDiskMetrics?"PASS":"UNKNOWN"),
                Map.of("requiredHostIds",requiredHostIds,"maxDiskUsedPercent",maxDiskUsedPercent,"completeMetrics",completeDiskMetrics,"hosts",diskMetrics),
                diskFailure?failures.toString():(!completeDiskMetrics || hostFailure?"Some disk metrics unavailable or host not reachable; existing policy is nonblocking":null)));
        return new HealthSnapshot(failures.isEmpty(), visibleBrokers, controllerId, offlinePartitions,
                underReplicatedPartitions, minIsrViolations, consumerLag, Collections.unmodifiableMap(new LinkedHashMap<>(failures)),Collections.unmodifiableMap(checks));
    }

    public void assertBrokerCanStop(UUID clusterId, int nodeId) {
        try {
            AdminClient client = kafkaAdminService.getAdminClient(clusterId);
            Set<String> names = client.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
            var minimum = loadMinimumIsr(client, names);
            if (names.isEmpty()) return;
            for (var topic : client.describeTopics(names).allTopicNames().get().values()) {
                for (var partition : topic.partitions()) {
                    if (partition.replicas().stream().noneMatch(node -> node.id() == nodeId)) continue;
                    long remaining = partition.isr().stream().filter(node -> node.id() != nodeId).count();
                    if (remaining < Math.max(1, minimum.getOrDefault(topic.name(), 1)))
                        throw new IllegalStateException("Restarting broker " + nodeId + " would leave " + topic.name() + "-" + partition.partition()
                                + " below min.insync.replicas or without an available replica.");
                }
            }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Broker safety inspection interrupted", e); }
        catch (RuntimeException e) { throw e; }
        catch (Exception e) { throw new IllegalStateException("Broker safety inspection failed", e); }
    }

    private Map<String, Integer> loadMinimumIsr(AdminClient client, Set<String> topicNames) throws Exception {
        if (topicNames.isEmpty()) return Map.of();
        Map<ConfigResource, String> resources = topicNames.stream().collect(Collectors.toMap(
                name -> new ConfigResource(ConfigResource.Type.TOPIC, name), name -> name));
        Map<ConfigResource, Config> configs = client.describeConfigs(resources.keySet()).all().get();
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
            ConfigEntry value = entry.getValue().get("min.insync.replicas");
            if (value != null && value.value() != null) {
                result.put(resources.get(entry.getKey()), Integer.parseInt(value.value()));
            }
        }
        return result;
    }

    private long totalConsumerLag(AdminClient client) throws Exception {
        long total = 0;
        for (ConsumerGroupListing group : client.listConsumerGroups().all().get()) {
            Map<TopicPartition, OffsetAndMetadata> committed = client.listConsumerGroupOffsets(group.groupId())
                    .partitionsToOffsetAndMetadata().get();
            if (committed.isEmpty()) continue;
            Map<TopicPartition, OffsetSpec> request = committed.keySet().stream()
                    .collect(Collectors.toMap(partition -> partition, ignored -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = client.listOffsets(request).all().get();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : committed.entrySet()) {
                ListOffsetsResult.ListOffsetsResultInfo end = latest.get(offset.getKey());
                if (end != null) total += Math.max(0, end.offset() - offset.getValue().offset());
            }
        }
        return total;
    }

    private void inspectHosts(Set<String> requiredHostIds, Map<String, String> failures) {
        OffsetDateTime cutoff = OffsetDateTime.now().minusSeconds(heartbeatTimeoutSeconds);
        List<String> unavailable = new ArrayList<>();
        List<String> diskPressure = new ArrayList<>();
        for (String hostId : requiredHostIds) {
            if(externalKafka != null && externalKafka.target(hostId)){externalKafka.inspectHosts(Set.of(hostId),failures,maxDiskUsedPercent,heartbeatTimeoutSeconds);continue;}
            Host host = hostRepository.findById(hostId).orElse(null);
            if (host == null || host.getLastHeartbeat() == null || host.getLastHeartbeat().isBefore(cutoff)
                    || (!"ONLINE".equalsIgnoreCase(host.getStatus())
                        && !"OCCUPIED".equalsIgnoreCase(host.getStatus()))) {
                unavailable.add(hostId);
                continue;
            }
            if (host.getDiskTotalGb() != null && host.getDiskTotalGb() > 0 && host.getDiskUsedGb() != null) {
                double usedPercent = (host.getDiskUsedGb() * 100.0) / host.getDiskTotalGb();
                if (usedPercent >= maxDiskUsedPercent) diskPressure.add(hostId + " (" + Math.round(usedPercent) + "%)");
            }
        }
        if (!unavailable.isEmpty()) failures.put("hostReachability", "Agent heartbeat missing or stale for: " + String.join(", ", unavailable));
        if (!diskPressure.isEmpty()) failures.put("disk", "Disk usage is above the safe limit on: " + String.join(", ", diskPressure));
    }

    private void outcome(Map<String,RollingRestartEvidenceService.Check> checks,String key,boolean pass,Map<String,Object> metrics,Map<String,String> failures) {
        checks.put(key,new RollingRestartEvidenceService.Check(pass?"PASS":"FAIL",metrics,pass?null:failures.toString()));
    }

    private String rootMessage(Exception exception) {
        Throwable current = exception;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    public record HealthSnapshot(boolean healthy, int visibleBrokers, Integer controllerId,
            int offlinePartitions, int underReplicatedPartitions, int minIsrViolations,
            long consumerLag, Map<String, String> failures, Map<String,RollingRestartEvidenceService.Check> checks) {
        public HealthSnapshot(boolean healthy,int visibleBrokers,Integer controllerId,int offlinePartitions,int underReplicatedPartitions,int minIsrViolations,long consumerLag,Map<String,String> failures) {
            this(healthy,visibleBrokers,controllerId,offlinePartitions,underReplicatedPartitions,minIsrViolations,consumerLag,failures,Map.of());
        }
        public String failureReason() {
            return failures.values().stream().collect(Collectors.joining("; "));
        }
    }
}
