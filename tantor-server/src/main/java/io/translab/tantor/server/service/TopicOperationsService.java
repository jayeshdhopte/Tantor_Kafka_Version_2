package io.translab.tantor.server.service;

import lombok.RequiredArgsConstructor;
import io.translab.tantor.server.audit.AuditService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class TopicOperationsService {

    private static final int MAX_MESSAGES = 10_000;
    private final KafkaAdminService kafkaAdminService;
    private final AuditService auditService;

    public Map<String, Object> getTopicDetails(UUID clusterId, String topicName) {
        AdminClient admin = kafkaAdminService.getAdminClient(clusterId);
        try {
            TopicDescription description = admin.describeTopics(List.of(topicName))
                    .allTopicNames().get().get(topicName);
            if (description == null) {
                throw new IllegalArgumentException("Topic not found: " + topicName);
            }

            List<TopicPartition> topicPartitions = description.partitions().stream()
                    .map(partition -> new TopicPartition(topicName, partition.partition()))
                    .toList();
            Map<TopicPartition, Long> earliest = listOffsets(admin, topicPartitions, OffsetSpec.earliest());
            Map<TopicPartition, Long> latest = listOffsets(admin, topicPartitions, OffsetSpec.latest());
            Config config = describeTopicConfig(admin, topicName);

            long messageCount = 0;
            List<Map<String, Object>> partitions = new ArrayList<>();
            for (TopicPartitionInfo partition : description.partitions()) {
                TopicPartition tp = new TopicPartition(topicName, partition.partition());
                long first = earliest.getOrDefault(tp, 0L);
                long next = latest.getOrDefault(tp, 0L);
                long count = Math.max(0, next - first);
                messageCount += count;

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("partition", partition.partition());
                row.put("leader", partition.leader() == null ? null : partition.leader().id());
                row.put("replicas", partition.replicas().stream().map(node -> node.id()).toList());
                row.put("inSyncReplicas", partition.isr().stream().map(node -> node.id()).toList());
                row.put("underReplicated", partition.replicas().size() > partition.isr().size());
                row.put("firstOffset", first);
                row.put("nextOffset", next);
                row.put("messageCount", count);
                partitions.add(row);
            }

            long storedBytes = replicaStorageBytes(admin, topicName, description);
            int replicationFactor = description.partitions().isEmpty()
                    ? 0 : description.partitions().get(0).replicas().size();
            long underReplicated = description.partitions().stream()
                    .filter(p -> p.replicas().size() > p.isr().size()).count();

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("name", topicName);
            result.put("internal", description.isInternal());
            result.put("partitionCount", description.partitions().size());
            result.put("replicationFactor", replicationFactor);
            result.put("underReplicated", underReplicated);
            result.put("inSyncReplicas", description.partitions().stream().mapToInt(p -> p.isr().size()).sum());
            result.put("totalReplicas", description.partitions().stream().mapToInt(p -> p.replicas().size()).sum());
            result.put("messageCount", messageCount);
            result.put("storedBytes", storedBytes);
            result.put("segmentCount", null);
            result.put("cleanupPolicy", configValue(config, "cleanup.policy", "delete"));
            result.put("partitions", partitions);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while loading topic details");
        } catch (ExecutionException e) {
            throw kafkaFailure("load topic details", e);
        }
    }

    public List<Map<String, Object>> getTopicConfigs(UUID clusterId, String topicName) {
        try {
            Config config = describeTopicConfig(kafkaAdminService.getAdminClient(clusterId), topicName);
            return config.entries().stream()
                    .sorted(Comparator.comparing(ConfigEntry::name))
                    .map(entry -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("name", entry.name());
                        row.put("value", entry.isSensitive() ? null : entry.value());
                        row.put("defaultValue", defaultValue(entry));
                        row.put("source", entry.source().name());
                        row.put("readOnly", entry.isReadOnly());
                        row.put("sensitive", entry.isSensitive());
                        return row;
                    }).toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while loading topic settings");
        } catch (ExecutionException e) {
            throw kafkaFailure("load topic settings", e);
        }
    }

    public void alterTopicConfig(UUID clusterId, String topicName, String key, String value) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Configuration key is required");
        }
        ConfigResource resource = topicResource(topicName);
        AlterConfigOp op = new AlterConfigOp(new ConfigEntry(key.trim(), value), AlterConfigOp.OpType.SET);
        try {
            kafkaAdminService.getAdminClient(clusterId)
                    .incrementalAlterConfigs(Map.of(resource, List.of(op))).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while updating topic setting");
        } catch (ExecutionException e) {
            throw kafkaFailure("update topic setting", e);
        }
    }

    public void resetTopicConfig(UUID clusterId, String topicName, String key) {
        ConfigResource resource = topicResource(topicName);
        AlterConfigOp op = new AlterConfigOp(new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE);
        try {
            kafkaAdminService.getAdminClient(clusterId)
                    .incrementalAlterConfigs(Map.of(resource, List.of(op))).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while resetting topic setting");
        } catch (ExecutionException e) {
            throw kafkaFailure("reset topic setting", e);
        }
    }

    public Map<String, Object> getMessages(
            UUID clusterId, String topicName, List<Integer> requestedPartitions,
            String order, int limit, String search) {
        long started = System.nanoTime();
        List<ConsumerRecord<byte[], byte[]>> records = readRecords(
                clusterId, topicName, requestedPartitions, order, Math.min(Math.max(limit, 1), 500), search);
        List<Map<String, Object>> messages = records.stream().map(this::messageMap).toList();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("messages", messages);
        result.put("count", messages.size());
        result.put("bytes", records.stream().mapToLong(this::recordSize).sum());
        result.put("elapsedMs", (System.nanoTime() - started) / 1_000_000);
        return result;
    }

    public Map<String, Object> produceMessage(
            UUID clusterId, String topicName, Integer partition, String key, String value,
            Map<String, String> headers) {
        Properties properties = kafkaAdminService.getKafkaClientProperties(clusterId);
        properties.put("key.serializer", ByteArraySerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        properties.put("acks", "all");

        byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, partition, keyBytes, valueBytes);
        if (headers != null) {
            headers.forEach((name, headerValue) ->
                    record.headers().add(name, headerValue == null ? null : headerValue.getBytes(StandardCharsets.UTF_8)));
        }

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            RecordMetadata metadata = producer.send(record).get();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("partition", metadata.partition());
            result.put("offset", metadata.offset());
            result.put("timestamp", metadata.timestamp());

            String details = String.format("Partition: %d, Key size: %d bytes, Value size: %d bytes", 
                    partition != null ? partition : -1,
                    keyBytes != null ? keyBytes.length : 0,
                    valueBytes != null ? valueBytes.length : 0);
            auditService.record("DATA_SERVICES", "PRODUCE_MESSAGE", "TOPIC", topicName, clusterId, 
                    "SUCCESS", null, null, null, details);

            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while producing message");
        } catch (ExecutionException e) {
            throw kafkaFailure("produce message", e);
        }
    }

    public void clearTopic(UUID clusterId, String topicName) {
        AdminClient admin = kafkaAdminService.getAdminClient(clusterId);
        try {
            Config config = describeTopicConfig(admin, topicName);
            String cleanupPolicy = configValue(config, "cleanup.policy", "delete");
            if (Arrays.stream(cleanupPolicy.split(",")).noneMatch("delete"::equalsIgnoreCase)) {
                throw new IllegalStateException("Clearing messages requires cleanup.policy to include delete");
            }

            TopicDescription description = admin.describeTopics(List.of(topicName))
                    .allTopicNames().get().get(topicName);
            List<TopicPartition> partitions = description.partitions().stream()
                    .map(p -> new TopicPartition(topicName, p.partition())).toList();
            Map<TopicPartition, Long> latest = listOffsets(admin, partitions, OffsetSpec.latest());
            Map<TopicPartition, RecordsToDelete> deletion = latest.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> RecordsToDelete.beforeOffset(entry.getValue())));
            admin.deleteRecords(deletion).all().get();
            
            auditService.record("DATA_SERVICES", "CLEAR_TOPIC", "TOPIC", topicName, clusterId, 
                    "SUCCESS", null, null, null, "Messages cleared from all partitions");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while clearing topic");
        } catch (ExecutionException e) {
            throw kafkaFailure("clear topic", e);
        }
    }

    public synchronized void recreateTopic(UUID clusterId, String topicName) {
        AdminClient admin = kafkaAdminService.getAdminClient(clusterId);
        try {
            TopicDescription description = admin.describeTopics(List.of(topicName))
                    .allTopicNames().get().get(topicName);
            Config currentConfig = describeTopicConfig(admin, topicName);

            Map<Integer, List<Integer>> assignments = new LinkedHashMap<>();
            description.partitions().forEach(partition -> assignments.put(
                    partition.partition(), partition.replicas().stream().map(node -> node.id()).toList()));
            Map<String, String> explicitConfigs = currentConfig.entries().stream()
                    .filter(entry -> !entry.isDefault() && !entry.isSensitive() && entry.value() != null)
                    .filter(entry -> !entry.isReadOnly())
                    .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value, (a, b) -> a, LinkedHashMap::new));

            admin.deleteTopics(List.of(topicName)).all().get();
            waitForTopic(admin, topicName, false);
            admin.createTopics(List.of(new NewTopic(topicName, assignments).configs(explicitConfigs))).all().get();
            waitForTopic(admin, topicName, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while recreating topic");
        } catch (ExecutionException e) {
            throw kafkaFailure("recreate topic", e);
        }
    }

    public List<Map<String, Object>> getTopicConsumers(UUID clusterId, String topicName) {
        AdminClient admin = kafkaAdminService.getAdminClient(clusterId);
        try {
            Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().all().get();
            if (groups.isEmpty()) {
                return List.of();
            }
            List<String> ids = groups.stream().map(ConsumerGroupListing::groupId).toList();
            Map<String, ConsumerGroupDescription> descriptions =
                    admin.describeConsumerGroups(ids).all().get();
            Map<TopicPartition, Long> latest = latestOffsetsForTopic(admin, topicName);
            List<Map<String, Object>> result = new ArrayList<>();

            for (String groupId : ids) {
                Map<TopicPartition, OffsetAndMetadata> offsets;
                try {
                    offsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                } catch (ExecutionException ignored) {
                    continue;
                }
                List<Map.Entry<TopicPartition, OffsetAndMetadata>> topicOffsets = offsets.entrySet().stream()
                        .filter(entry -> entry.getKey().topic().equals(topicName)).toList();
                ConsumerGroupDescription description = descriptions.get(groupId);
                boolean activelyAssigned = description != null && description.members().stream()
                        .flatMap(member -> member.assignment().topicPartitions().stream())
                        .anyMatch(tp -> tp.topic().equals(topicName));
                if (topicOffsets.isEmpty() && !activelyAssigned) {
                    continue;
                }

                long lag = topicOffsets.stream().mapToLong(entry ->
                        Math.max(0, latest.getOrDefault(entry.getKey(), entry.getValue().offset())
                                - entry.getValue().offset())).sum();
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("groupId", groupId);
                row.put("activeConsumers", description == null ? 0 : description.members().size());
                row.put("lag", lag);
                row.put("coordinator", description == null || description.coordinator() == null
                        ? null : description.coordinator().host() + ":" + description.coordinator().port());
                row.put("state", description == null ? "UNKNOWN" : description.state().toString());
                result.add(row);
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while loading topic consumers");
        } catch (ExecutionException e) {
            throw kafkaFailure("load topic consumers", e);
        }
    }

    public List<Map<String, Object>> getTopicAcls(UUID clusterId, String topicName) {
        try {
            Collection<AclBinding> bindings = kafkaAdminService.getAdminClient(clusterId)
                    .describeAcls(AclBindingFilter.ANY).values().get();
            return bindings.stream()
                    .filter(binding -> binding.pattern().resourceType() == ResourceType.TOPIC)
                    .filter(binding -> binding.pattern().name().equals(topicName)
                            || binding.pattern().name().equals("*"))
                    .map(binding -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("principal", binding.entry().principal());
                        row.put("host", binding.entry().host());
                        row.put("operation", binding.entry().operation().name());
                        row.put("permissionType", binding.entry().permissionType().name());
                        row.put("patternType", binding.pattern().patternType().name());
                        row.put("resourceName", binding.pattern().name());
                        return row;
                    }).toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while loading topic ACLs");
        } catch (ExecutionException e) {
            throw kafkaFailure("load topic ACLs", e);
        }
    }

    public Map<String, Object> analyzeTopic(UUID clusterId, String topicName, int requestedLimit) {
        int limit = Math.min(Math.max(requestedLimit, 1), MAX_MESSAGES);
        List<ConsumerRecord<byte[], byte[]>> records =
                readRecords(clusterId, topicName, List.of(), "oldest", limit, null);
        List<Integer> keySizes = records.stream().map(record -> record.key() == null ? 0 : record.key().length).sorted().toList();
        List<Integer> valueSizes = records.stream().map(record -> record.value() == null ? 0 : record.value().length).sorted().toList();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("analyzedAt", System.currentTimeMillis());
        result.put("sampleLimit", limit);
        result.put("truncated", records.size() == limit);
        result.put("messageCount", records.size());
        result.put("minOffset", records.stream().mapToLong(ConsumerRecord::offset).min().orElse(0));
        result.put("maxOffset", records.stream().mapToLong(ConsumerRecord::offset).max().orElse(0));
        result.put("minTimestamp", records.stream().mapToLong(ConsumerRecord::timestamp).min().orElse(0));
        result.put("maxTimestamp", records.stream().mapToLong(ConsumerRecord::timestamp).max().orElse(0));
        result.put("nullKeys", records.stream().filter(record -> record.key() == null).count());
        result.put("uniqueKeys", records.stream().map(record -> bytesKey(record.key())).distinct().count());
        result.put("nullValues", records.stream().filter(record -> record.value() == null).count());
        result.put("uniqueValues", records.stream().map(record -> bytesKey(record.value())).distinct().count());
        result.put("keySize", sizeStatistics(keySizes));
        result.put("valueSize", sizeStatistics(valueSizes));

        Map<Integer, List<ConsumerRecord<byte[], byte[]>>> byPartition =
                records.stream().collect(Collectors.groupingBy(ConsumerRecord::partition, TreeMap::new, Collectors.toList()));
        result.put("partitions", byPartition.entrySet().stream().map(entry -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("partition", entry.getKey());
            row.put("totalMessages", entry.getValue().size());
            row.put("minOffset", entry.getValue().stream().mapToLong(ConsumerRecord::offset).min().orElse(0));
            row.put("maxOffset", entry.getValue().stream().mapToLong(ConsumerRecord::offset).max().orElse(0));
            return row;
        }).toList());
        return result;
    }

    private List<ConsumerRecord<byte[], byte[]>> readRecords(
            UUID clusterId, String topicName, List<Integer> requestedPartitions,
            String order, int limit, String search) {
        Properties properties = kafkaAdminService.getKafkaClientProperties(clusterId);
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit", "false");
        properties.put("group.id", "tantor-browser-" + UUID.randomUUID());
        properties.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            List<TopicPartition> available = consumer.partitionsFor(topicName, Duration.ofSeconds(10)).stream()
                    .map(info -> new TopicPartition(topicName, info.partition())).toList();
            Set<Integer> selected = requestedPartitions == null ? Set.of() : new HashSet<>(requestedPartitions);
            List<TopicPartition> partitions = selected.isEmpty() ? available : available.stream()
                    .filter(tp -> selected.contains(tp.partition())).toList();
            if (partitions.isEmpty()) {
                return List.of();
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            boolean newest = !"oldest".equalsIgnoreCase(order);
            for (TopicPartition partition : partitions) {
                long beginning = beginnings.getOrDefault(partition, 0L);
                long end = ends.getOrDefault(partition, 0L);
                consumer.seek(partition, newest ? Math.max(beginning, end - limit) : beginning);
            }

            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 3_000;
            int emptyPolls = 0;
            while (System.currentTimeMillis() < deadline && records.size() < Math.max(limit * partitions.size(), limit)) {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(250));
                if (polled.isEmpty()) {
                    if (++emptyPolls >= 2) break;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<byte[], byte[]> record : polled) {
                    if (matches(record, search)) records.add(record);
                }
                boolean atEnd = partitions.stream().allMatch(tp -> consumer.position(tp) >= ends.getOrDefault(tp, 0L));
                if (atEnd) break;
            }

            Comparator<ConsumerRecord<byte[], byte[]>> comparator =
                    Comparator.comparingLong((ConsumerRecord<byte[], byte[]> record) -> record.timestamp())
                            .thenComparingInt(ConsumerRecord::partition)
                            .thenComparingLong(ConsumerRecord::offset);
            records.sort(newest ? comparator.reversed() : comparator);
            return records.stream().limit(limit).toList();
        }
    }

    private Map<String, Object> messageMap(ConsumerRecord<byte[], byte[]> record) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("partition", record.partition());
        row.put("offset", record.offset());
        row.put("timestamp", record.timestamp());
        row.put("key", asUtf8(record.key()));
        row.put("value", asUtf8(record.value()));
        row.put("keySize", record.key() == null ? 0 : record.key().length);
        row.put("valueSize", record.value() == null ? 0 : record.value().length);
        Map<String, List<String>> headers = new LinkedHashMap<>();
        record.headers().forEach(header -> headers.computeIfAbsent(header.key(), ignored -> new ArrayList<>())
                .add(asUtf8(header.value())));
        row.put("headers", headers);
        return row;
    }

    private boolean matches(ConsumerRecord<byte[], byte[]> record, String search) {
        if (search == null || search.isBlank()) return true;
        String needle = search.toLowerCase(Locale.ROOT);
        return Optional.ofNullable(asUtf8(record.key())).orElse("").toLowerCase(Locale.ROOT).contains(needle)
                || Optional.ofNullable(asUtf8(record.value())).orElse("").toLowerCase(Locale.ROOT).contains(needle);
    }

    private long recordSize(ConsumerRecord<byte[], byte[]> record) {
        return (record.key() == null ? 0 : record.key().length)
                + (record.value() == null ? 0 : record.value().length);
    }

    private String asUtf8(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private String bytesKey(byte[] bytes) {
        return bytes == null ? "<null>" : Base64.getEncoder().encodeToString(bytes);
    }

    private Map<String, Object> sizeStatistics(List<Integer> sizes) {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("total", sizes.stream().mapToLong(Integer::longValue).sum());
        stats.put("min", sizes.stream().mapToInt(Integer::intValue).min().orElse(0));
        stats.put("max", sizes.stream().mapToInt(Integer::intValue).max().orElse(0));
        stats.put("average", sizes.stream().mapToInt(Integer::intValue).average().orElse(0));
        stats.put("p50", percentile(sizes, 0.50));
        stats.put("p75", percentile(sizes, 0.75));
        stats.put("p95", percentile(sizes, 0.95));
        stats.put("p99", percentile(sizes, 0.99));
        stats.put("p999", percentile(sizes, 0.999));
        return stats;
    }

    private int percentile(List<Integer> sorted, double percentile) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private Map<TopicPartition, Long> latestOffsetsForTopic(AdminClient admin, String topicName)
            throws InterruptedException, ExecutionException {
        TopicDescription description = admin.describeTopics(List.of(topicName))
                .allTopicNames().get().get(topicName);
        List<TopicPartition> partitions = description.partitions().stream()
                .map(p -> new TopicPartition(topicName, p.partition())).toList();
        return listOffsets(admin, partitions, OffsetSpec.latest());
    }

    private Map<TopicPartition, Long> listOffsets(
            AdminClient admin, Collection<TopicPartition> partitions, OffsetSpec spec)
            throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetSpec> request = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, ignored -> spec));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> values =
                admin.listOffsets(request).all().get();
        return values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    private Config describeTopicConfig(AdminClient admin, String topicName)
            throws InterruptedException, ExecutionException {
        return admin.describeConfigs(List.of(topicResource(topicName)), new DescribeConfigsOptions().includeSynonyms(true))
                .all().get().get(topicResource(topicName));
    }

    private ConfigResource topicResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }

    private String configValue(Config config, String key, String fallback) {
        ConfigEntry entry = config.get(key);
        return entry == null || entry.value() == null ? fallback : entry.value();
    }

    private String defaultValue(ConfigEntry entry) {
        return entry.synonyms().stream()
                .filter(synonym -> synonym.source() == ConfigEntry.ConfigSource.DEFAULT_CONFIG)
                .map(ConfigEntry.ConfigSynonym::value)
                .filter(Objects::nonNull)
                .findFirst().orElse(null);
    }

    private long replicaStorageBytes(AdminClient admin, String topicName, TopicDescription description) {
        try {
            Set<Integer> brokerIds = description.partitions().stream()
                    .flatMap(partition -> partition.replicas().stream()).map(node -> node.id())
                    .collect(Collectors.toSet());
            Map<Integer, Map<String, LogDirDescription>> descriptions =
                    admin.describeLogDirs(brokerIds).allDescriptions().get();
            long allReplicaBytes = descriptions.values().stream()
                    .flatMap(logDirs -> logDirs.values().stream())
                    .flatMap(logDir -> logDir.replicaInfos().entrySet().stream())
                    .filter(entry -> entry.getKey().topic().equals(topicName))
                    .mapToLong(entry -> entry.getValue().size()).sum();
            int replicationFactor = description.partitions().isEmpty()
                    ? 1 : Math.max(1, description.partitions().get(0).replicas().size());
            return allReplicaBytes / replicationFactor;
        } catch (Exception ignored) {
            return -1;
        }
    }

    private void waitForTopic(AdminClient admin, String topicName, boolean expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                boolean present = admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get().contains(topicName);
                if (present == expected) return;
            } catch (ExecutionException ignored) {
                // Metadata is expected to be briefly inconsistent while the topic is recreated.
            }
            Thread.sleep(200);
        }
        throw new RuntimeException("Timed out waiting for topic metadata to refresh");
    }

    private RuntimeException kafkaFailure(String operation, ExecutionException error) {
        Throwable cause = error.getCause() == null ? error : error.getCause();
        return new RuntimeException("Failed to " + operation + ": " + cause.getMessage(), cause);
    }
}
