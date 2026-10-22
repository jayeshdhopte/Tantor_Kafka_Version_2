package io.translab.tantor.server.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.common.GroupType;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Read-only inventory. Never infer a group's purpose from its name. */
@Service
@RequiredArgsConstructor
public class KafkaGroupInventoryService {
    private final KafkaAdminService kafka;
    public record Group(String groupId, String protocol, String groupType, String state, String classification) {}

    public List<Group> list(UUID clusterId) {
        try {
            return kafka.getAdminClient(clusterId).listGroups(new ListGroupsOptions().timeoutMs(10000))
                    .all().get(15, TimeUnit.SECONDS).stream().map(KafkaGroupInventoryService::describe)
                    .sorted(Comparator.comparing(Group::groupId)).toList();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading Kafka group metadata.", interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("Kafka group metadata is unavailable. Check broker connectivity and group permissions.", failure);
        }
    }

    static Group describe(GroupListing listing) {
        String protocol = Objects.toString(listing.protocol(), "");
        GroupType type = listing.type().orElse(GroupType.UNKNOWN);
        String classification;
        if ("sr".equals(protocol)) classification = "SCHEMA_REGISTRY_COORDINATION";
        else if ("connect".equals(protocol)) classification = "CONNECT_COORDINATION";
        else if ("consumer".equals(protocol) || type == GroupType.CONSUMER || listing.isSimpleConsumerGroup()) classification = "CONSUMER";
        else if (type != GroupType.UNKNOWN && type != GroupType.CLASSIC) classification = "OTHER";
        else classification = "UNKNOWN";
        return new Group(listing.groupId(), protocol, type.name(), listing.groupState().map(Enum::name).orElse("UNKNOWN"), classification);
    }
}
