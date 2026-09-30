package io.translab.tantor.server.dto;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class PartitionSummaryDto {
    private String topicName;
    private int partitionId;
    private int leaderBroker;
    private String leaderHostname;
    private List<Integer> replicaBrokers;
    private List<Integer> isrBrokers;
    private long earliestOffset;
    private long latestOffset;
    private long messageCount;
    private boolean underReplicated;
    private String health; // HEALTHY, WARNING (Under-Replicated), CRITICAL (Offline)
}
