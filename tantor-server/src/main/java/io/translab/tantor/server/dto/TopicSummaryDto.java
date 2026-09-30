package io.translab.tantor.server.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicSummaryDto {
    private String name;
    private int partitionCount;
    private int replicationFactor;
    private long underReplicated;
    private long messageCount;
    private boolean internal;
}
