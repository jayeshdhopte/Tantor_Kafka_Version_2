package io.translab.tantor.server.dto;

import lombok.Builder;
import lombok.Data;
import java.time.OffsetDateTime;

@Data
@Builder
public class BrokerSummaryDto {
    private Integer brokerId;
    private String hostname;
    private String role; // broker, controller, broker_controller
    
    // Statuses
    private String brokerHealth; // HEALTHY, DEGRADED, OFFLINE
    private boolean isController;
    private boolean isJmxReachable; 

    // Hardware Metrics
    private Double cpuUsagePct;
    private Long memoryUsedMb;
    private Long memoryTotalMb;
    private Long diskUsedGb;
    private Long diskTotalGb;
    private Long diskUsedBytes;
    private Long diskTotalBytes;
    private String hostMetricStatus; // LIVE, STALE, UNAVAILABLE

    // Kafka Throughput
    private Double messagesInPerSec;
    private Double bytesInPerSec;
    private Double bytesOutPerSec;
    
    // Metadata
    private OffsetDateTime lastHeartbeat;
    private Long metricsTimestamp;
}
