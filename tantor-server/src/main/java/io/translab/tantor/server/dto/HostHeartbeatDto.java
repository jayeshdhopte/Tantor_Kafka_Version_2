package io.translab.tantor.server.dto;

import lombok.Data;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class HostHeartbeatDto {
    private String hostId;
    private Double cpuUsagePct;
    private Long memTotalMb;
    private Long memUsedMb;
    private Long diskTotalGb;
    private Long diskUsedGb;
    private String javaVersion;
}
