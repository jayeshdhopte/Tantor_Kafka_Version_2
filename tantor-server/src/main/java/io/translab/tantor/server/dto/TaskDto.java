package io.translab.tantor.server.dto;

import lombok.Data;
import java.util.Map;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TaskDto {
    private String taskId;
    private String claimToken;
    private String clusterId;
    private String command;
    private Map<String, Object> parameters;
    private String artifactUrl;
    private String checksum;
}
