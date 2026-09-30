package io.translab.tantor.server.dto;

import lombok.Data;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.util.List;
import java.util.Map;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TaskResultDto {
    private String taskId;
    private String claimToken;
    private String hostId;
    private String status; // SUCCESS, FAILED
    private String logOutput;
    private String errorMsg;
    private String currentStep;
    private String failedReason;
    private String planHash;
    private List<Map<String, Object>> checks;
}
