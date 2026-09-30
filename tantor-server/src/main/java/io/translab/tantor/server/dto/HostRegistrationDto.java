package io.translab.tantor.server.dto;

import lombok.Data;
import java.util.List;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class HostRegistrationDto {
    private String agentName;
    private String hostId;
    private String hostname;
    private List<String> ipAddresses;
    private String osDetails;
    private String agentVersion;
    private String agentPath;
}
