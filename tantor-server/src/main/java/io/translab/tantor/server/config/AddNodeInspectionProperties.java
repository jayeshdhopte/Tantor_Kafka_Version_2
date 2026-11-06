package io.translab.tantor.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.List;

/** Explicit opt-in for participant observation; independent of broker execution policy. */
@Data @Component @ConfigurationProperties(prefix = "tantor.add-node-inspection")
public class AddNodeInspectionProperties {
    private List<String> allowedHostIds = List.of();
    private List<String> excludedHostIds = List.of();
    private List<String> excludedHostIps = List.of();
    public boolean allows(String hostId, String hostIp) {
        return allowedHostIds.contains(java.util.Objects.toString(hostId, ""))
                && !excludes(hostId, hostIp);
    }
    public boolean excludes(String hostId, String hostIp) {
        return excludedHostIds.contains(java.util.Objects.toString(hostId, ""))
                || excludedHostIps.contains(java.util.Objects.toString(hostIp, ""));
    }
}
