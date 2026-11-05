package io.translab.tantor.server.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Observation-only contract. A PASS never authorizes deployment. */
public final class AddNodeReadiness {
    private AddNodeReadiness() {}
    public record Request(String hostId,String brokerRack) {}
    public record Check(String code, String name, String phase, String status, Object expected,
                        Object observed, String remedy, Instant observedAt, String source) {}
    public record Preview(UUID previewId, UUID clusterId, String hostId, String contextHash,
                          Instant checkedAt, Instant expiresAt, String status, List<Check> checks,
                          Map<String, Object> brokerPlan, boolean deploymentEnabled) {}
}
