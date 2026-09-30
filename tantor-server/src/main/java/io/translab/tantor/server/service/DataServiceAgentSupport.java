package io.translab.tantor.server.service;

/** Data-service prechecks require the updated internal agent binary. */
public final class DataServiceAgentSupport {
    private DataServiceAgentSupport() { }

    public static String precheckBlockReason(String version) {
        if (version != null && version.trim().contains("-dataservices.")) return null;
        return "Agent " + (version == null || version.isBlank() ? "version is unknown" : version)
                + " and cannot be verified for Schema Registry/Kafka Connect prechecks. "
                + "An agent update is required before deploying these roles. Install the updated internal agent.";
    }
}
