package io.translab.tantor.server.service;

/** Data-service prechecks require the updated internal agent binary. */
public final class DataServiceAgentSupport {
    private DataServiceAgentSupport() { }

    public static String sharedConnectBlockReason(String version) {
        var matcher = java.util.regex.Pattern.compile("-dataservices\\.([0-9]+)").matcher(version == null ? "" : version);
        if (matcher.find() && Integer.parseInt(matcher.group(1)) >= 3) return null;
        return "Using the existing unified Kafka installation requires internal agent dataservices.3 or newer. Update this host's agent before deploying Kafka Connect.";
    }

    public static String precheckBlockReason(String version) {
        if (version != null && version.trim().contains("-dataservices.")) return null;
        return "Agent " + (version == null || version.isBlank() ? "version is unknown" : version)
                + " and cannot be verified for Schema Registry/Kafka Connect prechecks. "
                + "An agent update is required before deploying these roles. Install the updated internal agent.";
    }
}
