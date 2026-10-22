package io.translab.tantor.server.service;

/** Only new deployments are normalized; existing cluster paths remain authoritative. */
public final class DeploymentInstallPaths {
    private DeploymentInstallPaths() {}
    public static String brandedKafkaBase(Object requested) {
        String base = requested == null ? "/opt" : requested.toString().trim();
        if (base.isBlank()) base = "/opt";
        if (!base.startsWith("/") || base.indexOf('\r') >= 0 || base.indexOf('\n') >= 0 || base.indexOf('\0') >= 0
                || java.util.Arrays.asList(base.split("/")).contains(".."))
            throw new IllegalArgumentException("Install directory must be an absolute Linux path without parent traversal.");
        base = base.replaceAll("/+", "/").replaceAll("/+$", "");
        String leaf = base.substring(base.lastIndexOf('/') + 1);
        if (leaf.equals("kafka") || leaf.startsWith("kafka_")) base = base.substring(0, Math.max(0, base.lastIndexOf('/')));
        if (!base.equals("/tantor") && !base.endsWith("/tantor")) base += "/tantor";
        return base;
    }
}
