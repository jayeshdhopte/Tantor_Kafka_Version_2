package io.translab.tantor.server.dto;

public record DiscoveredServicePath(String serviceType, String serviceName, int restPort,
        String installDir, String configFile, String pluginDir, String workingDir, String logDir) {}
