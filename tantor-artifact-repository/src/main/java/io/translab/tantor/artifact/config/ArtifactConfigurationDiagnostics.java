package io.translab.tantor.artifact.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;

@Component
public class ArtifactConfigurationDiagnostics implements InitializingBean {
    private static final Logger log = LoggerFactory.getLogger(ArtifactConfigurationDiagnostics.class);
    private final CorsProperties cors;
    private final StorageProperties storage;
    private final MalwareScanProperties malwareScan;
    private final Environment environment;

    public ArtifactConfigurationDiagnostics(CorsProperties cors, StorageProperties storage,
            MalwareScanProperties malwareScan, Environment environment) {
        this.cors = cors;
        this.storage = storage;
        this.malwareScan = malwareScan;
        this.environment = environment;
    }

    @Override
    public void afterPropertiesSet() {
        boolean productionLike = Arrays.stream(environment.getActiveProfiles())
                .anyMatch(profile -> "sit".equalsIgnoreCase(profile) || "uat".equalsIgnoreCase(profile)
                        || "production".equalsIgnoreCase(profile));
        if (productionLike) {
            for (String value : cors.getAllowedOrigins()) {
                URI origin = URI.create(value);
                if (!"https".equalsIgnoreCase(origin.getScheme()) || isLoopback(origin.getHost())) {
                    throw new IllegalStateException("Production-like CORS origins must use HTTPS and cannot be loopback hosts");
                }
            }
        }
        log.info("Effective non-secret configuration: profiles={}, corsOrigins={}, repositoryBasePath={}, uploadLimitBytes={}, malwareScanEnabled={}, malwareScanEndpoint={}",
                Arrays.asList(environment.getActiveProfiles()), cors.getAllowedOrigins(),
                storage.getBasePath(), storage.getMaxUploadBytes(), malwareScan.isEnabled(),
                malwareScan.isEnabled() ? malwareScan.getHost() + ":" + malwareScan.getPort() : "<disabled>");
    }

    private boolean isLoopback(String host) {
        return "localhost".equalsIgnoreCase(host) || "::1".equals(host)
                || host.matches("127(?:\\.\\d{1,3}){3}");
    }
}
