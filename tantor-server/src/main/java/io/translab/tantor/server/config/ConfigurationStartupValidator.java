package io.translab.tantor.server.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class ConfigurationStartupValidator implements InitializingBean {
    private final RuntimeEnvironmentProperties runtime;
    private final ArtifactRepositoryProperties artifactRepository;
    private final ControlPlaneProperties controlPlane;
    private final MonitoringProperties monitoring;
    private final OidcProperties oidc;
    private final KafkaDeploymentProperties kafkaDeployment;
    private final Environment springEnvironment;

    @Override
    public void afterPropertiesSet() {
        validate();
        log.info("Effective non-secret configuration: {}", diagnosticSummary());
    }

    void validate() {
        validateProfileSelection();
        validateOidcAliases();
        if (monitoring.isGrafanaSkipTlsValidation()) {
            throw new IllegalStateException("TANTOR_GRAFANA_SKIP_TLS_VALIDATION is forbidden");
        }
        if (!runtime.isProductionLike()) return;

        URI artifactUrl = artifactRepository.getInternalUrl();
        rejectLoopback("internal Artifact Repository", artifactUrl);
        if ("http".equalsIgnoreCase(artifactUrl.getScheme()) && !isPrivateServiceName(artifactUrl.getHost())) {
            throw new IllegalStateException("Production Artifact Repository must use HTTPS unless it is a private single-label service name");
        }
        URI publicArtifactUrl = artifactRepository.getPublicUrl();
        requirePublicHttps("public Artifact Repository", publicArtifactUrl);
        requirePublicHttps("public control plane", controlPlane.getPublicUrl());
        if (!sameAuthority(publicArtifactUrl, controlPlane.getPublicUrl())) {
            throw new IllegalStateException("Public Artifact Repository and control-plane URLs must use the same HTTPS authority");
        }

        URI issuer = oidc.getIssuerUri();
        if (issuer == null || !"https".equalsIgnoreCase(issuer.getScheme()) || issuer.getHost() == null) {
            throw new IllegalStateException("Production TANTOR_OIDC_ISSUER_URI must be an absolute HTTPS URL");
        }
        rejectLoopback("OIDC issuer", issuer);
        if (oidc.getAudience() == null || oidc.getAudience().isBlank()) {
            throw new IllegalStateException("Production TANTOR_OIDC_AUDIENCE is required");
        }

        URI monitoringEndpoint = "grafana-proxy".equalsIgnoreCase(monitoring.getMode())
                ? monitoring.getGrafanaUrl() : monitoring.getPrometheusUrl();
        rejectLoopback("Monitoring endpoint", monitoringEndpoint);
        if ("http".equalsIgnoreCase(monitoringEndpoint.getScheme())
                && !isPrivateServiceName(monitoringEndpoint.getHost())) {
            throw new IllegalStateException("Production monitoring HTTP is allowed only for a private single-label service name");
        }

    }

    Map<String, Object> diagnosticSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("environment", runtime.getEnvironment());
        summary.put("profiles", Arrays.asList(springEnvironment.getActiveProfiles()));
        summary.put("artifactRepositoryInternal", endpointSummary(artifactRepository.getInternalUrl()));
        summary.put("artifactRepositoryPublic", endpointSummary(artifactRepository.getPublicUrl()));
        summary.put("controlPlanePublic", endpointSummary(controlPlane.getPublicUrl()));
        summary.put("monitoringMode", monitoring.getMode());
        summary.put("monitoringEndpoint", endpointSummary("grafana-proxy".equalsIgnoreCase(monitoring.getMode())
                ? monitoring.getGrafanaUrl() : monitoring.getPrometheusUrl()));
        summary.put("oidcIssuer", endpointSummary(oidc.getIssuerUri()));
        summary.put("oidcRealm", oidcRealm(oidc.getIssuerUri()));
        summary.put("oidcAudienceConfigured", oidc.getAudience() != null && !oidc.getAudience().isBlank());
        summary.put("controlPlanePort", springEnvironment.getProperty("server.port", "<not-configured>"));
        summary.put("kafkaDeploymentSecurityMode", kafkaDeployment.getSecurityMode());
        summary.put("kafkaServicePrefix", kafkaDeployment.getServicePrefix());
        summary.put("monitoringCredentialsConfigured", monitoring.getGrafanaUsername() != null
                && !monitoring.getGrafanaUsername().isBlank());
        summary.put("exporterPorts", monitoring.getKafkaExporterPortBase() + "/"
                + monitoring.getJmxExporterPort() + "/" + monitoring.getControllerJmxExporterPort());
        return summary;
    }

    private String endpointSummary(URI uri) {
        if (uri == null) return "<not-configured>";
        int port = uri.getPort();
        return uri.getScheme() + "://" + uri.getHost() + (port < 0 ? "" : ":" + port);
    }

    private String oidcRealm(URI issuer) {
        if (issuer == null || issuer.getPath() == null) return "<not-configured>";
        String marker = "/realms/";
        int markerIndex = issuer.getPath().indexOf(marker);
        return markerIndex < 0 ? "<not-configured>" : issuer.getPath().substring(markerIndex + marker.length());
    }

    private void rejectLoopback(String name, URI uri) {
        if (uri == null || uri.getHost() == null || isLoopback(uri.getHost())) {
            throw new IllegalStateException(name + " cannot use a loopback host outside development");
        }
    }

    private void requirePublicHttps(String name, URI uri) {
        if (uri == null || !"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null
                || uri.getUserInfo() != null || isLoopback(uri.getHost()) || isPlaceholder(uri.getHost())) {
            throw new IllegalStateException(name + " must be a non-placeholder, non-loopback HTTPS URL without credentials");
        }
    }

    private boolean sameAuthority(URI left, URI right) {
        return left != null && right != null && left.getScheme().equalsIgnoreCase(right.getScheme())
                && left.getHost().equalsIgnoreCase(right.getHost()) && effectivePort(left) == effectivePort(right);
    }

    private int effectivePort(URI uri) {
        return uri.getPort() >= 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
    }

    private boolean isPlaceholder(String host) {
        String value = host == null ? "" : host.toLowerCase(Locale.ROOT);
        return value.endsWith(".example") || value.endsWith(".invalid") || value.endsWith(".test");
    }

    private void validateProfileSelection() {
        Map<String, RuntimeEnvironmentProperties.Environment> supported = Map.of(
                "dev", RuntimeEnvironmentProperties.Environment.DEVELOPMENT,
                "sit", RuntimeEnvironmentProperties.Environment.SIT,
                "uat", RuntimeEnvironmentProperties.Environment.UAT,
                "production", RuntimeEnvironmentProperties.Environment.PRODUCTION);
        var selected = Arrays.stream(springEnvironment.getActiveProfiles())
                .map(profile -> profile.toLowerCase(Locale.ROOT))
                .filter(supported::containsKey)
                .distinct()
                .toList();
        if (selected.size() != 1) {
            throw new IllegalStateException("Exactly one deployment profile is required: dev, sit, uat, or production");
        }
        if (supported.get(selected.get(0)) != runtime.getEnvironment()) {
            throw new IllegalStateException("Runtime environment " + runtime.getEnvironment()
                    + " conflicts with active deployment profile " + selected.get(0));
        }
    }

    private void validateOidcAliases() {
        rejectConflictingAlias("TANTOR_OIDC_ISSUER_URI", "TANTOR_KEYCLOAK_ISSUER_URI");
        rejectConflictingAlias("TANTOR_OIDC_AUDIENCE", "TANTOR_KEYCLOAK_CLIENT_ID");
    }

    private void rejectConflictingAlias(String canonicalName, String legacyName) {
        String canonical = springEnvironment.getProperty(canonicalName);
        String legacy = springEnvironment.getProperty(legacyName);
        if (canonical != null && !canonical.isBlank() && legacy != null && !legacy.isBlank()
                && !canonical.trim().equals(legacy.trim())) {
            throw new IllegalStateException(canonicalName + " conflicts with deprecated " + legacyName);
        }
    }

    private boolean isLoopback(String host) {
        return "localhost".equalsIgnoreCase(host) || "::1".equals(host)
                || host.matches("127(?:\\.\\d{1,3}){3}");
    }

    private boolean isPrivateServiceName(String host) {
        return host != null && !host.contains(".") && !host.contains(":");
    }
}
