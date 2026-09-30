package io.translab.tantor.server.config;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigurationStartupValidatorTest {
    @Test
    void typedPropertiesRejectMissingUrlAndInvalidPort() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(new ArtifactRepositoryProperties())).isNotEmpty();
            ArtifactRepositoryProperties malformed = new ArtifactRepositoryProperties();
            malformed.setInternalUrl(URI.create("not-a-url"));
            malformed.setPublicUrl(URI.create("https://tantor.corp.internal"));
            assertThat(validator.validate(malformed)).isNotEmpty();
            MonitoringProperties monitoring = validMonitoring();
            monitoring.setJmxExporterPort(70000);
            assertThat(validator.validate(monitoring)).isNotEmpty();
        }
    }

    @Test
    void effectiveConfigurationBindsFromDeploymentProperties() {
        Binder binder = new Binder(new MapConfigurationPropertySource(Map.of(
                "tantor.artifact-repo.internal-url", "http://artifact-repository:8081",
                "tantor.artifact-repo.public-url", "https://tantor.corp.internal",
                "tantor.artifact-repo.jmx-exporter-artifact-id", "artifact-1")));
        ArtifactRepositoryProperties properties = binder
                .bind("tantor.artifact-repo", Bindable.of(ArtifactRepositoryProperties.class))
                .orElseThrow(() -> new AssertionError("artifact repository properties did not bind"));
        assertThat(properties.getInternalUrl()).isEqualTo(URI.create("http://artifact-repository:8081"));
        assertThat(properties.getPublicUrl()).isEqualTo(URI.create("https://tantor.corp.internal"));
        assertThat(properties.getJmxExporterArtifactId()).isEqualTo("artifact-1");
    }

    @Test
    void productionRejectsLoopbackAndRequiresHttpsOidc() {
        ArtifactRepositoryProperties artifacts = artifacts("http://localhost:8081", "https://tantor.corp.internal");
        OidcProperties oidc = oidc("http://identity.example/realms/tantor", "tantor-ui");
        var validator = startupValidator(artifacts, validMonitoring(), oidc, "production");
        assertThatThrownBy(validator::validate).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void validProductionBindsAndDiagnosticsNeverContainCredentialValues() {
        MonitoringProperties monitoring = validMonitoring();
        monitoring.setGrafanaUsername("operator");
        monitoring.setGrafanaPassword("credential-must-not-appear");
        var validator = startupValidator(artifacts("http://tantor-artifact-repository:8081", "https://tantor.corp.internal"), monitoring,
                oidc("https://identity.corp.internal/realms/tantor", "tantor-ui"), "production");
        validator.validate();
        assertThat(validator.diagnosticSummary().toString())
                .contains("PRODUCTION", "identity.corp.internal", "tantor-artifact-repository")
                .doesNotContain("credential-must-not-appear", "operator");
    }

    @Test
    void rejectsMixedOrMismatchedDeploymentProfiles() {
        for (String[] profiles : new String[][] {
                { "dev", "sit" }, { "dev", "uat" }, { "dev", "production" },
                { "sit", "uat" }, { "sit", "production" }, { "uat", "production" } }) {
            var mixed = startupValidatorForEnvironment(validArtifacts(), validMonitoring(), validOidc(),
                    RuntimeEnvironmentProperties.Environment.PRODUCTION, profiles);
            assertThatThrownBy(mixed::validate).hasMessageContaining("Exactly one deployment profile");
        }

        var mismatched = startupValidatorForEnvironment(validArtifacts(), validMonitoring(), validOidc(),
                RuntimeEnvironmentProperties.Environment.UAT, "production");
        assertThatThrownBy(mismatched::validate).hasMessageContaining("conflicts with active deployment profile");
    }

    @Test
    void rejectsConflictingLegacyOidcAliasesButAcceptsIdenticalAliases() {
        var conflicting = startupValidator(validArtifacts(), validMonitoring(), validOidc(), "production");
        conflictingEnvironment(conflicting, "https://other.corp.internal/realms/tantor", "other-client");
        assertThatThrownBy(conflicting::validate).hasMessageContaining("conflicts with deprecated");

        var identical = startupValidator(validArtifacts(), validMonitoring(), validOidc(), "production");
        conflictingEnvironment(identical, validOidc().getIssuerUri().toString(), validOidc().getAudience());
        identical.validate();
    }

    private ConfigurationStartupValidator startupValidator(ArtifactRepositoryProperties artifacts,
            MonitoringProperties monitoring, OidcProperties oidc, String... profiles) {
        return startupValidatorForEnvironment(artifacts, monitoring, oidc,
                RuntimeEnvironmentProperties.Environment.valueOf(profiles[0].toUpperCase()), profiles);
    }

    private ConfigurationStartupValidator startupValidatorForEnvironment(ArtifactRepositoryProperties artifacts,
            MonitoringProperties monitoring, OidcProperties oidc,
            RuntimeEnvironmentProperties.Environment selectedEnvironment, String... profiles) {
        RuntimeEnvironmentProperties runtime = new RuntimeEnvironmentProperties();
        runtime.setEnvironment(selectedEnvironment);
        MockEnvironment environment = new MockEnvironment();
        environment.setActiveProfiles(profiles);
        environment.setProperty("server.port", "8443");
        KafkaDeploymentProperties kafka = new KafkaDeploymentProperties();
        kafka.setSecurityMode("SSL");
        ControlPlaneProperties controlPlane = new ControlPlaneProperties();
        controlPlane.setPublicUrl(artifacts.getPublicUrl());
        return new ConfigurationStartupValidator(runtime, artifacts, controlPlane, monitoring, oidc, kafka, environment);
    }

    private void conflictingEnvironment(ConfigurationStartupValidator validator, String issuer, String audience) {
        try {
            var field = ConfigurationStartupValidator.class.getDeclaredField("springEnvironment");
            field.setAccessible(true);
            MockEnvironment environment = (MockEnvironment) field.get(validator);
            environment.setProperty("TANTOR_OIDC_ISSUER_URI", validOidc().getIssuerUri().toString());
            environment.setProperty("TANTOR_KEYCLOAK_ISSUER_URI", issuer);
            environment.setProperty("TANTOR_OIDC_AUDIENCE", validOidc().getAudience());
            environment.setProperty("TANTOR_KEYCLOAK_CLIENT_ID", audience);
        } catch (ReflectiveOperationException error) {
            throw new AssertionError(error);
        }
    }

    private ArtifactRepositoryProperties artifacts(String internalUrl, String publicUrl) {
        ArtifactRepositoryProperties properties = new ArtifactRepositoryProperties();
        properties.setInternalUrl(URI.create(internalUrl));
        properties.setPublicUrl(URI.create(publicUrl));
        return properties;
    }

    private ArtifactRepositoryProperties validArtifacts() {
        return artifacts("http://tantor-artifact-repository:8081", "https://tantor.corp.internal");
    }

    private MonitoringProperties validMonitoring() {
        MonitoringProperties properties = new MonitoringProperties();
        properties.setMode("direct");
        properties.setPrometheusUrl(URI.create("http://prometheus:9090"));
        return properties;
    }

    private OidcProperties oidc(String issuer, String audience) {
        OidcProperties properties = new OidcProperties();
        properties.setIssuerUri(URI.create(issuer));
        properties.setAudience(audience);
        return properties;
    }

    private OidcProperties validOidc() {
        return oidc("https://identity.corp.internal/realms/tantor", "tantor-ui");
    }
}
