package io.translab.tantor.server.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;

class DeploymentProfileConfigurationContextTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ProfileConfiguration.class);

    @Test
    void sitUatAndProductionConfigurationContextsLoadWithExplicitValues() {
        assertProfileLoads("sit", "SIT");
        assertProfileLoads("uat", "UAT");
        assertProfileLoads("production", "PRODUCTION");
    }

    @Test
    void missingMonitoringEndpointStopsConfigurationContext() {
        runner.withPropertyValues(requiredProperties("production", "PRODUCTION"))
                .withPropertyValues("tantor.monitoring.prometheus-url=")
                .run(context -> assertThat(context).hasFailed());
    }

    private void assertProfileLoads(String profile, String environment) {
        runner.withPropertyValues(requiredProperties(profile, environment))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(RuntimeEnvironmentProperties.class).getEnvironment().name())
                            .isEqualTo(environment);
                });
    }

    private String[] requiredProperties(String profile, String environment) {
        return new String[] {
                "spring.profiles.active=" + profile,
                "tantor.runtime.environment=" + environment,
                "tantor.artifact-repo.internal-url=http://artifact-repository:8081",
                "tantor.artifact-repo.public-url=https://tantor.corp.internal",
                "tantor.control-plane.public-url=https://tantor.corp.internal",
                "tantor.monitoring.mode=direct",
                "tantor.monitoring.prometheus-url=http://monitoring:9090",
                "tantor.security.oidc.issuer-uri=https://identity.corp.internal/realms/tantor",
                "tantor.security.oidc.audience=tantor-ui",
                "tantor.kafka-deployment.security-mode=SSL"
        };
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({ RuntimeEnvironmentProperties.class, ArtifactRepositoryProperties.class,
            ControlPlaneProperties.class, MonitoringProperties.class, OidcProperties.class,
            KafkaDeploymentProperties.class })
    static class ProfileConfiguration {
        @Bean
        ConfigurationStartupValidator configurationStartupValidator(RuntimeEnvironmentProperties runtime,
                ArtifactRepositoryProperties artifacts, ControlPlaneProperties controlPlane,
                MonitoringProperties monitoring, OidcProperties oidc, KafkaDeploymentProperties kafka,
                Environment environment) {
            return new ConfigurationStartupValidator(runtime, artifacts, controlPlane, monitoring, oidc, kafka,
                    environment);
        }
    }
}
