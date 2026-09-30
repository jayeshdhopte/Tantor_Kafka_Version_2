package io.translab.tantor.server.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "tantor.runtime")
public class RuntimeEnvironmentProperties {
    public enum Environment { DEVELOPMENT, SIT, UAT, PRODUCTION }

    @NotNull
    private Environment environment;

    public Environment getEnvironment() { return environment; }
    public void setEnvironment(Environment environment) { this.environment = environment; }
    public boolean isProductionLike() { return environment != Environment.DEVELOPMENT; }
}
