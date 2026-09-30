package io.translab.tantor.server.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "tantor.kafka-deployment")
public class KafkaDeploymentProperties {
    private String runtimeUser = "";
    private String runtimeGroup = "";
    private String javaHome = "";
    @Min(1024) private int limitNofile = 100000;
    @NotBlank @Pattern(regexp = "PLAINTEXT|SSL|SASL_SSL") private String securityMode;
    @NotBlank @Pattern(regexp = "[A-Za-z0-9_.@-]+") private String servicePrefix = "tantor-kafka-";

    public String getRuntimeUser() { return runtimeUser; }
    public void setRuntimeUser(String value) { this.runtimeUser = value; }
    public String getRuntimeGroup() { return runtimeGroup; }
    public void setRuntimeGroup(String value) { this.runtimeGroup = value; }
    public String getJavaHome() { return javaHome; }
    public void setJavaHome(String value) { this.javaHome = value; }
    public int getLimitNofile() { return limitNofile; }
    public void setLimitNofile(int value) { this.limitNofile = value; }
    public String getSecurityMode() { return securityMode; }
    public void setSecurityMode(String value) { this.securityMode = value; }
    public String getServicePrefix() { return servicePrefix; }
    public void setServicePrefix(String value) { this.servicePrefix = value; }
}
