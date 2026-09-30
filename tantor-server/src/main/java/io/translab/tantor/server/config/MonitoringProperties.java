package io.translab.tantor.server.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;

@Validated
@ConfigurationProperties(prefix = "tantor.monitoring")
public class MonitoringProperties {
    @Pattern(regexp = "direct|grafana-proxy", flags = Pattern.Flag.CASE_INSENSITIVE)
    private String mode;
    private URI prometheusUrl;
    private URI grafanaUrl;
    private String grafanaDatasourceUid = "";
    private String grafanaUsername = "";
    private String grafanaPassword = "";
    private boolean grafanaSkipTlsValidation;
    private String tlsCaFile = "";
    private String exporterHost = "";
    @Min(1) @Max(65535) private int kafkaExporterPortBase = 9308;
    @Min(1) @Max(65535) private int jmxExporterPort = 7071;
    @Min(1) @Max(65535) private int controllerJmxExporterPort = 7072;

    @AssertTrue(message = "monitoring mode requires a valid endpoint and grafana-proxy requires HTTPS plus datasource UID")
    public boolean isModeConfigurationValid() {
        if (mode == null) return false;
        if ("grafana-proxy".equalsIgnoreCase(mode)) {
            return validAbsolute(grafanaUrl, true) && !grafanaDatasourceUid.isBlank();
        }
        return "direct".equalsIgnoreCase(mode) && validAbsolute(prometheusUrl, false);
    }

    private boolean validAbsolute(URI uri, boolean httpsOnly) {
        if (uri == null || !uri.isAbsolute() || uri.getHost() == null || uri.getUserInfo() != null) return false;
        return httpsOnly ? "https".equalsIgnoreCase(uri.getScheme())
                : ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()));
    }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }
    public URI getPrometheusUrl() { return prometheusUrl; }
    public void setPrometheusUrl(URI value) { this.prometheusUrl = value; }
    public URI getGrafanaUrl() { return grafanaUrl; }
    public void setGrafanaUrl(URI value) { this.grafanaUrl = value; }
    public String getGrafanaDatasourceUid() { return grafanaDatasourceUid; }
    public void setGrafanaDatasourceUid(String value) { this.grafanaDatasourceUid = value; }
    public String getGrafanaUsername() { return grafanaUsername; }
    public void setGrafanaUsername(String value) { this.grafanaUsername = value; }
    public String getGrafanaPassword() { return grafanaPassword; }
    public void setGrafanaPassword(String value) { this.grafanaPassword = value; }
    public boolean isGrafanaSkipTlsValidation() { return grafanaSkipTlsValidation; }
    public void setGrafanaSkipTlsValidation(boolean value) { this.grafanaSkipTlsValidation = value; }
    public String getTlsCaFile() { return tlsCaFile; }
    public void setTlsCaFile(String value) { this.tlsCaFile = value; }
    public String getExporterHost() { return exporterHost; }
    public void setExporterHost(String value) { this.exporterHost = value; }
    public int getKafkaExporterPortBase() { return kafkaExporterPortBase; }
    public void setKafkaExporterPortBase(int value) { this.kafkaExporterPortBase = value; }
    public int getJmxExporterPort() { return jmxExporterPort; }
    public void setJmxExporterPort(int value) { this.jmxExporterPort = value; }
    public int getControllerJmxExporterPort() { return controllerJmxExporterPort; }
    public void setControllerJmxExporterPort(int value) { this.controllerJmxExporterPort = value; }
}
