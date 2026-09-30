package io.translab.tantor.server.config;

import org.junit.jupiter.api.Test;

import javax.net.ssl.HttpsURLConnection;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MonitoringHttpClientConfigurationTest {

    @Test
    void insecureTlsOverrideIsRejectedWithoutMutatingJvmDefaults() {
        var socketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
        var hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
        MonitoringHttpClientConfiguration configuration = configured("direct", "http://prometheus:9090", "https://grafana.example");
        configurationProperties(configuration).setGrafanaSkipTlsValidation(true);

        assertThatThrownBy(configuration::rejectInsecureTls)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("forbidden");
        assertThat(HttpsURLConnection.getDefaultSSLSocketFactory()).isSameAs(socketFactory);
        assertThat(HttpsURLConnection.getDefaultHostnameVerifier()).isSameAs(hostnameVerifier);
    }

    @Test
    void grafanaProxyRequiresHttps() {
        MonitoringHttpClientConfiguration configuration = configured("grafana-proxy", "http://prometheus:9090", "http://grafana.example");
        assertThatThrownBy(configuration::rejectInsecureTls)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Grafana URL must use HTTPS");
    }

    @Test
    void externalPlaintextPrometheusIsRejected() {
        MonitoringHttpClientConfiguration configuration = configured("direct", "http://prometheus.example.com:9090", "https://grafana.example");
        assertThatThrownBy(configuration::rejectInsecureTls)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Plain HTTP Prometheus");
    }

    @Test
    void missingConfiguredCaFailsClosed() {
        assertThatThrownBy(() -> MonitoringHttpClientConfiguration.sslContextForCa(Path.of("missing-monitoring-ca.pem")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
    }

    private static MonitoringHttpClientConfiguration configured(String mode, String prometheus, String grafana) {
        MonitoringProperties properties = new MonitoringProperties();
        properties.setMode(mode);
        properties.setPrometheusUrl(java.net.URI.create(prometheus));
        properties.setGrafanaUrl(java.net.URI.create(grafana));
        return new MonitoringHttpClientConfiguration(properties);
    }

    private static MonitoringProperties configurationProperties(MonitoringHttpClientConfiguration configuration) {
        return (MonitoringProperties) org.springframework.test.util.ReflectionTestUtils.getField(configuration, "properties");
    }
}
