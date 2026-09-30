package io.translab.tantor.server.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Collection;

@Configuration
public class MonitoringHttpClientConfiguration {
    private final MonitoringProperties properties;

    public MonitoringHttpClientConfiguration(MonitoringProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    void rejectInsecureTls() {
        if (properties.isGrafanaSkipTlsValidation()) {
            throw new IllegalStateException("TANTOR_GRAFANA_SKIP_TLS_VALIDATION is forbidden; configure TANTOR_MONITORING_TLS_CA_FILE instead");
        }
        if ("grafana-proxy".equalsIgnoreCase(properties.getMode())) {
            requireHttps("Grafana", value(properties.getGrafanaUrl()));
        } else {
            URI prometheus = parseAbsolute("Prometheus", value(properties.getPrometheusUrl()));
            if ("http".equalsIgnoreCase(prometheus.getScheme())
                    && !isPrivateServiceName(prometheus.getHost())) {
                throw new IllegalStateException("Plain HTTP Prometheus is allowed only for a loopback or single-label private service hostname");
            }
        }
    }

    private static String value(URI uri) {
        return uri == null ? "" : uri.toString();
    }

    private static void requireHttps(String name, String value) {
        URI uri = parseAbsolute(name, value);
        if (!"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalStateException(name + " URL must use HTTPS");
        }
    }

    private static URI parseAbsolute(String name, String value) {
        try {
            URI uri = URI.create(value == null ? "" : value.trim());
            if (uri.getHost() == null || uri.getScheme() == null) {
                throw new IllegalArgumentException();
            }
            return uri;
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException(name + " URL must be an absolute URL", exception);
        }
    }

    private static boolean isPrivateServiceName(String host) {
        return host != null && ("localhost".equalsIgnoreCase(host)
                || host.matches("127(?:\\.\\d{1,3}){3}")
                || (!host.contains(".") && !host.contains(":")));
    }

    @Bean
    MonitoringRestTemplate monitoringRestTemplate() throws Exception {
        SSLParameters parameters = new SSLParameters();
        parameters.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
        parameters.setEndpointIdentificationAlgorithm("HTTPS");

        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER)
                .sslParameters(parameters);
        if (properties.getTlsCaFile() != null && !properties.getTlsCaFile().isBlank()) {
            builder.sslContext(sslContextForCa(Path.of(properties.getTlsCaFile().trim())));
        }

        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(builder.build());
        requestFactory.setReadTimeout(Duration.ofSeconds(10));
        return new MonitoringRestTemplate(requestFactory);
    }

    static SSLContext sslContextForCa(Path caFile) throws Exception {
        if (!Files.isRegularFile(caFile)) {
            throw new IllegalStateException("Monitoring TLS CA file does not exist: " + caFile);
        }
        Collection<? extends Certificate> certificates;
        try (InputStream input = Files.newInputStream(caFile)) {
            certificates = CertificateFactory.getInstance("X.509").generateCertificates(input);
        }
        if (certificates.isEmpty()) {
            throw new IllegalStateException("Monitoring TLS CA file contains no certificates: " + caFile);
        }
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null);
        int index = 0;
        for (Certificate certificate : certificates) {
            trustStore.setCertificateEntry("monitoring-ca-" + index++, certificate);
        }
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trustManagerFactory.getTrustManagers(), null);
        return context;
    }

    public static final class MonitoringRestTemplate extends RestTemplate {
        MonitoringRestTemplate(JdkClientHttpRequestFactory requestFactory) {
            super(requestFactory);
        }
    }
}
