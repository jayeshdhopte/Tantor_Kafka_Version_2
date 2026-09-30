package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.MonitoringProperties;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MetricsControllerConfigurationTest {
    @Test
    void endpointUsesConfiguredJmxExporterPort() {
        MonitoringProperties monitoring = new MonitoringProperties();
        monitoring.setJmxExporterPort(17071);
        MetricsController controller = new MetricsController(mock(ClusterRepository.class), mock(HostRepository.class),
                new ObjectMapper(), monitoring);

        assertThat(controller.metricsEndpoint("10.20.0.11")).isEqualTo("http://10.20.0.11:17071/metrics");
    }
}
