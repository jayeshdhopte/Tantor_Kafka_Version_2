package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.dto.*;
import io.translab.tantor.server.repository.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExternalConnectionSettingsServiceTest {
    private final ExternalClusterRepository repo=mock(ExternalClusterRepository.class);
    private final ClusterRepository mirrors=mock(ClusterRepository.class);
    private final KafkaAdminService kafka=mock(KafkaAdminService.class);
    private final DataServiceConnectionService connections=mock(DataServiceConnectionService.class);
    private final ObjectMapper mapper=new ObjectMapper();
    private final ExternalConnectionSettingsService service=spy(new ExternalConnectionSettingsService(repo,mirrors,kafka,connections,mapper,mock(AuditService.class)));
    private ExternalCluster cluster(UUID id){var c=new ExternalCluster();c.setId(id);c.setStatus("SUCCESS");c.setBootstrapServers("old:9092");c.setKafkaClusterId("physical-cluster");when(repo.findById(id)).thenReturn(Optional.of(c));return c;}

    @Test void normalizesMultipleBootstrapServersAndIpv6WithoutAllowingUrlsOrCredentials(){
        assertThat(ExternalConnectionSettingsService.normalizeBootstrap(" a:9092, [::1]:9093,a:9092 ")).isEqualTo("a:9092,[::1]:9093");
        for(String bad:List.of("http://a:9092","user:password@a:9092","a:0","a:65536","a:9092,","a:9092/path","a:9092?x=1"))
            assertThatThrownBy(()->ExternalConnectionSettingsService.normalizeBootstrap(bad)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void savesConnectionsUsingExistingServiceAndRoundTripsMetricsAndMasks() {
        UUID id=UUID.randomUUID();var c=cluster(id);var request=new ExternalConnectionSettings.Request();
        var metrics=new ExternalConnectionSettings.Metrics();metrics.setBrokerPort(17071);request.getSettings().setMetrics(metrics);
        var mask=new ExternalConnectionSettings.Mask();mask.setFields(List.of("password"));request.getSettings().setMasking(List.of(mask));
        var connection=new SaveConnectionRequest();connection.setConnectionName("Registry");connection.setHost("registry");connection.setProtocol("https");connection.setPort(8081);request.setSchemaRegistry(List.of(connection));
        var response=service.save(id,request);
        assertThat(c.getConnectionSettings()).contains("17071","password");
        assertThat(((ExternalConnectionSettings)response.get("settings")).getMasking()).hasSize(1);
        verify(connections).saveConnection(eq(id),eq("SCHEMA_REGISTRY"),same(connection),any());
        verify(kafka,never()).refreshAdminClient(any());
    }
    @Test void refusesBootstrapServersForAnotherPhysicalClusterBeforeSavingAnything(){
        UUID id=UUID.randomUUID();var c=cluster(id);var request=new ExternalConnectionSettings.Request();request.setBootstrapServers("other:9092");
        doReturn("different-cluster").when(service).probe(id,"other:9092");
        assertThatThrownBy(()->service.save(id,request)).hasMessageContaining("different Kafka cluster");
        assertThat(c.getBootstrapServers()).isEqualTo("old:9092");verify(repo,never()).save(any());verifyNoInteractions(connections);
    }
    @Test void verifiedBootstrapUpdatesBothRepresentationsAndInvalidatesAdminCache(){
        UUID id=UUID.randomUUID();var c=cluster(id);var mirror=new Cluster();when(mirrors.findById(id)).thenReturn(Optional.of(mirror));
        var request=new ExternalConnectionSettings.Request();request.setBootstrapServers("new:9092,new2:9092");
        doReturn("physical-cluster").when(service).probe(id,request.getBootstrapServers());service.save(id,request);
        assertThat(c.getBootstrapServers()).isEqualTo(request.getBootstrapServers());assertThat(mirror.getBootstrapServers()).isEqualTo(request.getBootstrapServers());verify(kafka).refreshAdminClient(id);
    }
    @Test void rejectsInvalidMasksAndMetricsBeforeDatabaseWrites(){
        var request=new ExternalConnectionSettings.Request();var mask=new ExternalConnectionSettings.Mask();mask.setFields(List.of("secret"));mask.setFieldsPattern("[");request.getSettings().setMasking(List.of(mask));
        assertThatThrownBy(()->service.validate(request)).hasMessageContaining("Invalid masking pattern");
        request.getSettings().setMasking(List.of());var metrics=new ExternalConnectionSettings.Metrics();metrics.setBrokerPort(0);request.getSettings().setMetrics(metrics);
        assertThatThrownBy(()->service.validate(request)).hasMessageContaining("Ports");verifyNoInteractions(repo,connections);
    }
}
