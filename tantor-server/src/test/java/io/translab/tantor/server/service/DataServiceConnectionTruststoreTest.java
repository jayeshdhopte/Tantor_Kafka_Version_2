package io.translab.tantor.server.service;

import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.domain.DataServiceConnection;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.dto.ConnectionResponse;
import io.translab.tantor.server.dto.SaveConnectionRequest;
import io.translab.tantor.server.repository.DataServiceConnectionRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.security.EncryptionService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataServiceConnectionTruststoreTest {

    @ParameterizedTest
    @ValueSource(strings = {"SCHEMA_REGISTRY", "KAFKA_CONNECT"})
    void editingWithoutNewFileRetainsTruststoreAndPassword(String serviceType) throws Exception {
        DataServiceConnectionRepository repository = mock(DataServiceConnectionRepository.class);
        ExternalClusterRepository externalClusters = mock(ExternalClusterRepository.class);
        EncryptionService encryption = mock(EncryptionService.class);
        DataServiceConnectionService service = service(repository, externalClusters, encryption);
        UUID clusterId = UUID.randomUUID();
        String certificateData = emptyTruststore();
        DataServiceConnection saved = new DataServiceConnection();
        saved.setId(UUID.randomUUID());
        saved.setClusterId(clusterId);
        saved.setServiceType(serviceType);
        saved.setCertificateType("PKCS12");
        saved.setCertificateData(certificateData);
        saved.setCertificateFileName("original.p12");
        saved.setTruststorePasswordEncrypted("encrypted-password");
        when(encryption.decrypt("encrypted-password")).thenReturn("secret");

        when(repository.countByClusterIdAndServiceTypeAndIsActiveTrue(clusterId, serviceType)).thenReturn(1L);
        when(repository.findByIdAndClusterIdAndServiceTypeAndIsActiveTrue(saved.getId(), clusterId, serviceType))
                .thenReturn(Optional.of(saved));
        when(repository.save(any(DataServiceConnection.class))).thenAnswer(call -> call.getArgument(0));

        SaveConnectionRequest edit = request(saved.getId());
        ConnectionResponse response = service.saveConnection(clusterId, serviceType, edit, "admin");

        assertThat(saved.getCertificateData()).isEqualTo(certificateData);
        assertThat(saved.getCertificateType()).isEqualTo("PKCS12");
        assertThat(saved.getCertificateFileName()).isEqualTo("original.p12");
        assertThat(saved.getTruststorePasswordEncrypted()).isEqualTo("encrypted-password");
        assertThat(response.isCertificateConfigured()).isTrue();
        assertThat(response.getCertificateFileName()).isEqualTo("original.p12");
    }

    @ParameterizedTest
    @ValueSource(strings = {"SCHEMA_REGISTRY", "KAFKA_CONNECT"})
    void newHttpsConnectionInheritsExternalClusterTruststore(String serviceType) throws Exception {
        DataServiceConnectionRepository repository = mock(DataServiceConnectionRepository.class);
        ExternalClusterRepository externalClusters = mock(ExternalClusterRepository.class);
        EncryptionService encryption = mock(EncryptionService.class);
        DataServiceConnectionService service = service(repository, externalClusters, encryption);
        UUID clusterId = UUID.randomUUID();
        String certificateData = emptyTruststore();
        ExternalCluster cluster = new ExternalCluster();
        cluster.setId(clusterId);
        cluster.setTruststoreType("PKCS12");
        cluster.setTruststoreContentEncrypted("encrypted-content");
        cluster.setTruststorePasswordEncrypted("encrypted-password");
        when(externalClusters.findById(clusterId)).thenReturn(Optional.of(cluster));
        when(encryption.decrypt("encrypted-content")).thenReturn(certificateData);
        when(encryption.decrypt("encrypted-password")).thenReturn("secret");
        when(repository.findByClusterIdAndServiceTypeAndIsActiveTrueOrderByConnectionNameAsc(clusterId, serviceType))
                .thenReturn(List.of());
        when(repository.save(any(DataServiceConnection.class))).thenAnswer(call -> {
            DataServiceConnection connection = call.getArgument(0);
            if (connection.getId() == null) connection.setId(UUID.randomUUID());
            return connection;
        });

        ConnectionResponse response = service.saveConnection(clusterId, serviceType, request(null), "admin");
        assertThat(response.isCertificateConfigured()).isTrue();
        assertThat(response.isTruststoreConfigured()).isTrue();
        assertThat(response.getCertificateType()).isEqualTo("PKCS12");
        assertThat(response.getCertificateFileName()).isEqualTo("Cluster truststore");
        assertThat(service.getClusterTruststoreInfo(clusterId).available()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"SCHEMA_REGISTRY", "KAFKA_CONNECT"})
    void replacementFileUpdatesDisplayNameAndContent(String serviceType) throws Exception {
        DataServiceConnectionRepository repository = mock(DataServiceConnectionRepository.class);
        EncryptionService encryption = mock(EncryptionService.class);
        DataServiceConnectionService service = service(repository, mock(ExternalClusterRepository.class), encryption);
        UUID clusterId = UUID.randomUUID();
        String certificateData = emptyTruststore();
        DataServiceConnection saved = new DataServiceConnection();
        saved.setId(UUID.randomUUID());
        saved.setCertificateData("old-content");
        saved.setCertificateFileName("old.p12");
        saved.setTruststorePasswordEncrypted("encrypted-password");
        when(encryption.decrypt("encrypted-password")).thenReturn("secret");
        when(repository.countByClusterIdAndServiceTypeAndIsActiveTrue(clusterId, serviceType)).thenReturn(1L);
        when(repository.findByIdAndClusterIdAndServiceTypeAndIsActiveTrue(saved.getId(), clusterId, serviceType))
                .thenReturn(Optional.of(saved));
        when(repository.save(any(DataServiceConnection.class))).thenAnswer(call -> call.getArgument(0));

        SaveConnectionRequest edit = request(saved.getId());
        edit.setCertificateType("PKCS12");
        edit.setCertificateData(certificateData);
        edit.setCertificateFileName("C:\\uploads\\replacement.p12");
        ConnectionResponse response = service.saveConnection(clusterId, serviceType, edit, "admin");

        assertThat(saved.getCertificateData()).isEqualTo(certificateData);
        assertThat(response.getCertificateFileName()).isEqualTo("replacement.p12");
    }

    private String emptyTruststore() throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        store.store(output, "secret".toCharArray());
        return Base64.getEncoder().encodeToString(output.toByteArray());
    }

    private SaveConnectionRequest request(UUID id) {
        SaveConnectionRequest request = new SaveConnectionRequest();
        request.setId(id);
        request.setConnectionName("Default connection");
        request.setProtocol("https");
        request.setHost("127.0.0.1");
        request.setPort(1);
        return request;
    }

    private DataServiceConnectionService service(
            DataServiceConnectionRepository repository,
            ExternalClusterRepository externalClusters,
            EncryptionService encryption) {
        return new DataServiceConnectionService(repository, externalClusters, encryption, mock(AuditService.class));
    }
}
