package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.security.TruststoreStorageService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaAdminServiceSecurityTest {

    @TempDir
    Path tempDir;

    @Test
    void buildsExpectedPropertiesForAllExternalSecurityModes() throws Exception {
        EncryptionService encryptionService = mock(EncryptionService.class);
        when(encryptionService.decrypt("encrypted-password")).thenReturn("broker-password");
        when(encryptionService.decrypt("encrypted-truststore-password")).thenReturn("changeit");

        KafkaAdminService service = new KafkaAdminService(
                mock(ClusterRepository.class),
                mock(ExternalClusterRepository.class),
                mock(HostRepository.class),
                new ObjectMapper(),
                encryptionService,
                mock(TruststoreStorageService.class));

        Properties plaintext = securityProperties(service, cluster("PLAINTEXT", null));
        assertThat(plaintext).containsEntry("security.protocol", "PLAINTEXT");
        assertThat(plaintext).doesNotContainKey("sasl.mechanism");
        assertThat(plaintext).doesNotContainKey("ssl.truststore.location");

        Path truststore = Files.writeString(tempDir.resolve("cluster.p12"), "test-store");
        Properties ssl = securityProperties(service, cluster("SSL", truststore));
        assertThat(ssl).containsEntry("security.protocol", "SSL");
        assertThat(ssl).containsEntry("ssl.truststore.location", truststore.toString());
        assertThat(ssl).containsEntry("ssl.truststore.password", "changeit");

        Properties saslPlaintext = securityProperties(service, cluster("SASL_PLAINTEXT", null));
        assertThat(saslPlaintext).containsEntry("security.protocol", "SASL_PLAINTEXT");
        assertThat(saslPlaintext).containsEntry("sasl.mechanism", "SCRAM-SHA-512");
        assertThat(String.valueOf(saslPlaintext.get("sasl.jaas.config")))
                .contains("ScramLoginModule", "username=\"admin\"", "password=\"broker-password\"");

        Properties saslSsl = securityProperties(service, cluster("SASL_SSL", truststore));
        assertThat(saslSsl).containsEntry("security.protocol", "SASL_SSL");
        assertThat(saslSsl).containsEntry("sasl.mechanism", "SCRAM-SHA-512");
        assertThat(saslSsl).containsEntry("ssl.truststore.location", truststore.toString());
        assertThat(String.valueOf(saslSsl.get("sasl.jaas.config")))
                .contains("ScramLoginModule", "username=\"admin\"", "password=\"broker-password\"");
    }

    @Test
    void classifiesKafkaAndPlatformManagedTopicsAsInternal() {
        KafkaAdminService service = new KafkaAdminService(
                mock(ClusterRepository.class),
                mock(ExternalClusterRepository.class),
                mock(HostRepository.class),
                new ObjectMapper(),
                mock(EncryptionService.class),
                mock(TruststoreStorageService.class));

        Set<String> internalTopics = Set.of(
                "__consumer_offsets", "_schemas", "connect-configs", "connect-offsets", "connect-status");

        internalTopics.forEach(topic -> assertThat((Boolean) ReflectionTestUtils.invokeMethod(
                service, "isManagedInternalTopic", topic)).isTrue());
        assertThat((Boolean) ReflectionTestUtils.invokeMethod(
                service, "isManagedInternalTopic", "customer-orders")).isFalse();
    }

    @Test
    void detectsCoordinationModeFromBrokerConfiguration() {
        KafkaAdminService service = service();

        Config zooKeeper = new Config(List.of(
                new ConfigEntry("zookeeper.connect", "192.168.3.150:9097")));
        Config kraft = new Config(List.of(
                new ConfigEntry("process.roles", "broker,controller")));
        Config unknown = new Config(List.of());

        assertThat((String) ReflectionTestUtils.invokeMethod(
                service, "configuredKafkaMode", zooKeeper)).isEqualTo("ZooKeeper");
        assertThat((String) ReflectionTestUtils.invokeMethod(
                service, "configuredKafkaMode", kraft)).isEqualTo("KRaft");
        assertThat((String) ReflectionTestUtils.invokeMethod(
                service, "configuredKafkaMode", unknown)).isNull();
    }

    @Test
    void treatsUnsupportedMetadataQuorumApiAsZooKeeperEvidence() {
        KafkaAdminService service = service();
        ExecutionException wrapped = new ExecutionException(
                new UnsupportedVersionException("metadata quorum API unsupported"));

        assertThat((Boolean) ReflectionTestUtils.invokeMethod(
                service, "isUnsupportedMetadataQuorum", wrapped)).isTrue();
        assertThat((Boolean) ReflectionTestUtils.invokeMethod(
                service, "isUnsupportedMetadataQuorum", new IllegalStateException("timeout"))).isFalse();
    }

    @Test
    void treatsZooKeeperActiveControllerAsBrokerRoleNotKraftControllerRole() {
        KafkaAdminService service = service();
        Map<String, Object> node = new HashMap<>();
        node.put("isBroker", true);
        node.put("isController", true);

        service.normalizeNodeRolesForMode(List.of(node), "ZooKeeper");

        assertThat(node).containsEntry("isBroker", true)
                .containsEntry("isController", false);
    }

    @Test
    void preservesKraftBrokerControllerRole() {
        KafkaAdminService service = service();
        Map<String, Object> node = new HashMap<>();
        node.put("isBroker", true);
        node.put("isController", true);

        service.normalizeNodeRolesForMode(List.of(node), "KRaft");

        assertThat(node).containsEntry("isBroker", true)
                .containsEntry("isController", true);
    }

    private Properties securityProperties(KafkaAdminService service, ExternalCluster cluster) {
        Properties properties = new Properties();
        ReflectionTestUtils.invokeMethod(service, "applySecurityProperties", properties, cluster, true);
        return properties;
    }

    private KafkaAdminService service() {
        return new KafkaAdminService(
                mock(ClusterRepository.class),
                mock(ExternalClusterRepository.class),
                mock(HostRepository.class),
                new ObjectMapper(),
                mock(EncryptionService.class),
                mock(TruststoreStorageService.class));
    }

    private ExternalCluster cluster(String protocol, Path truststore) {
        ExternalCluster cluster = new ExternalCluster();
        cluster.setSecurityProtocol(protocol);
        if (protocol.startsWith("SASL_")) {
            cluster.setSaslMechanism("SCRAM-SHA-512");
            cluster.setSaslUsername("admin");
            cluster.setSaslPasswordEncrypted("encrypted-password");
        }
        if (protocol.endsWith("SSL")) {
            cluster.setTruststorePath(truststore.toString());
            cluster.setTruststoreType("PKCS12");
            cluster.setTruststoreContentEncrypted("encrypted-content");
            cluster.setTruststorePasswordEncrypted("encrypted-truststore-password");
        }
        return cluster;
    }
}
