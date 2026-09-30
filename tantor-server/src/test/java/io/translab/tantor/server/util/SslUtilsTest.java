package io.translab.tantor.server.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.security.KeyStore;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class SslUtilsTest {

    @Test
    void loadsCanonicalPkcs12ConnectionCertificateAsTruststore() throws Exception {
        String password = "truststore-password"; // gitleaks:allow -- deterministic test fixture
        String encodedTruststore = emptyPkcs12(password);

        var sslContext = SslUtils.createSslContext("PKCS12", encodedTruststore, password);

        assertThat(sslContext).isNotNull();
        assertThat(sslContext.getProtocol()).isEqualTo("TLS");
    }

    @Test
    void continuesToAcceptLegacyPkcs12RequestHeaderAlias() throws Exception {
        String password = "truststore-password"; // gitleaks:allow -- deterministic test fixture
        String encodedTruststore = emptyPkcs12(password);

        var sslContext = SslUtils.createSslContext("PKCS12_JKS", encodedTruststore, password);

        assertThat(sslContext).isNotNull();
    }

    private String emptyPkcs12(String password) throws Exception {
        KeyStore truststore = KeyStore.getInstance("PKCS12");
        truststore.load(null, null);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        truststore.store(output, password.toCharArray());
        return Base64.getEncoder().encodeToString(output.toByteArray());
    }
}
