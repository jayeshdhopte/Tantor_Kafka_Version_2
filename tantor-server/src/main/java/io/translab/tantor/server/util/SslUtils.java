package io.translab.tantor.server.util;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.Base64;

public class SslUtils {

    /**
     * Builds an SSL context from the base64 certificate representation used by
     * data-service connection requests and persisted connections.
     *
     * PKCS12_JKS is retained as a legacy request-header alias. Persisted values
     * are normalized to PKCS12 by DataServiceConnectionService.
     */
    public static SSLContext createSslContext(String certificateType,
                                              String base64Certificate,
                                              String password) throws Exception {
        if ("PKCS12".equalsIgnoreCase(certificateType)
                || "PKCS12_JKS".equalsIgnoreCase(certificateType)) {
            return createSslContextFromPkcs12(base64Certificate, password);
        }
        if (certificateType == null
                || certificateType.isBlank()
                || "PEM".equalsIgnoreCase(certificateType)) {
            String pem = new String(Base64.getDecoder().decode(base64Certificate), StandardCharsets.UTF_8);
            return createSslContextFromPem(pem);
        }
        throw new IllegalArgumentException("Unsupported certificate type: " + certificateType);
    }

    public static SSLContext createSslContextFromPem(String pemCertificate) throws Exception {
        if (pemCertificate == null || pemCertificate.isBlank()) {
            return SSLContext.getDefault();
        }

        // CertificateFactory can parse a full PEM file directly (headers + base64 body)
        byte[] pemBytes = pemCertificate.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null); // Initialize empty keystore

        // Parse all certificates in the PEM (supports certificate chains)
        int certIndex = 0;
        java.io.InputStream pemStream = new ByteArrayInputStream(pemBytes);
        while (pemStream.available() > 0) {
            try {
                java.security.cert.Certificate cert = cf.generateCertificate(pemStream);
                trustStore.setCertificateEntry("custom-cert-" + certIndex, cert);
                certIndex++;
            } catch (Exception e) {
                // Reached end of parseable certs
                break;
            }
        }

        if (certIndex == 0) {
            throw new IllegalArgumentException("No valid X.509 certificates found in the provided PEM data.");
        }

        return createSslContextFromTrustStore(trustStore);
    }

    public static SSLContext createSslContextFromPkcs12(String base64Pkcs12, String password) throws Exception {
        if (base64Pkcs12 == null || base64Pkcs12.isBlank()) {
            return SSLContext.getDefault();
        }

        byte[] certBytes = Base64.getDecoder().decode(base64Pkcs12);
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        char[] passChars = password != null ? password.toCharArray() : new char[0];
        trustStore.load(new ByteArrayInputStream(certBytes), passChars);

        return createSslContextFromTrustStore(trustStore);
    }

    private static SSLContext createSslContextFromTrustStore(KeyStore trustStore) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), new java.security.SecureRandom());
        
        return sslContext;
    }
}
