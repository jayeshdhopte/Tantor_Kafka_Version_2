package io.translab.tantor.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties(prefix = "tantor.security.oidc")
public class OidcProperties {
    private URI issuerUri;
    private String audience = "";

    public URI getIssuerUri() { return issuerUri; }
    public void setIssuerUri(URI issuerUri) { this.issuerUri = issuerUri; }
    public String getAudience() { return audience; }
    public void setAudience(String audience) { this.audience = audience; }
}
