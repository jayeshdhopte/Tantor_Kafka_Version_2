package io.translab.tantor.server.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;

@Validated
@ConfigurationProperties(prefix = "tantor.artifact-repo")
public class ArtifactRepositoryProperties {
    @NotNull
    private URI internalUrl;
    @NotNull
    private URI publicUrl;
    private String jmxExporterArtifactId = "";

    public URI getInternalUrl() { return internalUrl; }
    public void setInternalUrl(URI value) { this.internalUrl = value; }
    public URI getPublicUrl() { return publicUrl; }
    public void setPublicUrl(URI value) { this.publicUrl = value; }
    public String getJmxExporterArtifactId() { return jmxExporterArtifactId; }
    public void setJmxExporterArtifactId(String value) { this.jmxExporterArtifactId = value; }

    @AssertTrue(message = "Artifact Repository internal/public URLs must be absolute HTTP(S) URLs without credentials")
    public boolean isValidUrls() {
        return valid(internalUrl) && valid(publicUrl);
    }

    private boolean valid(URI value) {
        return value != null && value.isAbsolute() && value.getHost() != null && value.getUserInfo() == null
                && value.getQuery() == null && value.getFragment() == null
                && ("http".equalsIgnoreCase(value.getScheme()) || "https".equalsIgnoreCase(value.getScheme()));
    }
}
