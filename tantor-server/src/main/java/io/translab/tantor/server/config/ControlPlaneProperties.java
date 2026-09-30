package io.translab.tantor.server.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;

@Validated
@ConfigurationProperties(prefix = "tantor.control-plane")
public class ControlPlaneProperties {
    @NotNull
    private URI publicUrl;

    public URI getPublicUrl() { return publicUrl; }
    public void setPublicUrl(URI value) { this.publicUrl = value; }

    @AssertTrue(message = "tantor.control-plane.public-url must be an absolute HTTPS URL without credentials")
    public boolean isValid() {
        return publicUrl != null && publicUrl.isAbsolute() && publicUrl.getHost() != null
                && publicUrl.getUserInfo() == null && publicUrl.getQuery() == null && publicUrl.getFragment() == null
                && "https".equalsIgnoreCase(publicUrl.getScheme());
    }
}
