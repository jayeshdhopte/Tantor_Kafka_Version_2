package io.translab.tantor.artifact.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@Validated
@ConfigurationProperties(prefix = "tantor.cors")
public class CorsProperties {
    @NotEmpty
    private List<String> allowedOrigins = new ArrayList<>();

    public List<String> getAllowedOrigins() { return allowedOrigins; }
    public void setAllowedOrigins(List<String> allowedOrigins) { this.allowedOrigins = allowedOrigins; }

    @AssertTrue(message = "tantor.cors.allowed-origins entries must be absolute HTTP(S) origins without paths or credentials")
    public boolean isValid() {
        return allowedOrigins != null && !allowedOrigins.isEmpty() && allowedOrigins.stream().allMatch(value -> {
            try {
                URI uri = URI.create(value.trim());
                return uri.isAbsolute() && uri.getHost() != null && uri.getUserInfo() == null
                        && (uri.getPath() == null || uri.getPath().isEmpty())
                        && uri.getQuery() == null && uri.getFragment() == null
                        && ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()));
            } catch (RuntimeException exception) {
                return false;
            }
        });
    }
}
