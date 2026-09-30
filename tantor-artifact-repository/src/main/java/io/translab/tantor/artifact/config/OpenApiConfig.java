package io.translab.tantor.artifact.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI tantorArtifactOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Tantor Artifact Repository API")
                .version("1.0.0")
                .description("Upload, download, version, verify and bundle deployment artifacts "
                        + "for the Tantor Kafka management platform.")
                .license(new License().name("Proprietary - Translab Technologies")));
    }
}
