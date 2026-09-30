package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.ArtifactRepositoryProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

@Service @RequiredArgsConstructor
public class ExternalServiceArtifacts {
 private final ArtifactRepositoryProperties properties;
 private final ObjectMapper mapper;
 public String verifiedChecksum(UUID id,String role) {
  try {
   var uri=URI.create(properties.getInternalUrl().toString().replaceAll("/+$","")+"/api/v1/artifacts/"+id);
   var client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
   var response=client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(20)).GET().build(),HttpResponse.BodyHandlers.ofString());
   if(response.statusCode()!=200) throw new IllegalArgumentException("Artifact metadata is unavailable.");
   var data=mapper.readTree(response.body());
   if(!"AVAILABLE".equals(data.path("status").asText())) throw new IllegalArgumentException("Select an available artifact.");
   if(!role.toUpperCase(Locale.ROOT).equals(data.path("serviceType").asText()) && !data.path("fileName").asText().startsWith("tantor-unified-kafka-")) throw new IllegalArgumentException("Artifact does not support this service.");
   String hash=data.path("sha256").asText();
   if(!hash.matches("[a-fA-F0-9]{64}")) throw new IllegalArgumentException("Artifact has no verified SHA-256.");
   return hash.toLowerCase(Locale.ROOT);
  } catch(InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Artifact lookup interrupted",e); }
  catch(java.io.IOException e) { throw new IllegalStateException("Artifact repository unavailable",e); }
 }
}
