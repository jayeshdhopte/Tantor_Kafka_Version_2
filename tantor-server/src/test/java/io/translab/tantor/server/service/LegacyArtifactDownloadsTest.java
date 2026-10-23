package io.translab.tantor.server.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Task;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
class LegacyArtifactDownloadsTest {
 @Test void snapshotsOnlyKafkaTaskArchivesAndOriginalDirectories() throws Exception {
  var kafka=task("INSTALL_KAFKA","{\"artifact_load_dir\":\"/client/artifacts\"}");var fallback=task("INSTALL_KAFKA","{}");var alias=task("INSTALL_KAFKA","{\"artifacts_dir\":\"/srv/artifacts\"}");var parcel=task("DISTRIBUTE_PARCEL","{}");
  var result=DeploymentService.legacyArtifactDownloads(List.of(kafka,fallback,alias,parcel),new ObjectMapper());
  assertThat(result).containsExactly(Map.of("taskId",kafka.getId().toString(),"artifactRoot","/client/artifacts"),Map.of("taskId",fallback.getId().toString(),"artifactRoot",""),Map.of("taskId",alias.getId().toString(),"artifactRoot","/srv/artifacts"));
 }
 private Task task(String command,String parameters){var task=new Task();task.setId(UUID.randomUUID());task.setCommand(command);task.setParameters(parameters);return task;}
}
