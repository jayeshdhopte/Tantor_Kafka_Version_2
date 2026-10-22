package io.translab.tantor.server.service;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DeploymentInstallPathsTest {
 @Test void namespacesNewInstallationsWithoutDuplicatingTantorOrKafka() {
  assertThat(DeploymentInstallPaths.brandedKafkaBase(null)).isEqualTo("/opt/tantor");
  assertThat(DeploymentInstallPaths.brandedKafkaBase("/client-approved-mount")).isEqualTo("/client-approved-mount/tantor");
  assertThat(DeploymentInstallPaths.brandedKafkaBase("/data/tantor/kafka/")).isEqualTo("/data/tantor");
  assertThat(DeploymentInstallPaths.brandedKafkaBase("/data/tantor/kafka_2.13-4.1.0")).isEqualTo("/data/tantor");
  assertThat(DeploymentInstallPaths.brandedKafkaBase("/data/tantor")).isEqualTo("/data/tantor");
 }
 @Test void rejectsRelativeAndParentTraversalPaths() {
  assertThatThrownBy(()->DeploymentInstallPaths.brandedKafkaBase("relative")).isInstanceOf(IllegalArgumentException.class);
  assertThatThrownBy(()->DeploymentInstallPaths.brandedKafkaBase("/data/../etc")).isInstanceOf(IllegalArgumentException.class);
  assertThatThrownBy(()->DeploymentInstallPaths.brandedKafkaBase("/data\r\ninvalid")).isInstanceOf(IllegalArgumentException.class);
 }
}
