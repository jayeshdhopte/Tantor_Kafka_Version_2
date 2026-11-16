package io.translab.tantor.server.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ExternalControllerRelease {
  private final ExternalBrokerAdditionPlanning broker;

  @Value("${tantor.external-add-node.controller-execution-enabled:false}")
  private boolean enabled;

  @Value("${tantor.external-add-node.phase4-accepted:false}")
  private boolean brokerAccepted;

  public boolean enabled() {
    return enabled && brokerAccepted && broker.released();
  }

  public void require() {
    if (!enabled())
      throw new IllegalStateException(
          "External controller execution is held until broker/readiness live acceptance and the"
              + " separate controller release gate are enabled.");
  }
}
