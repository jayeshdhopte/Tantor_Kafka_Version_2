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

  @Value("${tantor.external-add-node.separate-execution-enabled:false}")
  private boolean separateEnabled;

  @Value("${tantor.external-add-node.phase5-accepted:false}")
  private boolean controllerAccepted;

  public boolean enabled(boolean separate) {
    return enabled() && (!separate || separateEnabled && controllerAccepted);
  }

  public void require(boolean separate) {
    if (!enabled(separate))
      throw new IllegalStateException(
          "External selected-layout release is held pending live acceptance and explicit"
              + " approval.");
  }

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
