package io.translab.tantor.server.service;

import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** A conflicting operation is evidence, not a rollback of the saved preview. */
@Service
@RequiredArgsConstructor
public class ExternalAddNodeConflictObservation {
  private final JobService jobs;

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void verify(UUID clusterId) {
    jobs.assertClusterMutable(clusterId);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void verifyOwned(UUID clusterId, UUID jobId) {
    jobs.assertExternalBrokerAdditionOwnsCluster(clusterId, jobId);
  }
}
