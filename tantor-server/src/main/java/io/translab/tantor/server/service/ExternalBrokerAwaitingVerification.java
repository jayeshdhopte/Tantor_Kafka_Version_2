package io.translab.tantor.server.service;

/** A started process must never be labelled a verified addition. */
public final class ExternalBrokerAwaitingVerification extends RuntimeException {
  public ExternalBrokerAwaitingVerification() {
    super(
        "Candidate broker started; Verification and inventory publication are pending. Reservations"
            + " are retained.");
  }
}
