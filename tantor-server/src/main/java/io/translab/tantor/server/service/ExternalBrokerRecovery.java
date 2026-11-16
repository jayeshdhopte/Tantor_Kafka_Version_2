package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.JsonNode;

/** Physical ownership/storage evidence chooses the next action, never task status alone. */
public final class ExternalBrokerRecovery {
  private ExternalBrokerRecovery() {}

  public static String checkpoint(JsonNode proof) {
    if (!proof.path("formatterObservationComplete").asBoolean()
        || proof.path("formatterActive").asBoolean())
      throw new IllegalStateException(
          "Formatter activity is unknown or still active; preserve the operation.");
    String phase = proof.path("phase").asText(), storage = proof.path("storageState").asText();
    boolean active = proof.path("serviceActive").asBoolean(),
        empty = proof.path("storageEmpty").asBoolean();
    if ("ABSENT".equals(phase) || "CLAIMED".equals(phase)) {
      if (!"MISSING".equals(storage) || !empty || active)
        throw new IllegalStateException(
            "Unowned or partial storage requires operator investigation.");
      return "ABSENT".equals(phase) ? "ACCEPTED" : "VALIDATING";
    }
    if (!proof.path("runtimeVerified").asBoolean())
      throw new IllegalStateException("Installed Kafka payload is not verified.");
    return switch (phase) {
      case "PREPARED" -> {
        if (!"MISSING".equals(storage) || !empty || active)
          throw new IllegalStateException("Prepared storage/process state changed.");
        yield "PREPARED";
      }
      case "FORMAT_INTENT", "FORMATTED" -> {
        if (!"MATCHING".equals(storage) || active)
          throw new IllegalStateException(
              "Unknown or partial format outcome; formatting cannot be repeated.");
        yield "FORMATTED";
      }
      case "START_INTENT", "STARTED" -> {
        if (!"MATCHING".equals(storage))
          throw new IllegalStateException("Formatted storage identity changed.");
        if (active && !proof.path("processVerified").asBoolean())
          throw new IllegalStateException(
              "Running broker process is not bound to its frozen runtime.");
        yield active ? "STARTED_PENDING_VERIFICATION" : "FORMATTED";
      }
      default -> throw new IllegalStateException("Unknown native ownership checkpoint.");
    };
  }

  public static int storageRank(String state) {
    return switch (state) {
      case "ACCEPTED", "VALIDATING" -> 0;
      case "PREPARED" -> 1;
      case "FORMATTED", "STARTED_PENDING_VERIFICATION", "VERIFYING", "PUBLISHING" -> 2;
      default -> -1;
    };
  }
}
