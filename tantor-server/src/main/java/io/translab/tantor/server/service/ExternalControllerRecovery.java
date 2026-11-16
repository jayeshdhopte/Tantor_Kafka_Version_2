package io.translab.tantor.server.service;

/** Physical receipt + storage selects incomplete work; never infer failure from a timeout. */
public final class ExternalControllerRecovery {
  private ExternalControllerRecovery() {}

  public static String checkpoint(
      KafkaExpansionNativeEvidence.Observation proof, boolean claimedFormat) {
    var state = proof.state();
    String phase = proof.phase(), storage = state.path("storageState").asText();
    boolean active = state.path("serviceActive").asBoolean();
    if (!state.path("formatterObservationComplete").asBoolean()
        || state.path("formatterActive").asBoolean())
      throw new IllegalStateException(
          "Formatter may still be active; retain controller ownership.");
    if (!state.path("admissionObservationComplete").asBoolean()
        || state.path("admissionActive").asBoolean())
      throw new IllegalStateException(
          "A controller admission tool may still be active; inspect after its lease settles.");
    if (SetHolder.EARLY.contains(phase)) {
      if (claimedFormat
          || !"MISSING".equals(storage)
          || active
          || !state.path("storageEmpty").asBoolean())
        throw new IllegalStateException(
            "Missing/regressed journal or nonempty controller storage requires investigation.");
      return "ABSENT".equals(phase) ? "ACCEPTED" : "VALIDATING";
    }
    if (!state.path("mountsVerified").asBoolean())
      throw new IllegalStateException(
          "Controller storage/log mounts or runtime-user access are no longer verified.");
    if (!state.path("configOwned").asBoolean() || !state.path("unitOwned").asBoolean())
      throw new IllegalStateException("Controller configuration/unit ownership changed.");
    if ("PREPARED".equals(phase)) {
      if (!"MISSING".equals(storage) || active || !state.path("storageEmpty").asBoolean())
        throw new IllegalStateException("Prepared controller storage is no longer empty.");
      return "PREPARED";
    }
    if (!"MATCHING".equals(storage))
      throw new IllegalStateException(
          "Uncertain/partial format outcome; never repeat the controller formatter.");
    if ("FORMAT_INTENT".equals(phase) || "FORMATTED".equals(phase)) {
      if (active) throw new IllegalStateException("Unexpected active formatted controller.");
      return "FORMATTED";
    }
    if (java.util.Set.of("START_INTENT", "STARTED", "ADMISSION_INTENT", "ADMISSION_SENT")
        .contains(phase)) {
      if (active && !state.path("runtimeVerified").asBoolean())
        throw new IllegalStateException("Active controller runtime is not verified.");
      if (!active && java.util.Set.of("ADMISSION_INTENT", "ADMISSION_SENT").contains(phase))
        throw new IllegalStateException(
            "An admitted/possibly admitted controller stopped; inspect quorum before manual"
                + " recovery.");
      return active ? "OBSERVER_STARTED" : "FORMATTED";
    }
    throw new IllegalStateException("Unknown controller checkpoint.");
  }

  private static class SetHolder {
    static final java.util.Set<String> EARLY = java.util.Set.of("ABSENT", "CLAIMED");
  }
}
