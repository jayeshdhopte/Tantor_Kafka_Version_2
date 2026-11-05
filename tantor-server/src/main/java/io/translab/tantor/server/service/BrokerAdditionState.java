package io.translab.tantor.server.service;

import java.util.Set;

/** Operation state is stronger than a generic FAILED/PARTIAL_SUCCESS job summary. */
public enum BrokerAdditionState {
    ACCEPTED, VALIDATING, PREPARING, PREPARED, FORMATTING, FORMATTED, STARTING,
    VERIFYING, RECOVERY_REQUIRED, SUCCEEDED, CANCELLED_SAFE;

    public boolean reserved() { return this != SUCCEEDED && this != CANCELLED_SAFE; }

    public boolean canAdvanceTo(BrokerAdditionState next) {
        if (this == next) return true;
        if (!reserved()) return false;
        if (next == RECOVERY_REQUIRED) return true;
        return switch (this) {
            case ACCEPTED -> next == VALIDATING || next == CANCELLED_SAFE;
            case VALIDATING -> next == PREPARING || next == CANCELLED_SAFE;
            case PREPARING -> next == PREPARED;
            case PREPARED -> next == FORMATTING;
            case FORMATTING -> next == FORMATTED;
            case FORMATTED -> next == STARTING;
            case STARTING -> next == VERIFYING;
            case VERIFYING -> next == SUCCEEDED;
            // Reconciliation, never a job retry flag, supplies the prior checkpoint.
            case RECOVERY_REQUIRED -> Set.of(VALIDATING, PREPARING, PREPARED, FORMATTED, VERIFYING, CANCELLED_SAFE).contains(next);
            default -> false;
        };
    }

    public void requireAdvanceTo(BrokerAdditionState next) {
        if (!canAdvanceTo(next)) throw new IllegalStateException("Unsafe broker transition: " + this + " -> " + next);
    }
}
