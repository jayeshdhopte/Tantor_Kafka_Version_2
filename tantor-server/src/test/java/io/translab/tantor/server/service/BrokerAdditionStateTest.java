package io.translab.tantor.server.service;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static io.translab.tantor.server.service.BrokerAdditionState.*;
class BrokerAdditionStateTest {
    @Test void publicationCannotSkipFormatStartAndVerification(){assertFalse(ACCEPTED.canAdvanceTo(SUCCEEDED));assertFalse(FORMATTING.canAdvanceTo(SUCCEEDED));assertFalse(STARTING.canAdvanceTo(SUCCEEDED));assertTrue(VERIFYING.canAdvanceTo(SUCCEEDED));}
    @Test void failuresRetainReservationsAndOnlySafeTerminalsReleaseThem(){for(var state:values())assertEquals(state!=SUCCEEDED&&state!=CANCELLED_SAFE,state.reserved());assertTrue(FORMATTING.canAdvanceTo(RECOVERY_REQUIRED));assertFalse(RECOVERY_REQUIRED.canAdvanceTo(SUCCEEDED));}
    @Test void terminalOperationsCannotRestart(){for(var state:values())if(state!=SUCCEEDED)assertFalse(SUCCEEDED.canAdvanceTo(state));for(var state:values())if(state!=CANCELLED_SAFE)assertFalse(CANCELLED_SAFE.canAdvanceTo(state));}
}
