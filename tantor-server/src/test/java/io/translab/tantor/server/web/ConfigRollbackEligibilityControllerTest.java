package io.translab.tantor.server.web;

import io.translab.tantor.server.service.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.UUID;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ConfigRollbackEligibilityControllerTest {
    @Test void eligibilityAndRetryExposeTheSameStaleRecoveryReason() throws Exception {
        var service = mock(JobService.class);
        var controller = new JobController(service, mock(RestartNodeDisplayService.class), mock(ExternalDataServiceManager.class));
        var mvc = MockMvcBuilders.standaloneSetup(controller).build();
        UUID id = UUID.randomUUID(), newer = UUID.randomUUID();
        String reason = "A newer configuration change affects this job's recovery targets.";
        when(service.getConfigRollbackEligibility(id)).thenReturn(new ConfigRollbackPolicy.Eligibility(false, reason, newer));
        when(service.retryJob(id)).thenThrow(new ClusterOperationConflictException(reason));
        mvc.perform(get("/api/v1/ui/jobs/{id}/rollback-eligibility", id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.allowed").value(false)).andExpect(jsonPath("$.reason").value(reason))
                .andExpect(jsonPath("$.conflictingJobId").value(newer.toString()));
        mvc.perform(post("/api/v1/ui/jobs/{id}/retry", id)).andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value(reason));
    }
}
