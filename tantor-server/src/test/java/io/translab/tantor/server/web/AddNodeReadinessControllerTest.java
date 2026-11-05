package io.translab.tantor.server.web;

import io.translab.tantor.server.service.AddNodeReadinessService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.UUID;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class AddNodeReadinessControllerTest {
    @Test void unauthorizedCreationAndReadDoNotObserveOrScheduleTasks() throws Exception {
        var service=mock(AddNodeReadinessService.class);var permissions=mock(RoleAuthenticationUtil.class);
        var planning=mock(io.translab.tantor.server.service.BrokerAdditionPlanningService.class);
        var mvc=MockMvcBuilders.standaloneSetup(new AddNodeReadinessController(service,permissions,planning)).build();
        String base="/api/v1/ui/clusters/"+UUID.randomUUID()+"/add-node/previews";
        mvc.perform(post(base).contentType("application/json").content("{\"hostId\":\"candidate\"}")).andExpect(status().isForbidden());
        mvc.perform(get(base+"/"+UUID.randomUUID())).andExpect(status().isForbidden());
        mvc.perform(get(base+"/"+UUID.randomUUID()+"/broker-plan")).andExpect(status().isForbidden());
        verifyNoInteractions(service,planning);
    }
}
