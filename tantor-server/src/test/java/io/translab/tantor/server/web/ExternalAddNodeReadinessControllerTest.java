package io.translab.tantor.server.web;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import io.translab.tantor.server.service.ExternalAddNodeReadinessService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class ExternalAddNodeReadinessControllerTest {
  @Test
  void allObservationAndAgentEndpointsRequireAddNodePermission() throws Exception {
    var service = mock(ExternalAddNodeReadinessService.class);
    var permissions = mock(RoleAuthenticationUtil.class);
    var mvc =
        MockMvcBuilders.standaloneSetup(
                new ExternalAddNodeReadinessController(service, permissions))
            .build();
    String base = "/api/v1/ui/external-clusters/" + UUID.randomUUID() + "/add-node/readiness";
    mvc.perform(post(base).contentType("application/json").content("{}"))
        .andExpect(status().isForbidden());
    mvc.perform(get(base + "/" + UUID.randomUUID())).andExpect(status().isForbidden());
    String tasks = "/api/v1/ui/external-clusters/discovery/add-node-tasks/candidate";
    mvc.perform(post(tasks + "/claim")).andExpect(status().isForbidden());
    mvc.perform(
            post(tasks + "/" + UUID.randomUUID() + "/result")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isForbidden());
    verifyNoInteractions(service);
  }
}
