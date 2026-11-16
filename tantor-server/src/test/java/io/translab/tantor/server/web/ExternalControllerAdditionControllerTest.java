package io.translab.tantor.server.web;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class ExternalControllerAdditionControllerTest {
  @Test
  void everyControllerEndpointRequiresExplicitPermission() throws Exception {
    var planning = mock(ExternalControllerPlanning.class);
    var ledger = mock(ExternalControllerLedger.class);
    var tasks = mock(ExternalControllerTasks.class);
    var permissions = mock(RoleAuthenticationUtil.class);
    var mvc =
        MockMvcBuilders.standaloneSetup(
                new ExternalControllerAdditionController(planning, ledger, tasks, permissions))
            .build();
    String base = "/api/v1/ui/external-clusters/" + UUID.randomUUID() + "/add-node/",
        id = UUID.randomUUID().toString();
    mvc.perform(post(base + "controller-previews").contentType("application/json").content("{}"))
        .andExpect(status().isForbidden());
    mvc.perform(get(base + "controller-previews/" + id)).andExpect(status().isForbidden());
    mvc.perform(post(base + "controller-operations").contentType("application/json").content("{}"))
        .andExpect(status().isForbidden());
    mvc.perform(get(base + "controller-operations/" + id)).andExpect(status().isForbidden());
    mvc.perform(get(base + "controller-operations/request/fixture-request-key"))
        .andExpect(status().isForbidden());
    mvc.perform(post(base + "controller-operations/" + id + "/resume"))
        .andExpect(status().isForbidden());
    mvc.perform(post(base + "controller-operations/" + id + "/cancel-safe"))
        .andExpect(status().isForbidden());
    mvc.perform(
            post("/api/v1/ui/external-clusters/discovery/controller-tasks/candidate/claim")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isForbidden());
    mvc.perform(
            post("/api/v1/ui/external-clusters/discovery/controller-tasks/candidate/"
                    + id
                    + "/result")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isForbidden());
    verifyNoInteractions(planning, ledger, tasks);
  }
}
