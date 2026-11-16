package io.translab.tantor.server.web;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import io.translab.tantor.server.service.ExternalAddNodeInspectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class ExternalAddNodeInspectionControllerTest {
  @Test
  void unauthorizedRequestsCannotRecordOrInspectHosts() throws Exception {
    var service = mock(ExternalAddNodeInspectionService.class);
    var permissions = mock(RoleAuthenticationUtil.class);
    var mvc =
        MockMvcBuilders.standaloneSetup(
                new ExternalAddNodeInspectionController(service, permissions))
            .build();
    mvc.perform(
            post("/api/v1/ui/external-clusters/discovery/add-node-inspections")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isForbidden());
    mvc.perform(get("/api/v1/ui/external-clusters/" + UUID.randomUUID() + "/add-node/candidates"))
        .andExpect(status().isForbidden());
    verifyNoInteractions(service);
  }
}
