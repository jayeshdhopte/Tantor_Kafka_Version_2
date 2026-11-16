package io.translab.tantor.server.web;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class ExternalBrokerAdditionControllerTest {
  @Test
  void everyExternalBrokerEndpointRequiresExplicitPermission() throws Exception {
    var planning = mock(ExternalBrokerAdditionPlanning.class);
    var ledger = mock(ExternalBrokerAdditionLedger.class);
    var permissions = mock(RoleAuthenticationUtil.class);
    var mvc =
        MockMvcBuilders.standaloneSetup(
                new ExternalBrokerAdditionController(
                    planning, ledger, permissions, mock(ExternalBrokerRecoveryService.class)))
            .build();
    String base = "/api/v1/ui/external-clusters/" + UUID.randomUUID() + "/add-node/";
    mvc.perform(get(base + "broker-review/" + UUID.randomUUID())).andExpect(status().isForbidden());
    mvc.perform(post(base + "broker-operations").contentType("application/json").content("{}"))
        .andExpect(status().isForbidden());
    mvc.perform(get(base + "broker-operations/" + UUID.randomUUID()))
        .andExpect(status().isForbidden());
    mvc.perform(get(base + "broker-operations/request/fixture-request-key"))
        .andExpect(status().isForbidden());
    mvc.perform(post(base + "broker-operations/" + UUID.randomUUID() + "/resume"))
        .andExpect(status().isForbidden());
    mvc.perform(post(base + "broker-operations/" + UUID.randomUUID() + "/cancel-safe"))
        .andExpect(status().isForbidden());
    String tasks = "/api/v1/ui/external-clusters/discovery/add-broker-tasks/candidate";
    mvc.perform(post(tasks + "/claim")).andExpect(status().isForbidden());
    mvc.perform(
            post(tasks + "/" + UUID.randomUUID() + "/result")
                .contentType("application/json")
                .content("{}"))
        .andExpect(status().isForbidden());
    verifyNoInteractions(planning, ledger);
  }
}
