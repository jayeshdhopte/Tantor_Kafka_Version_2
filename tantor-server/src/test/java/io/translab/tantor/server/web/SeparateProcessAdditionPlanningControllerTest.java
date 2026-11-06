package io.translab.tantor.server.web;

import io.translab.tantor.server.service.SeparateProcessAdditionPlanningService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.server.ResponseStatusException;
import java.util.UUID;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class SeparateProcessAdditionPlanningControllerTest {
    @Test void deniedReviewCannotObserveKafka() throws Exception {
        var service = mock(SeparateProcessAdditionPlanningService.class); var permissions = mock(RoleAuthenticationUtil.class);
        var mvc = MockMvcBuilders.standaloneSetup(new SeparateProcessAdditionPlanningController(service, permissions)).build();
        mvc.perform(get("/api/v1/ui/clusters/" + UUID.randomUUID() + "/add-node/separate-plan").param("hostId", "candidate"))
                .andExpect(status().isForbidden()); verifyNoInteractions(service);
    }
    @Test void authorizedGetPassesContextAndProvidesNoMutationEndpoint() throws Exception {
        var service = mock(SeparateProcessAdditionPlanningService.class); var permissions = mock(RoleAuthenticationUtil.class);
        var id = UUID.randomUUID(); when(permissions.canAccess("Bearer review", RoleAuthenticationUtil.ADD_NODE)).thenReturn(true);
        when(service.review(id, "candidate", "rack1")).thenThrow(new ResponseStatusException(HttpStatus.CONFLICT, "Resolve candidate conflict."));
        var mvc = MockMvcBuilders.standaloneSetup(new SeparateProcessAdditionPlanningController(service, permissions)).build();
        String path = "/api/v1/ui/clusters/" + id + "/add-node/separate-plan";
        mvc.perform(get(path).header("Authorization", "Bearer review").param("hostId", "candidate").param("brokerRack", "rack1"))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.message").value("Resolve candidate conflict."));
        verify(service).review(id, "candidate", "rack1");
        mvc.perform(post(path).header("Authorization", "Bearer review")).andExpect(status().isMethodNotAllowed()); verifyNoMoreInteractions(service);
    }
}
