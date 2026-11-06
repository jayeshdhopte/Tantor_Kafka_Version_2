package io.translab.tantor.server.web;

import io.translab.tantor.server.service.AddNodeProcessInspectionService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.UUID;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class AddNodeProcessInspectionControllerTest {
    @Test void bothEndpointsRequireAddNodePermissionBeforeAnyObservation()throws Exception {
        var service=mock(AddNodeProcessInspectionService.class);var permissions=mock(RoleAuthenticationUtil.class);
        var mvc=MockMvcBuilders.standaloneSetup(new AddNodeProcessInspectionController(service,permissions)).build();String path="/api/v1/ui/clusters/"+UUID.randomUUID()+"/add-node/process-inspections";
        mvc.perform(post(path).contentType("application/json").content("{\"serviceId\":\""+UUID.randomUUID()+"\"}")).andExpect(status().isForbidden());
        mvc.perform(get(path+"/"+UUID.randomUUID())).andExpect(status().isForbidden());verifyNoInteractions(service);
    }
    @Test void authorizedStartRequiresServiceSelectionAndNeverAcceptsHostCommands()throws Exception {
        var service=mock(AddNodeProcessInspectionService.class);var permissions=mock(RoleAuthenticationUtil.class);when(permissions.canAccess("Bearer fixture",RoleAuthenticationUtil.ADD_NODE)).thenReturn(true);
        var mvc=MockMvcBuilders.standaloneSetup(new AddNodeProcessInspectionController(service,permissions)).build();String path="/api/v1/ui/clusters/"+UUID.randomUUID()+"/add-node/process-inspections";
        mvc.perform(post(path).header("Authorization","Bearer fixture").contentType("application/json").content("{\"hostId\":\"arbitrary\",\"command\":\"stop\"}")).andExpect(status().isBadRequest());verifyNoInteractions(service);
    }
}
