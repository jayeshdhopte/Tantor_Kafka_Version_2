package io.translab.tantor.server.web;
import io.translab.tantor.server.service.ControllerAdditionPlanningService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.UUID;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
class ControllerAdditionPlanningControllerTest {
 @Test void unauthorizedReviewCannotObserveKafka()throws Exception{var service=mock(ControllerAdditionPlanningService.class);var permissions=mock(RoleAuthenticationUtil.class);var mvc=MockMvcBuilders.standaloneSetup(new ControllerAdditionPlanningController(service,permissions)).build();mvc.perform(get("/api/v1/ui/clusters/"+UUID.randomUUID()+"/add-node/controller-plan").param("hostId","candidate")).andExpect(status().isForbidden());verifyNoInteractions(service);}
}
