package io.translab.tantor.server.web;
import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.UUID;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
class BrokerAdditionControllerTest {
 @Test void everyOperationEndpointRequiresAddNodePermission()throws Exception {
  var ledger=mock(BrokerAdditionLedger.class);var jobs=mock(JobService.class);var permissions=mock(RoleAuthenticationUtil.class);
  var mvc=MockMvcBuilders.standaloneSetup(new BrokerAdditionController(ledger,jobs,permissions)).build();String base="/api/v1/ui/clusters/"+UUID.randomUUID()+"/add-node/operations";String op=UUID.randomUUID().toString();
  mvc.perform(post(base).contentType("application/json").content("{\"previewId\":\""+UUID.randomUUID()+"\",\"contextHash\":\"hash\"}")).andExpect(status().isForbidden());
  mvc.perform(get(base+"/"+op)).andExpect(status().isForbidden());mvc.perform(post(base+"/"+op+"/resume")).andExpect(status().isForbidden());mvc.perform(post(base+"/"+op+"/cancel-safe")).andExpect(status().isForbidden());verifyNoInteractions(ledger,jobs);
 }
}
