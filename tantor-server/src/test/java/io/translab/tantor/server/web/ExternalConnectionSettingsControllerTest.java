package io.translab.tantor.server.web;
import io.translab.tantor.server.service.ExternalConnectionSettingsService;
import io.translab.tantor.server.dto.ExternalConnectionSettings;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
class ExternalConnectionSettingsControllerTest {
 @Test void cannotReadOrMutateSettingsWithoutTheCorrectPermission(){
  var service=mock(ExternalConnectionSettingsService.class);var permissions=mock(RoleAuthenticationUtil.class);
  var controller=new ExternalConnectionSettingsController(service,permissions);var id=UUID.randomUUID();
  assertThat(controller.get("bad",id).getStatusCode().value()).isEqualTo(403);
  assertThat(controller.save("bad",id,new ExternalConnectionSettings.Request()).getStatusCode().value()).isEqualTo(403);
  verifyNoInteractions(service);
 }
}
