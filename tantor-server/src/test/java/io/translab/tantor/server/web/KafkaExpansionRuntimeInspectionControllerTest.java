package io.translab.tantor.server.web;

import io.translab.tantor.server.service.KafkaExpansionRuntimeInspection;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaExpansionRuntimeInspectionControllerTest {
    final KafkaExpansionRuntimeInspection service=mock(KafkaExpansionRuntimeInspection.class);final RoleAuthenticationUtil permissions=mock(RoleAuthenticationUtil.class);final KafkaExpansionRuntimeInspectionController controller=new KafkaExpansionRuntimeInspectionController(service,permissions);
    @Test void unauthorizedRequestsCannotReadOrQueueParticipantTasks(){var cluster=UUID.randomUUID();assertEquals(403,controller.start(cluster,new KafkaExpansionRuntimeInspectionController.Request(UUID.randomUUID()),null).getStatusCode().value());assertEquals(403,controller.get(cluster,UUID.randomUUID(),null).getStatusCode().value());verifyNoInteractions(service);}
    @Test void submissionTakesAServiceReferenceRatherThanArbitraryPaths(){UUID cluster=UUID.randomUUID(),id=UUID.randomUUID();when(permissions.canAccess("auth",RoleAuthenticationUtil.ADD_NODE)).thenReturn(true);assertEquals(202,controller.start(cluster,new KafkaExpansionRuntimeInspectionController.Request(id),"auth").getStatusCode().value());verify(service).start(cluster,id);assertEquals(400,controller.start(cluster,new KafkaExpansionRuntimeInspectionController.Request(null),"auth").getStatusCode().value());}
    @Test void conflictsRemainExplicitAndDoNotBecomeSuccessfulObservations(){var response=controller.error(new ResponseStatusException(HttpStatus.CONFLICT,"Participant context changed."));assertEquals(409,response.getStatusCode().value());assertEquals(Map.of("message","Participant context changed."),response.getBody());}
}
