package io.translab.tantor.server.web;

import io.translab.tantor.server.service.*;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaExpansionControllerTest {
    final KafkaExpansionPreviewService previews=mock(KafkaExpansionPreviewService.class);
    final KafkaExpansionLedger ledger=mock(KafkaExpansionLedger.class);
    final JobService jobs=mock(JobService.class);
    final RoleAuthenticationUtil permissions=mock(RoleAuthenticationUtil.class);
    final KafkaExpansionController controller=new KafkaExpansionController(previews,ledger,jobs,permissions);
    @Test void permissionsProtectEveryExecutionAndRecoveryRoute(){
        UUID cluster=UUID.randomUUID(),id=UUID.randomUUID();
        assertEquals(403,controller.submit(cluster,new KafkaExpansionController.Submit(id,"a".repeat(64)),null,"key").getStatusCode().value());
        assertEquals(403,controller.preview(cluster,null,null).getStatusCode().value());
        assertEquals(403,controller.review(cluster,id,null).getStatusCode().value());
        assertEquals(403,controller.get(cluster,id,null).getStatusCode().value());
        assertEquals(403,controller.resume(cluster,id,null).getStatusCode().value());
        assertEquals(403,controller.cancel(cluster,id,null).getStatusCode().value());
        verifyNoInteractions(previews,ledger,jobs);
    }
    @Test void lostSubmissionResponseReturnsExistingOperationWithoutRequiringExpiredPreview(){
        when(permissions.canAccess("auth",RoleAuthenticationUtil.ADD_NODE)).thenReturn(true);
        UUID cluster=UUID.randomUUID(),preview=UUID.randomUUID();String context="a".repeat(64),key="same-request-key";
        var accepted=new KafkaExpansionLedger.Accepted(UUID.randomUUID(),UUID.randomUUID(),KafkaExpansionState.RECOVERY_REQUIRED);
        when(ledger.acceptedRequest(cluster,key,BrokerAdditionLedger.hash(preview+":"+context))).thenReturn(Optional.of(accepted));
        var result=controller.submit(cluster,new KafkaExpansionController.Submit(preview,context),"auth",key);
        assertEquals(202,result.getStatusCode().value());assertSame(accepted,result.getBody());verifyNoInteractions(previews,jobs);
        verify(ledger,never()).accept(any(),any(),any());
    }
    @Test void publicationRecoveryRetriesOriginalJobButSafeCancellationCannotResume(){
        when(permissions.canAccess("auth",RoleAuthenticationUtil.ADD_NODE)).thenReturn(true);
        UUID cluster=UUID.randomUUID(),id=UUID.randomUUID();var operation=new KafkaExpansionOperation();operation.setJobId(UUID.randomUUID());operation.setState(KafkaExpansionState.SUCCEEDED);
        when(ledger.get(cluster,id)).thenReturn(operation);
        assertEquals(202,controller.resume(cluster,id,"auth").getStatusCode().value());verify(jobs).retryJob(operation.getJobId());
        clearInvocations(jobs);operation.setState(KafkaExpansionState.CANCELLED_SAFE);
        assertEquals(409,controller.resume(cluster,id,"auth").getStatusCode().value());verifyNoInteractions(jobs);
    }
}
