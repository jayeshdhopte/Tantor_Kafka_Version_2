package io.translab.tantor.artifact.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.artifact.audit.ArtifactAuditService;
import io.translab.tantor.artifact.domain.*;
import io.translab.tantor.artifact.service.*;
import io.translab.tantor.artifact.util.RoleAuthenticationUtil;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import java.nio.file.Path;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class ArtifactActorControllerTest {
    final ArtifactService service=mock(ArtifactService.class);
    final ArtifactAuditService audit=mock(ArtifactAuditService.class);
    final RoleAuthenticationUtil roles=mock(RoleAuthenticationUtil.class);
    final ArtifactController controller=new ArtifactController(service,mock(ManifestService.class),new ObjectMapper(),audit,roles);
    ArtifactActorControllerTest(){ReflectionTestUtils.setField(controller,"downloadIdentity",new ArtifactDownloadIdentity(mock(JdbcTemplate.class)));}
    @Test void directDownloadUsesTheAuthenticatedPrincipalAndWritesTheSameActorToAudit() {
        UUID id=UUID.randomUUID();Artifact source=new Artifact();source.setId(id);source.setFileName("fixture.tgz");source.setChecksumSha256("sha");source.setContentType("application/gzip");
        when(service.get(id)).thenReturn(source);when(service.resolveForDownload(id)).thenReturn(Path.of("fixture.tgz"));
        var request=new MockHttpServletRequest();request.addHeader("Authorization","Bearer actual-user");
        when(roles.authenticatedUsername("Bearer actual-user")).thenReturn("reader.omega");
        assertThat(controller.download(id,request).getStatusCode().value()).isEqualTo(200);
        var identity=org.mockito.ArgumentCaptor.forClass(ArtifactDownloadIdentity.Identity.class);
        verify(service).logDownloadStarted(eq(id),identity.capture());assertThat(identity.getValue().actor()).isEqualTo("reader.omega");
        verify(audit).record(eq("reader.omega"),eq("PACKAGE_DOWNLOAD_STARTED"),eq(id.toString()),eq("SUCCESS"),isNull(),isNull(),any(),anyString());
    }
    @Test void verifyAndDeletePropagateTheActualCallerToMetadataAndAudit() {
        UUID id=UUID.randomUUID();Artifact source=new Artifact();source.setId(id);source.setStatus(ArtifactStatus.AVAILABLE);source.setFileName("fixture.tgz");
        when(roles.canAccess(anyString(),anyString())).thenReturn(true);
        when(roles.username("Bearer verifier")).thenReturn("verifier.sigma");when(service.verifyIntegrity(id,"verifier.sigma")).thenReturn(true);
        controller.verify(id,"Bearer verifier");verify(service).verifyIntegrity(id,"verifier.sigma");
        verify(audit).record(eq("verifier.sigma"),eq("PACKAGE_VERIFIED"),eq(id.toString()),eq("SUCCESS"),isNull(),isNull(),any(),isNull());
        when(roles.username("Bearer remover")).thenReturn("remover.tau");when(service.get(id)).thenReturn(source);
        controller.delete(id,"Bearer remover");verify(service).delete(id,"remover.tau");
        verify(audit).record(eq("remover.tau"),eq("PACKAGE_REMOVED"),eq(id.toString()),eq("SUCCESS"),any(),any(),isNull(),isNull());
    }
}
