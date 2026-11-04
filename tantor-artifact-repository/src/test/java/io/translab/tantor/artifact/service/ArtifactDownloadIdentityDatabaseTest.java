package io.translab.tantor.artifact.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class ArtifactDownloadIdentityDatabaseTest {
    final JdbcTemplate jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"),System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"),System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD")));
    final ArtifactDownloadIdentity resolver=new ArtifactDownloadIdentity(jdbc);
    final UUID artifact=UUID.randomUUID();
    String task(String actor,boolean expired) throws Exception {
        String token=UUID.randomUUID().toString();
        String host=jdbc.queryForObject("SELECT id FROM kf_hosts LIMIT 1",String.class);
        var parameters=Map.of("initiated_by",actor,"artifact_id",artifact.toString(),"download_context_token",token,"target_hostname","node-fixture","target_host_ip","192.168.3.191");
        jdbc.update("INSERT INTO kf_tasks(id,host_id,command,status,parameters,created_at) VALUES (?,?,'DISTRIBUTE_PARCEL','IN_PROGRESS',?::jsonb,now()-(? * interval '1 hour'))",UUID.randomUUID(),host,new ObjectMapper().writeValueAsString(parameters),expired?25:0);
        return token;
    }
    @Test void persistedTaskInitiatorWinsOverAgentAuthenticationAndCannotBeSuppliedByTheClient() throws Exception {
        String token=task("reviewer.delta",false);
        var identity=resolver.resolve(artifact,token,"agent-account","127.0.0.1");
        assertThat(identity.actor()).isEqualTo("reviewer.delta");assertThat(identity.executor()).startsWith("agent:");
        assertThat(identity.taskId()).isNotNull();assertThat(identity.hostIp()).isEqualTo("192.168.3.191");
    }
    @Test void rejectsChangedArtifactUnknownTokenExpiredTokenAndMissingInitiator() throws Exception {
        String token=task("reviewer.epsilon",false);
        assertThatThrownBy(()->resolver.resolve(UUID.randomUUID(),token,"spoofed","localhost")).hasMessageContaining("403");
        assertThatThrownBy(()->resolver.resolve(artifact,UUID.randomUUID().toString(),"spoofed","localhost")).hasMessageContaining("403");
        String expired=task("reviewer.zeta",true);
        assertThatThrownBy(()->resolver.resolve(artifact,expired,"spoofed","localhost")).hasMessageContaining("403");
        String missing=task("",false);
        assertThatThrownBy(()->resolver.resolve(artifact,missing,"spoofed","localhost")).hasMessageContaining("initiator not recorded");
    }
    @Test void directAuthenticatedAndUnattributedDownloadsRemainDistinct() {
        assertThat(resolver.resolve(artifact,null,"user.theta","client").actor()).isEqualTo("user.theta");
        assertThat(resolver.resolve(artifact,null,null,"client").actor()).isEqualTo("unknown");
        assertThat(resolver.resolve(artifact,null,"system","client").actor()).isEqualTo("system"); // a genuine principal name is not rewritten
    }
}
