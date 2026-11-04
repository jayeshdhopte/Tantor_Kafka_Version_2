package io.translab.tantor.artifact.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@AutoConfigureMockMvc
@EnabledIfEnvironmentVariable(named="TANTOR_STATUS_AUDIT_TEST_URL",matches=".+_check_.+")
class ArtifactActorPersistenceTest {
    static final Path root;
    static final com.sun.net.httpserver.HttpServer identityServer;
    static final Map<String,String> identities=new java.util.concurrent.ConcurrentHashMap<>();
    static {try{
        root=Files.createTempDirectory("artifact-actor-check-");
        identityServer=com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1",0),0);
        identityServer.createContext("/identity",exchange->{
            String actor=identities.get(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body=actor==null?new byte[0]:new ObjectMapper().writeValueAsBytes(Map.of("username",actor,"roles",List.of("ROLE_ADMIN")));
            exchange.sendResponseHeaders(actor==null?401:200,body.length);
            try(var out=exchange.getResponseBody()){out.write(body);}
        });identityServer.start();
    }catch(Exception e){throw new ExceptionInInitializerError(e);}}
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("tantor.auth.identity-url",()->"http://127.0.0.1:"+identityServer.getAddress().getPort()+"/identity");
        p.add("spring.datasource.url",()->System.getenv("TANTOR_STATUS_AUDIT_TEST_URL"));
        p.add("spring.datasource.username",()->System.getenv("TANTOR_STATUS_AUDIT_TEST_USER"));
        p.add("spring.datasource.password",()->System.getenv("TANTOR_STATUS_AUDIT_TEST_PASSWORD"));
        p.add("tantor.repository.base-path",()->root.toString());p.add("tantor.cors.allowed-origins",()->"http://localhost");
    }
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    String authorization(String actor) {
        String token="Bearer fixture-"+UUID.randomUUID();identities.put(token,actor);return token;
    }
    @org.junit.jupiter.api.AfterAll static void stopIdentityServer(){identityServer.stop(0);}
    @Test void persistsTheDistinctInitiatorsForTaskDownloadDirectDownloadVerifyAndDelete() throws Exception {
        UUID artifact=UUID.randomUUID(),task=UUID.randomUUID();String capability=UUID.randomUUID().toString();
        Files.createDirectories(root.resolve("artifacts/fixture"));byte[] bytes="artifact identity persistence fixture".getBytes(StandardCharsets.UTF_8);
        Files.write(root.resolve("artifacts/fixture/identity.tgz"),bytes);String checksum=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        jdbc.update("""
            INSERT INTO kf_artifact(id,root_artifact_id,action,service_type,version_no,binary_file_name,path_of_tar,full_file_path,file_size_bytes,content_type,checksum,status,created_by,updated_by,created_time,update_time)
            VALUES (?,?,'UPLOAD','KAFKA','fixture','identity.tgz','fixture/identity.tgz',?,?,'application/gzip',?,'AVAILABLE','fixture-uploader','fixture-uploader',now(),now())
            """,artifact,artifact,root.resolve("artifacts/fixture/identity.tgz").toString(),bytes.length,checksum);
        String host=jdbc.queryForObject("select id from kf_hosts limit 1",String.class);
        jdbc.update("INSERT INTO kf_tasks(id,host_id,command,status,parameters) VALUES (?,?,'DISTRIBUTE_PARCEL','IN_PROGRESS',?::jsonb)",task,host,
                mapper.writeValueAsString(Map.of("artifact_id",artifact.toString(),"initiated_by","requester.one","download_context_token",capability,"target_hostname","fixture-node","target_host_ip","192.168.3.191")));
        mvc.perform(get("/api/v1/artifacts/"+artifact+"/download").param("downloadToken",capability).header("Authorization",authorization("agent.account")))
                .andExpect(status().isOk()).andExpect(content().bytes(bytes));
        var row=jdbc.queryForMap("SELECT * FROM kf_artifact WHERE task_id=? AND action='DOWNLOAD_STARTED'",task);
        for(String key:List.of("created_by","updated_by","user_name","downloaded_by"))assertThat(row.get(key)).as(key).isEqualTo("requester.one");
        assertThat(row.get("executor")).isEqualTo("agent:"+host);assertThat(row.get("verified_checksum")).isNull();
        assertThat(row.get("host_ip")).isEqualTo("192.168.3.191");
        mvc.perform(get("/api/v1/artifacts/"+artifact+"/download").header("Authorization",authorization("reader.two"))).andExpect(status().isOk());
        mvc.perform(post("/api/v1/artifacts/"+artifact+"/verify").header("Authorization",authorization("verifier.three")))
                .andExpect(status().isOk()).andExpect(jsonPath("verified").value(true));
        mvc.perform(delete("/api/v1/artifacts/"+artifact).header("Authorization",authorization("remover.four"))).andExpect(status().isNoContent());
        assertThat(jdbc.queryForObject("SELECT created_by FROM kf_artifact WHERE root_artifact_id=? AND action='VERIFY'",String.class,artifact)).isEqualTo("verifier.three");
        assertThat(jdbc.queryForObject("SELECT created_by FROM kf_artifact WHERE root_artifact_id=? AND action='DELETE'",String.class,artifact)).isEqualTo("remover.four");
        assertThat(jdbc.queryForList("SELECT DISTINCT user_name FROM kf_artifact_audit_log WHERE artifact_id=?",String.class,artifact.toString()))
                .containsExactlyInAnyOrder("requester.one","reader.two","verifier.three","remover.four");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM kf_artifact_audit_log WHERE artifact_id=? AND (created_by IS DISTINCT FROM user_name OR user_name='system')",Integer.class,artifact.toString())).isZero();
    }
}
