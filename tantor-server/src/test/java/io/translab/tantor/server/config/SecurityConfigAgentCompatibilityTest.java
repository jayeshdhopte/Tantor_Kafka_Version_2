package io.translab.tantor.server.config;

import io.translab.tantor.server.security.JwtUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(
        controllers = SecurityConfigAgentCompatibilityTest.AgentProbeController.class,
        properties = {
                "tantor.runtime.environment=production",
                "tantor.agent.legacy-unauthenticated-enabled=true",
                "tantor.ui.legacy-unauthenticated-enabled=false"
        })
@Import({SecurityConfig.class, SecurityConfigAgentCompatibilityTest.AgentProbeController.class})
class SecurityConfigAgentCompatibilityTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JwtUtils jwtUtils;

    @Test
    void permitsOnlyDedicatedAgentEndpointsWithoutUserJwt() throws Exception {
        mockMvc.perform(post("/api/v1/agents/register")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/agents/host-1/tasks")).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/ui/external-clusters/discovery/report")).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/ui/external-clusters/discovery/heartbeat")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/ui/external-clusters/discovery/test/tasks")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/ui/clusters/external/test/tasks")).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/ui/external-clusters/discovery/test/tasks/complete"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/ui/external-clusters/discovery/test/metrics"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/ui/dashboard")).andExpect(status().isUnauthorized());
        mockMvc.perform(post("/api/v1/ui/external-clusters/bootstrap/register"))
                .andExpect(status().isUnauthorized());
    }

    @RestController
    static class AgentProbeController {
        @PostMapping({
                "/api/v1/agents/register",
                "/api/v1/ui/external-clusters/discovery/report",
                "/api/v1/ui/external-clusters/discovery/heartbeat",
                "/api/v1/ui/external-clusters/discovery/{name}/tasks/complete",
                "/api/v1/ui/external-clusters/discovery/{name}/metrics",
                "/api/v1/ui/external-clusters/bootstrap/register"
        })
        ResponseEntity<Void> postEndpoint() {
            return ResponseEntity.ok().build();
        }

        @GetMapping({
                "/api/v1/agents/{id}/tasks",
                "/api/v1/ui/external-clusters/discovery/{name}/tasks",
                "/api/v1/ui/clusters/external/{name}/tasks",
                "/api/v1/ui/dashboard"
        })
        ResponseEntity<Void> getEndpoint() {
            return ResponseEntity.ok().build();
        }
    }
}
