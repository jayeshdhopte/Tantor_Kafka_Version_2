package io.translab.tantor.server.config;

import io.translab.tantor.server.security.JwtUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = SecurityConfigAuthorizationTest.PolicyProbeController.class)
@Import({ SecurityConfig.class, SecurityConfigAuthorizationTest.PolicyProbeController.class })
class SecurityConfigAuthorizationTest {

        @Autowired
        private MockMvc mockMvc;

        @MockBean
        private JwtUtils jwtUtils;

        @BeforeEach
        void configureVerifiedTokens() {
                when(jwtUtils.verify("monitor-token")).thenReturn(principal("monitor-user", "monitor"));
                when(jwtUtils.verify("admin-token")).thenReturn(principal("admin-user", "admin"));
                when(jwtUtils.verify("invalid-token")).thenReturn(null);
        }

        @Test
        void permitsOnlyTheExplicitAnonymousLoginAndHealthEndpoints() throws Exception {
                mockMvc.perform(post("/api/v1/auth/login")).andExpect(status().isOk());
                mockMvc.perform(get("/api/v1/monitoring/health")).andExpect(status().isOk());

                mockMvc.perform(get("/api/v1/auth/me")).andExpect(status().isUnauthorized());
                mockMvc.perform(get("/api/v1/ui/dashboard")).andExpect(status().isUnauthorized());
                mockMvc.perform(get("/api/v1/clusters/cluster-1/overview")).andExpect(status().isUnauthorized());
                mockMvc.perform(get("/api/v1/clusters/cluster-1/config")).andExpect(status().isUnauthorized());
                mockMvc.perform(get("/api/v1/monitoring/clusters")).andExpect(status().isUnauthorized());
                mockMvc.perform(get("/internal/prometheus/targets").with(remoteAddress("203.0.113.10")))
                                .andExpect(status().isUnauthorized());
                mockMvc.perform(get("/api/v1/unclassified")).andExpect(status().isUnauthorized());
                mockMvc.perform(get("/error")).andExpect(status().isUnauthorized());
        }

        @Test
        void acceptsAuthenticatedReadRolesAndRejectsInvalidTokens() throws Exception {
                mockMvc.perform(get("/api/v1/ui/dashboard").header("Authorization", "Bearer monitor-token"))
                                .andExpect(status().isOk());
                mockMvc.perform(get("/api/v1/clusters/cluster-1/overview")
                                .header("Authorization", "Bearer monitor-token"))
                                .andExpect(status().isOk());
                mockMvc.perform(get("/api/v1/monitoring/clusters")
                                .header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isOk());
                mockMvc.perform(get("/api/v1/ui/dashboard").header("Authorization", "Bearer invalid-token"))
                                .andExpect(status().isUnauthorized());
        }

        @Test
        void requiresAdminForMutationsAndAdministrativeNamespaces() throws Exception {
                mockMvc.perform(post("/api/v1/clusters/cluster-1/actions/rolling-restart")
                                .header("Authorization", "Bearer monitor-token"))
                                .andExpect(status().isForbidden());
                mockMvc.perform(delete("/api/v1/ui/clusters/cluster-1")
                                .header("Authorization", "Bearer monitor-token"))
                                .andExpect(status().isForbidden());
                mockMvc.perform(get("/api/v1/auth/users").header("Authorization", "Bearer monitor-token"))
                                .andExpect(status().isForbidden());

                mockMvc.perform(post("/api/v1/clusters/cluster-1/actions/rolling-restart")
                                .header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isOk());
                mockMvc.perform(delete("/api/v1/ui/clusters/cluster-1")
                                .header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isNoContent());
                mockMvc.perform(get("/api/v1/auth/users").header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isOk());
        }

        @Test
        void agentEndpointsAreFullyOpenWithoutAuthentication() throws Exception {
                // Agent endpoints are intentionally permitAll — no JWT required.
                mockMvc.perform(post("/api/v1/agents/register")).andExpect(status().isOk());
                mockMvc.perform(post("/api/v1/ui/external-clusters/discovery/report"))
                                .andExpect(status().isOk());
                mockMvc.perform(post("/api/v1/agents/register").header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isOk());
                mockMvc.perform(get("/api/v1/agents/host-1/tasks")
                                .header("Authorization", "Bearer monitor-token"))
                                .andExpect(status().isOk());
        }

        @Test
        void internalPrometheusIsLoopbackOnlyAndUnclassifiedMethodsFailClosed() throws Exception {
                mockMvc.perform(get("/internal/prometheus/targets"))
                                .andExpect(status().isOk());
                mockMvc.perform(get("/internal/prometheus/targets")
                                .with(remoteAddress("203.0.113.10"))
                                .header("X-Forwarded-For", "127.0.0.1")
                                .header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isForbidden());
                mockMvc.perform(options("/api/v1/ui/dashboard")
                                .header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isForbidden());
                mockMvc.perform(get("/not-an-api").header("Authorization", "Bearer admin-token"))
                                .andExpect(status().isForbidden());
        }

        private static JwtUtils.VerifiedPrincipal principal(String username, String role) {
                return new JwtUtils.VerifiedPrincipal(username, List.of(role), Map.of("roles", List.of(role)));
        }

        private static org.springframework.test.web.servlet.request.RequestPostProcessor remoteAddress(String address) {
                return request -> {
                        request.setRemoteAddr(address);
                        return request;
                };
        }

        @RestController
        public static class PolicyProbeController {

                @PostMapping("/api/v1/auth/login")
                ResponseEntity<Void> login() {
                        return ResponseEntity.ok().build();
                }

                @GetMapping({
                                "/api/v1/auth/me",
                                "/api/v1/auth/users",
                                "/api/v1/ui/dashboard",
                                "/api/v1/clusters/{id}/overview",
                                "/api/v1/clusters/{id}/config",
                                "/api/v1/monitoring/health",
                                "/api/v1/monitoring/clusters",
                                "/api/v1/unclassified",
                                "/internal/prometheus/targets",
                                "/api/v1/agents/{id}/tasks",
                                "/not-an-api"
                })
                ResponseEntity<Void> read() {
                        return ResponseEntity.ok().build();
                }

                @PostMapping({
                                "/api/v1/clusters/{id}/actions/rolling-restart",
                                "/api/v1/agents/register",
                                "/api/v1/ui/external-clusters/discovery/report"
                })
                ResponseEntity<Void> mutate() {
                        return ResponseEntity.ok().build();
                }

                @DeleteMapping("/api/v1/ui/clusters/{id}")
                ResponseEntity<Void> delete() {
                        return ResponseEntity.noContent().build();
                }
        }
}
