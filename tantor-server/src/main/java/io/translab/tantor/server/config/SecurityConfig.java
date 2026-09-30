package io.translab.tantor.server.config;

import io.translab.tantor.server.security.JwtAuthenticationFilter;
import io.translab.tantor.server.security.JwtUtils;
import jakarta.servlet.DispatcherType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.util.matcher.IpAddressMatcher;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

        private static final RequestMatcher INTERNAL_PROMETHEUS_PATH = new AntPathRequestMatcher(
                        "/internal/prometheus/**", HttpMethod.GET.name());
        private static final RequestMatcher IPV4_LOOPBACK = new IpAddressMatcher("127.0.0.0/8");
        private static final RequestMatcher IPV6_LOOPBACK = new IpAddressMatcher("::1");
        private static final RequestMatcher LOOPBACK_INTERNAL_PROMETHEUS = request -> INTERNAL_PROMETHEUS_PATH
                        .matches(request)
                        && (IPV4_LOOPBACK.matches(request) || IPV6_LOOPBACK.matches(request));

        private final JwtUtils jwtUtils;

        public SecurityConfig(JwtUtils jwtUtils) {
                this.jwtUtils = jwtUtils;
        }

        @Bean
        public JwtAuthenticationFilter authenticationJwtTokenFilter() {
                return new JwtAuthenticationFilter(jwtUtils);
        }

        @Bean
        public PasswordEncoder passwordEncoder() {
                return new BCryptPasswordEncoder();
        }

        @Bean
        public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
                http.csrf(AbstractHttpConfigurer::disable)
                                .sessionManagement(session -> session
                                                .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                                .exceptionHandling(exceptions -> exceptions
                                                .authenticationEntryPoint(
                                                                new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)))
                                .authorizeHttpRequests(auth -> {
                                        // Allow error dispatches so Spring can render error pages.
                                        auth.dispatcherTypeMatchers(DispatcherType.ERROR).permitAll();

                                        // ── Anonymous / public endpoints ──
                                        auth.requestMatchers(HttpMethod.POST, "/api/v1/auth/login").permitAll();
                                        auth.requestMatchers(HttpMethod.GET, "/api/v1/monitoring/health").permitAll();

                                        // ── Agent endpoints — fully open, no JWT required ──
                                        auth.requestMatchers("/api/v1/agents/**").permitAll();

                                        // Discovery-agent endpoints (heartbeat, reports, tasks, metrics)
                                        auth.requestMatchers(HttpMethod.POST,
                                                        "/api/v1/ui/external-clusters/discovery/report",
                                                        "/api/v1/ui/external-clusters/discovery/heartbeat",
                                                        "/api/v1/ui/external-clusters/discovery/service-tasks/claim",
                                                        "/api/v1/ui/external-clusters/discovery/service-tasks/*/complete",
                                                        "/api/v1/ui/external-clusters/discovery/*/tasks/complete",
                                                        "/api/v1/ui/external-clusters/discovery/*/metrics",
                                                        "/api/v1/ui/clusters/external/*/tasks/complete",
                                                        "/api/v1/ui/clusters/external/*/tasks/metrics").permitAll();
                                        auth.requestMatchers(HttpMethod.GET,
                                                        "/api/v1/ui/external-clusters/discovery/*/tasks",
                                                        "/api/v1/ui/clusters/external/*/tasks").permitAll();

                                        // Artifact downloads — agents need these without a JWT
                                        auth.requestMatchers(HttpMethod.GET, "/api/v1/artifacts/*/download").permitAll();

                                        // ── Prometheus scrape (localhost only) ──
                                        auth.requestMatchers(LOOPBACK_INTERNAL_PROMETHEUS).permitAll();
                                        auth.requestMatchers("/internal/prometheus/**").denyAll();

                                        // ── UI / browser endpoints — require JWT roles ──
                                        auth.requestMatchers("/api/v1/auth/users/**", "/api/v1/ldap/**")
                                                        .hasRole("ADMIN");
                                        auth.requestMatchers("/swagger-ui/**", "/v3/api-docs/**").hasRole("ADMIN");

                                        auth.requestMatchers(HttpMethod.GET, "/api/v1/**")
                                                        .hasAnyRole("MONITOR", "ADMIN");
                                        auth.requestMatchers(HttpMethod.HEAD, "/api/v1/**")
                                                        .hasAnyRole("MONITOR", "ADMIN");
                                        auth.requestMatchers(HttpMethod.POST, "/api/v1/**").hasRole("ADMIN");
                                        auth.requestMatchers(HttpMethod.PUT, "/api/v1/**").hasRole("ADMIN");
                                        auth.requestMatchers(HttpMethod.PATCH, "/api/v1/**").hasRole("ADMIN");
                                        auth.requestMatchers(HttpMethod.DELETE, "/api/v1/**").hasRole("ADMIN");

                                        // Reject everything else
                                        auth.requestMatchers("/api/v1/**").denyAll();
                                        auth.anyRequest().denyAll();
                                });

                // The JWT filter is kept in the chain so that *if* a valid token is
                // present the SecurityContext is populated (useful for audit logging),
                // but no request will be rejected for lacking one.
                http.addFilterBefore(authenticationJwtTokenFilter(), UsernamePasswordAuthenticationFilter.class);

                return http.build();
        }
}
