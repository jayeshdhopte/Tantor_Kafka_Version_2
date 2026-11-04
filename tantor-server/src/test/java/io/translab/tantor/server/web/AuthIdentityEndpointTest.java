package io.translab.tantor.server.web;

import io.translab.tantor.server.audit.AuditService;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.security.JwtUtils;
import io.translab.tantor.server.service.LdapService;
import org.junit.jupiter.api.*;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.password.PasswordEncoder;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class AuthIdentityEndpointTest {
    final AuthController controller=new AuthController(mock(JwtUtils.class),mock(LdapConfigRepository.class),mock(LdapService.class),mock(UserRepository.class),mock(PasswordEncoder.class),mock(AuditService.class));
    @AfterEach void clear(){SecurityContextHolder.clearContext();}
    @Test void exposesTheVerifiedPrincipalAndAuthoritiesWithoutUserOrRoleSubstitution() {
        for(String user:List.of("operator.actual","system")) {
            SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(user,null,List.of(new SimpleGrantedAuthority("ROLE_MONITOR"))));
            var response=controller.identity();assertThat(response.getStatusCode().value()).isEqualTo(200);
            assertThat(response.getBody()).containsEntry("username",user).containsEntry("roles",List.of("ROLE_MONITOR"));
        }
    }
    @Test void anonymousRequestsHaveNoClaimedIdentity(){assertThat(controller.identity().getStatusCode().value()).isEqualTo(401);}
}
