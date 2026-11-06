package io.translab.tantor.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.HostRepository;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AddNodeProtectedHostGuardTest {
    final HostRepository hosts=mock(HostRepository.class);
    final AddNodeInspectionProperties policy=new AddNodeInspectionProperties();
    final ObjectMapper mapper=new ObjectMapper();
    Cluster cluster(String config){var c=new Cluster();c.setConfigJson(config);c.setServices(List.of());return c;}
    @Test void excludedIdBlocksBeforeAddressLookup(){policy.setExcludedHostIds(List.of("broker6"));assertThrows(ResponseStatusException.class,()->AddNodeProtectedHostGuard.hostId("broker6",hosts,policy));verifyNoInteractions(hosts);}
    @Test void excludedIpBlocksAliasHostIdentity(){policy.setExcludedHostIps(List.of("192.168.3.213"));var h=new Host();h.setId("other-alias");h.setHostIp("192.168.3.213");when(hosts.findById(h.getId())).thenReturn(Optional.of(h));assertThrows(ResponseStatusException.class,()->AddNodeProtectedHostGuard.hostId(h.getId(),hosts,policy));}
    @Test void excludedBootstrapAndControllerEndpointCannotBeUsed(){policy.setExcludedHostIps(List.of("192.168.3.213"));var c=cluster("{}");c.setBootstrapServers("192.168.3.213:9092");assertThrows(ResponseStatusException.class,()->AddNodeProtectedHostGuard.cluster(c,hosts,policy,mapper));var withController=cluster("{\"controller_quorum_voters\":\"101@192.168.3.213:9093\"}");assertThrows(ResponseStatusException.class,()->AddNodeProtectedHostGuard.cluster(withController,hosts,policy,mapper));}
    @Test void unavailableParticipantMetadataDoesNotBypassIpExclusion(){policy.setExcludedHostIps(List.of("192.168.3.213"));var c=cluster("{}");var member=new ClusterServiceAssignment();member.setHostId("unknown");c.setServices(List.of(member));assertThrows(ResponseStatusException.class,()->AddNodeProtectedHostGuard.cluster(c,hosts,policy,mapper));}
    @Test void allowedClusterAndUnrestrictedDefaultDoNotTouchExcludedHost(){policy.setExcludedHostIps(List.of("192.168.3.213"));var c=cluster("{\"controller_quorum_voters\":\"101@192.0.2.10:9093\"}");c.setBootstrapServers("192.0.2.10:9092");assertDoesNotThrow(()->AddNodeProtectedHostGuard.cluster(c,hosts,policy,mapper));verifyNoInteractions(hosts);}
}
