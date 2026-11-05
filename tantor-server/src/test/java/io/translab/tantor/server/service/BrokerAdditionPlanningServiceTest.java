package io.translab.tantor.server.service;
import io.translab.tantor.server.dto.AddNodeReadiness.Preview;
import io.translab.tantor.server.repository.ClusterRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.time.Instant;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
class BrokerAdditionPlanningServiceTest {
    @Test void staleEvidenceCannotReadTopologyOrScheduleAnything() {
        var readiness=mock(AddNodeReadinessService.class);var clusters=mock(ClusterRepository.class);
        UUID cluster=UUID.randomUUID(),preview=UUID.randomUUID();
        when(readiness.get(cluster,preview)).thenReturn(new Preview(preview,cluster,"candidate","hash",Instant.now(),Instant.now(),"STALE",List.of(),Map.of(),false));
        var service=new BrokerAdditionPlanningService(readiness,clusters,new ObjectMapper());
        assertThrows(org.springframework.web.server.ResponseStatusException.class,()->service.review(cluster,preview));
        verifyNoInteractions(clusters);
    }
}
