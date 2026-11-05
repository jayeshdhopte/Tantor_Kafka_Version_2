package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.repository.ClusterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

/** Reviewable contract only. Does not reserve identities, publish membership or dispatch mutations. */
@Service @RequiredArgsConstructor
public class BrokerAdditionPlanningService {
    @org.springframework.beans.factory.annotation.Value("${tantor.add-node.execution-enabled:false}") private boolean executionEnabled;
    private final AddNodeReadinessService readiness;
    private final ClusterRepository clusters;
    private final ObjectMapper mapper;

    public record Review(UUID previewId, String contextHash, java.time.Instant expiresAt,
                         BrokerAdditionPlan proposedPlan, List<String> executionSteps,
                         boolean executionEnabled, String blocker) {}

    public Review review(UUID clusterId, UUID previewId) {
        var preview=readiness.get(clusterId,previewId);
        if(!"PASS".equals(preview.status()))
            throw new ResponseStatusException(HttpStatus.CONFLICT,"Run complete passing readiness checks before reviewing an execution plan.");
        var cluster=clusters.findWithServicesById(clusterId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,"Cluster not found."));
        var ids=new TreeSet<Integer>();
        cluster.getNodeIds().stream().filter(Objects::nonNull).forEach(ids::add);
        cluster.getServices().stream().map(s->s.getNodeId()).filter(Objects::nonNull).forEach(ids::add);
        // C02 proved the stored identity set equals the live broker/controller/observer set.
        int last=ids.stream().mapToInt(Integer::intValue).max().orElse(0);
        if(last==Integer.MAX_VALUE) throw new ResponseStatusException(HttpStatus.CONFLICT,"No safe broker identity is available.");
        try {
            Map<String,Object> config=mapper.readValue(Objects.toString(cluster.getConfigJson(),"{}"),new TypeReference<>(){});
            String voters=Objects.toString(config.getOrDefault("controller_quorum_voters",config.getOrDefault("quorum_voters","")),"");
            // Operation identity is a proposal, never an accepted operation or idempotency receipt.
            UUID proposedId=UUID.nameUUIDFromBytes((clusterId+":"+previewId+":"+preview.contextHash()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            var plan=BrokerAdditionPlan.freeze(preview,proposedId,last+1,voters);
            return new Review(previewId,preview.contextHash(),preview.expiresAt(),plan,List.of(
                    "Repeat final readiness under the cluster/host reservation",
                    "Reserve the host and broker identity atomically",
                    "Prepare operation-owned broker configuration and service",
                    "Format empty storage with the existing Kafka cluster identity",
                    "Start only the candidate broker",
                    "Verify unfenced registration, unchanged controller voters and stable health",
                    "Publish verified membership and release the reservation"),executionEnabled,
                    executionEnabled?"Final checks will run before any candidate changes.":"Execution is held until the collective release gates pass.");
        } catch(ResponseStatusException e) { throw e; }
        catch(Exception e) { throw new ResponseStatusException(HttpStatus.CONFLICT,
                "The inherited settings cannot form a safe broker plan: "+e.getMessage()); }
    }
}
