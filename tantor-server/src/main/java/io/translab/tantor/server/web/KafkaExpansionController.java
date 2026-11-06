package io.translab.tantor.server.web;

import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/clusters/{clusterId}/add-node/expansions")
public class KafkaExpansionController {
    private final KafkaExpansionPreviewService previews;
    private final KafkaExpansionLedger ledger;
    private final JobService jobs;
    private final RoleAuthenticationUtil permissions;
    public record Submit(UUID previewId,String contextHash) {}
    @PostMapping("/previews") public ResponseEntity<?> preview(@PathVariable UUID clusterId,@RequestBody KafkaExpansionPlanCollector.Request request,@RequestHeader(value="Authorization",required=false)String auth){if(!allowed(auth))return ResponseEntity.status(403).build();return ResponseEntity.accepted().body(previews.start(clusterId,request));}
    @GetMapping("/previews/{previewId}") public ResponseEntity<?> review(@PathVariable UUID clusterId,@PathVariable UUID previewId,@RequestHeader(value="Authorization",required=false)String auth){if(!allowed(auth))return ResponseEntity.status(403).build();return ResponseEntity.ok(previews.review(clusterId,previewId));}
    @PostMapping public ResponseEntity<?> submit(@PathVariable UUID clusterId,@RequestBody Submit request,@RequestHeader(value="Authorization",required=false)String auth,@RequestHeader(value="Idempotency-Key",required=false)String key){if(!allowed(auth))return ResponseEntity.status(403).build();if(request.previewId()==null||request.contextHash()==null||!request.contextHash().matches("[a-f0-9]{64}"))return ResponseEntity.badRequest().body(Map.of("message","A reviewed preview ID and context are required."));String digest=BrokerAdditionLedger.hash(request.previewId()+":"+request.contextHash());var prior=ledger.acceptedRequest(clusterId,key,digest);if(prior.isPresent())return ResponseEntity.accepted().body(prior.get());return ResponseEntity.accepted().body(ledger.accept(previews.requireReady(clusterId,request.previewId(),request.contextHash()),key,digest));}
    @GetMapping("/{operationId}") public ResponseEntity<?> get(@PathVariable UUID clusterId,@PathVariable UUID operationId,@RequestHeader(value="Authorization",required=false)String auth){if(!allowed(auth))return ResponseEntity.status(403).build();var op=ledger.get(clusterId,operationId);return ResponseEntity.ok(Map.of("operationId",op.getId(),"jobId",op.getJobId(),"state",op.getState(),"reservationHeld",op.getState().reserved(),"checkpoints",ledger.checkpoints(op).snapshot(),"reason",Objects.toString(op.getLastError(),"")));}
    @PostMapping("/{operationId}/resume") public ResponseEntity<?> resume(@PathVariable UUID clusterId,@PathVariable UUID operationId,@RequestHeader(value="Authorization",required=false)String auth){if(!allowed(auth))return ResponseEntity.status(403).build();var op=ledger.get(clusterId,operationId);if(op.getState()==KafkaExpansionState.CANCELLED_SAFE)return ResponseEntity.status(409).body(Map.of("message","This expansion was safely cancelled."));return ResponseEntity.accepted().body(jobs.retryJob(op.getJobId()));}
    @PostMapping("/{operationId}/cancel-safe") public ResponseEntity<?> cancel(@PathVariable UUID clusterId,@PathVariable UUID operationId,@RequestHeader(value="Authorization",required=false)String auth){if(!allowed(auth))return ResponseEntity.status(403).build();return ResponseEntity.ok(ledger.cancelSafe(clusterId,operationId));}
    private boolean allowed(String auth){return permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE);}
    @ExceptionHandler(ResponseStatusException.class) public ResponseEntity<?> error(ResponseStatusException error){return ResponseEntity.status(error.getStatusCode()).body(Map.of("message",Objects.toString(error.getReason(),"Expansion request could not be completed.")));}
    @ExceptionHandler(ClusterOperationConflictException.class) public ResponseEntity<?> conflict(ClusterOperationConflictException error){return ResponseEntity.status(409).body(Map.of("message",error.getMessage()));}
}
