package io.translab.tantor.server.web;

import io.translab.tantor.server.service.*;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/clusters/{clusterId}/add-node/operations")
public class BrokerAdditionController {
    private final BrokerAdditionLedger ledger;
    private final JobService jobs;
    private final RoleAuthenticationUtil permissions;
    public record Request(UUID previewId,String contextHash) {}
    @PostMapping public ResponseEntity<?> accept(@PathVariable UUID clusterId,@RequestBody Request request,
            @RequestHeader(value="Authorization",required=false)String auth,@RequestHeader(value="Idempotency-Key",required=false)String key) {
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();
        if(request.previewId()==null)throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"A reviewed preview ID is required.");
        return ResponseEntity.accepted().body(ledger.accept(clusterId,request.previewId(),request.contextHash(),key));
    }
    @GetMapping("/{operationId}") public ResponseEntity<?> get(@PathVariable UUID clusterId,@PathVariable UUID operationId,@RequestHeader(value="Authorization",required=false)String auth) {
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();
        var op=ledger.get(clusterId,operationId);
        return ResponseEntity.ok(Map.of("operationId",op.getId(),"jobId",op.getJobId(),"state",op.getState(),"reservationHeld",op.getState().reserved(),"reason",Objects.toString(op.getLastError(),"")));
    }
    @PostMapping("/{operationId}/resume") public ResponseEntity<?> resume(@PathVariable UUID clusterId,@PathVariable UUID operationId,@RequestHeader(value="Authorization",required=false)String auth) {
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();
        var op=ledger.get(clusterId,operationId);
        if(!op.getState().reserved())throw new ResponseStatusException(HttpStatus.CONFLICT,"This broker operation is already closed.");
        return ResponseEntity.accepted().body(jobs.retryJob(op.getJobId()));
    }
    @PostMapping("/{operationId}/cancel-safe") public ResponseEntity<?> cancel(@PathVariable UUID clusterId,@PathVariable UUID operationId,@RequestHeader(value="Authorization",required=false)String auth) {
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();
        return ResponseEntity.ok(ledger.cancelSafe(clusterId,operationId));
    }
    @ExceptionHandler(ResponseStatusException.class) public ResponseEntity<?> error(ResponseStatusException error){return ResponseEntity.status(error.getStatusCode()).body(Map.of("message",Objects.toString(error.getReason(),"Broker operation could not be completed.")));}
    @ExceptionHandler(ClusterOperationConflictException.class) public ResponseEntity<?> conflict(ClusterOperationConflictException error){return ResponseEntity.status(409).body(Map.of("message",error.getMessage()));}
}
