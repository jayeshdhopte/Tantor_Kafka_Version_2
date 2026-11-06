package io.translab.tantor.server.web;
import io.translab.tantor.server.service.ControllerAdditionPlanningService;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/clusters/{clusterId}/add-node/controller-plan")
public class ControllerAdditionPlanningController {
    private final ControllerAdditionPlanningService planning;
    private final RoleAuthenticationUtil permissions;
    @GetMapping public ResponseEntity<?> review(@PathVariable UUID clusterId,@RequestParam String hostId,@RequestHeader(value="Authorization",required=false)String auth){
        if(!permissions.canAccess(auth,RoleAuthenticationUtil.ADD_NODE))return ResponseEntity.status(403).build();
        return ResponseEntity.ok(planning.review(clusterId,hostId));
    }
    @ExceptionHandler(ResponseStatusException.class) public ResponseEntity<?> error(ResponseStatusException e){return ResponseEntity.status(e.getStatusCode()).body(Map.of("message",Objects.toString(e.getReason(),"Controller review unavailable.")));}
}
