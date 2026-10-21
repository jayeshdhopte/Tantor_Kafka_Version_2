package io.translab.tantor.server.web;
import io.translab.tantor.server.service.ExternalKafkaManagement;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @RequiredArgsConstructor
@RequestMapping("/api/v1/ui/external-clusters/discovery/kafka-tasks")
public class ExternalKafkaTasksController {
 private final ExternalKafkaManagement management;
 @PostMapping("/claim") public Map<String,Object> claim(@RequestParam String agentId){return management.claim(agentId);}
 @PostMapping("/{id}/complete") public void complete(@RequestParam String agentId,@PathVariable UUID id,@RequestBody Map<String,String> result){management.complete(agentId,id,result);}
}
