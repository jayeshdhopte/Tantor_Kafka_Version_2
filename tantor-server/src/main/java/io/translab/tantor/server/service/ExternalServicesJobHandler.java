package io.translab.tantor.server.service;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import io.translab.tantor.server.dto.SaveConnectionRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component @RequiredArgsConstructor
public class ExternalServicesJobHandler implements JobHandler {
 private final JobService jobs;
 private final ExternalDataServiceManager manager;
 private final ExternalDataServiceRepository services;
 private final ExternalServiceTaskRepository tasks;
 private final KafkaAdminService kafka;
 private final DataServiceConnectionService connections;
 public boolean supports(JobType type) { return type==JobType.EXTERNAL_SERVICES; }
 public void execute(Job job) {
  for(JobStep step:jobs.getSteps(job.getId())) {
   if(step.getStatus()==JobStepStatus.SUCCESS) continue;
   var payload=manager.read(step.getPayload());
   var s=services.findById(UUID.fromString(payload.get("serviceId"))).orElseThrow();
   jobs.startStep(step.getId());
   try {
    manager.requireCluster(s.getClusterId()); manager.requireAgent(s.getClusterId(),s.getAgentId());
    s.setStatus("DEPLOYING"); s.setLastError(null); services.save(s);
    var p=manager.read(s.getConfigJson()); String op=payload.get("operation"); String output;
    switch(op) {
     case "PRECHECK", "INSTALL" -> {
      UUID taskId=step.getExternalServiceTaskId();
      if(taskId==null) { var t=manager.enqueue(s,op); taskId=t.getId(); jobs.attachExternalServiceTask(step.getId(),taskId); }
      output=await(taskId);
     }
     case "TOPICS" -> { ensureTopics(s,p); output="Kafka internal topics created or verified; existing broker configuration unchanged."; }
     case "SAVE" -> {
      var req=new SaveConnectionRequest(); req.setConnectionName("Deployed "+s.getRole()); req.setProtocol("http"); req.setHost(p.get("host_name")); req.setPort(Integer.parseInt(p.get("rest_port"))); req.setIsDefault(true);
      var saved=connections.saveConnection(s.getClusterId(),s.getRole().toUpperCase(Locale.ROOT),req,job.getRequestedBy());
      if(!"ONLINE".equals(saved.getStatus())) throw new IllegalStateException("Service started but management-server REST verification failed: "+saved.getLastError());
      s.setStatus("ONLINE"); services.save(s); output="Service verified and connection associated with the external cluster.";
     }
     default -> throw new IllegalArgumentException("Unsupported external service operation");
    }
    jobs.completeStep(step.getId(),output);
   } catch(Exception e) {
    if(e instanceof InterruptedException) Thread.currentThread().interrupt();
    s.setStatus("FAILED"); s.setLastError(e.getMessage()); services.save(s); jobs.failStep(step.getId(),e.getMessage());
    throw new IllegalStateException("External service deployment failed: "+e.getMessage(),e);
   }
  }
 }
 private String await(UUID id) throws InterruptedException {
  while(true) {
   var t=tasks.findById(id).orElseThrow();
   if("SUCCESS".equals(t.getStatus())) return Objects.toString(t.getMessage(),"");
   if("FAILED".equals(t.getStatus())) throw new IllegalStateException(t.getMessage());
   if(Instant.now().isAfter(t.getDeadline())) { manager.expire(id); throw new IllegalStateException("Discovery agent task timed out; inspect the host before retrying."); }
   Thread.sleep(1000);
  }
 }
 private void ensureTopics(ExternalDataService s,Map<String,String> p) throws Exception {
  var admin=kafka.getAdminClient(s.getClusterId());
  int rf=Integer.parseInt(p.get("replication_factor"));
  if(s.getRole().equals("schema_registry")) ensureTopic(admin,"_schemas",1,rf);
  else { ensureTopic(admin,"connect-configs",1,rf); ensureTopic(admin,"connect-offsets",25,rf); ensureTopic(admin,"connect-status",5,rf); }
 }
 static void ensureTopic(AdminClient admin,String name,int partitions,int rf) throws Exception {
  var existing=admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get(60,TimeUnit.SECONDS);
  if(!existing.contains(name)) {
   try { admin.createTopics(List.of(new NewTopic(name,partitions,(short)rf).configs(Map.of("cleanup.policy","compact")))).all().get(60,TimeUnit.SECONDS); }
   catch(java.util.concurrent.ExecutionException e) { if(!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw e; }
  }
  var description=admin.describeTopics(List.of(name)).allTopicNames().get(60,TimeUnit.SECONDS).get(name);
  if(description.partitions().size()!=partitions || description.partitions().stream().anyMatch(p->p.replicas().size()!=rf)) throw new IllegalStateException("Existing internal topic "+name+" has incompatible partitions or replication; it will not be altered.");
  var resource=new ConfigResource(ConfigResource.Type.TOPIC,name);
  var entry=admin.describeConfigs(List.of(resource)).all().get(60,TimeUnit.SECONDS).get(resource).get("cleanup.policy");
  if(entry==null||!"compact".equals(entry.value())) throw new IllegalStateException("Internal topic "+name+" must use cleanup.policy=compact.");
 }
}
