package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.time.*;
import java.util.*;

@Service @RequiredArgsConstructor
public class ExternalDataServiceManager {
 private final ExternalClusterRepository clusters;
 private final DiscoveryAgentRepository agents;
 private final ExternalDataServiceRepository services;
 private final ExternalServiceTaskRepository tasks;
 private final JobService jobs;
 private final ExternalServiceArtifacts artifacts;
 private final ObjectMapper mapper;
 static final Set<String> ROLES=Set.of("schema_registry","kafka_connect");
 static String value(Map<String,?> p,String k,String fallback) { Object v=p.get(k); return v==null||v.toString().isBlank()?fallback:v.toString().trim(); }
 static String blocked(DiscoveryAgent a) {
  if(a.getLastHeartbeat()==null || a.getLastHeartbeat().isBefore(OffsetDateTime.now().minusSeconds(120)) || !"ONLINE".equalsIgnoreCase(a.getStatus())) return "Discovery agent is offline or its heartbeat is stale.";
  if(!Boolean.TRUE.equals(a.getCanDeployServices()) || a.getVersion()==null || !a.getVersion().startsWith("3.3.0-dataservices.")) return "Upgrade to Discovery 3.3.0-dataservices and enable service deployment as root.";
  return "";
 }
 ExternalCluster requireCluster(UUID id) {
  ExternalCluster c=clusters.findById(id).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"External cluster not found"));
  if("DELETED".equalsIgnoreCase(c.getStatus())) throw new IllegalArgumentException("Cluster has been deleted.");
  return c;
 }
 DiscoveryAgent requireAgent(UUID cluster,String id) {
  DiscoveryAgent a=agents.findById(id).orElseThrow(()->new IllegalArgumentException("Discovery agent not found."));
  if(!cluster.equals(a.getClusterId())) throw new IllegalArgumentException("Agent is not linked to this external cluster.");
  if(!blocked(a).isEmpty()) throw new IllegalArgumentException(blocked(a));
  return a;
 }
 public Map<String,Object> options(UUID id) {
  ExternalCluster c=requireCluster(id);
  List<Map<String,Object>> hosts=new ArrayList<>();
  for(var a:agents.findByClusterId(id)) hosts.add(Map.of("hostId",a.getId(),"hostname",Objects.toString(a.getHostname(),a.getId()),"canDeployServices",blocked(a).isEmpty(),"reason",blocked(a)));
  var existing=services.findByClusterId(id);
  String protocol=Objects.toString(c.getSecurityProtocol(),Objects.toString(c.getSecurity(),"PLAINTEXT"));
  return Map.of("hosts",hosts,"serviceRoles",existing.stream().map(ExternalDataService::getRole).toList(),"services",existing,
    "deploymentWarning",!protocol.isBlank()&&!"PLAINTEXT".equalsIgnoreCase(protocol)?"Service deployment currently supports PLAINTEXT Kafka clusters only.":"");
 }
 @Transactional
 public Map<String,String> create(UUID id,Map<String,Map<String,Object>> request,String username) {
  ExternalCluster c=clusters.findForServiceUpdate(id).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND));
  requireCluster(id);
  if(request==null || request.isEmpty() || !ROLES.containsAll(request.keySet())) throw new IllegalArgumentException("Select Schema Registry and/or Kafka Connect.");
  String protocol=Objects.toString(c.getSecurityProtocol(),Objects.toString(c.getSecurity(),"PLAINTEXT"));
  if(!protocol.isBlank()&&!"PLAINTEXT".equalsIgnoreCase(protocol)) throw new IllegalArgumentException("Service deployment supports PLAINTEXT Kafka only; existing security settings will not be changed.");
  if(c.getBootstrapServers()==null||c.getBootstrapServers().isBlank()) throw new IllegalArgumentException("Cluster has no bootstrap servers.");
  List<ExternalDataService> additions=new ArrayList<>();
  for(String role:List.of("schema_registry","kafka_connect")) {
   if(!request.containsKey(role)) continue;
   if(services.existsByClusterIdAndRole(id,role)) throw new ResponseStatusException(HttpStatus.CONFLICT,"Service already added; inspect or retry its existing job.");
   Map<String,Object> input=request.get(role);
   if(input==null) throw new IllegalArgumentException("Missing service configuration.");
   DiscoveryAgent agent=requireAgent(id,value(input,"host_id",""));
   Map<String,String> p=configuration(role,input);
   p.put("host_name",Objects.toString(agent.getHostname(),""));
   if(!p.get("host_name").matches("[a-zA-Z0-9.:-]+")) throw new IllegalArgumentException("Agent must advertise a valid reachable hostname.");
   p.put("bootstrap_servers",c.getBootstrapServers());
   p.put("kafka_cluster_id",Objects.toString(c.getKafkaClusterId(),""));
   p.put("replication_factor",String.valueOf(Math.max(1,Math.min(3,Objects.requireNonNullElse(c.getBrokerCount(),1)))));
   UUID artifact=UUID.fromString(value(input,"artifact_id",""));
   p.put("artifact_id",artifact.toString()); p.put("checksum",artifacts.verifiedChecksum(artifact,role));
   List<ExternalDataService> neighbours=new ArrayList<>(services.findByAgentId(agent.getId())); neighbours.addAll(additions);
   for(var other:neighbours) if(agent.getId().equals(other.getAgentId())) {
    var op=read(other.getConfigJson());
    if(p.get("rest_port").equals(op.get("rest_port"))) throw new IllegalArgumentException("REST port conflicts with another service on this host.");
    for(String a:paths(p)) for(String b:paths(op)) if(overlaps(a,b)) throw new IllegalArgumentException("Service directories overlap another service on this host.");
   }
   ExternalDataService s=new ExternalDataService(); s.setClusterId(id); s.setAgentId(agent.getId()); s.setRole(role); s.setStatus("PENDING"); s.setConfigJson(json(p)); additions.add(s);
  }
  List<JobStep> steps=new ArrayList<>();
  for(var s:additions) { services.saveAndFlush(s); for(String op:List.of("PRECHECK","TOPICS","INSTALL","SAVE")) {
   JobStep step=new JobStep(); step.setStepOrder(steps.size()+1); step.setTargetId(s.getAgentId()); step.setName(s.getRole()+": "+op); step.setPayload(json(Map.of("serviceId",s.getId().toString(),"operation",op))); steps.add(step);
  } }
  Job job=new Job(); job.setType(JobType.EXTERNAL_SERVICES); job.setStatus(JobStatus.PENDING); job.setRollbackSupported(false); job.setRequestedBy(username); job.setResourceKey("external-services:"+id); job.setPayload(json(Map.of("clusterId",id.toString())));
  Job saved=jobs.createJob(job,steps);
  return Map.of("id",id.toString(),"jobId",saved.getId().toString());
 }
 static List<String> paths(Map<String,String> p) { return List.of("install_dir","config_dir","log_dir","working_dir","plugin_dir").stream().map(p::get).filter(Objects::nonNull).filter(v->!v.isBlank()).toList(); }
 static boolean overlaps(String a,String b) { return a.equals(b)||a.startsWith(b+"/")||b.startsWith(a+"/"); }
 static Map<String,String> configuration(String role,Map<String,Object> in) {
  String kind=role.equals("schema_registry")?"schema-registry":"kafka-connect";
  Map<String,String> p=new LinkedHashMap<>();
  p.put("install_dir",value(in,"install_dir","/opt/tantor/"+kind));
  p.put("config_dir",value(in,"config_dir",p.get("install_dir")+(role.equals("schema_registry")?"/etc/schema-registry":"/config")));
  p.put("log_dir",value(in,"log_dir","/var/log/tantor/"+kind)); p.put("working_dir",value(in,"working_dir","/var/lib/tantor/"+kind));
  if(role.equals("kafka_connect")) p.put("plugin_dir",value(in,"plugin_dir",p.get("install_dir")+"/plugins"));
  for(String path:paths(p)) if(!path.matches("/[a-zA-Z0-9_./-]+")||path.contains("//")||path.endsWith("/")||Arrays.asList(path.split("/")).contains("..")||Arrays.asList(path.split("/")).contains(".")) throw new IllegalArgumentException("Use clean absolute service paths without spaces or traversal.");
  int port=Integer.parseInt(value(in,"port",role.equals("schema_registry")?"8081":"8083"));
  if(port<1||port>65535) throw new IllegalArgumentException("Invalid REST port."); p.put("rest_port",String.valueOf(port));
  String heap=value(in,"heap_size","1G"); if(!heap.matches("[1-9][0-9]*[mMgG]")) throw new IllegalArgumentException("Invalid heap size."); p.put("heap_size",heap);
  String group=value(in,"group_id","tantor-"+kind); if(!group.matches("[a-zA-Z0-9._-]{1,200}")||Set.of(".","..").contains(group)) throw new IllegalArgumentException("Invalid service group ID."); p.put("group_id",group);
  String compatibility=value(in,"compatibility_level","BACKWARD").toUpperCase(Locale.ROOT);
  if(!Set.of("BACKWARD","BACKWARD_TRANSITIVE","FORWARD","FORWARD_TRANSITIVE","FULL","FULL_TRANSITIVE","NONE").contains(compatibility)) throw new IllegalArgumentException("Invalid schema compatibility.");
  p.put("compatibility_level",compatibility); p.put("service_user","root"); p.put("service_group","root"); return p;
 }
 public Map<String,String> read(String data) { try { return mapper.readValue(data,new TypeReference<>(){}); } catch(Exception e) { throw new IllegalStateException("Invalid persisted configuration",e); } }
 public String json(Object data) { try { return mapper.writeValueAsString(data); } catch(Exception e) { throw new IllegalStateException(e); } }
 @Transactional
 public ExternalServiceTask enqueue(ExternalDataService service,String command) {
  requireAgent(service.getClusterId(),service.getAgentId());
  ExternalServiceTask t=new ExternalServiceTask(); t.setServiceId(service.getId()); t.setAgentId(service.getAgentId()); t.setCommand(command); return tasks.saveAndFlush(t);
 }
 @Transactional
 public Map<String,Object> claim(String agentId) {
  var agent=agents.findById(agentId).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND));
  if(!blocked(agent).isEmpty()) throw new IllegalArgumentException(blocked(agent));
  var optional=tasks.findFirstByAgentIdAndStatusInOrderByCreatedAtAsc(agentId,List.of("PENDING","IN_PROGRESS"));
  if(optional.isEmpty()) return Map.of();
  var t=optional.get();
  if(t.getDeadline().isBefore(Instant.now())) { t.setStatus("FAILED"); t.setMessage("Agent task timed out; inspect the host before retrying."); tasks.save(t); return Map.of(); }
  var s=services.findById(t.getServiceId()).orElseThrow(); requireCluster(s.getClusterId()); requireAgent(s.getClusterId(),agentId);
  t.setStatus("IN_PROGRESS"); tasks.save(t);
  var p=read(s.getConfigJson());
  return Map.of("id",t.getId(),"command",t.getCommand(),"kind",s.getRole().equals("schema_registry")?"schema-registry":"kafka-connect","parameters",p,"artifactId",p.get("artifact_id"),"checksum",p.get("checksum"));
 }
 @Transactional
 public void complete(String agentId,UUID id,Map<String,String> result) {
  var t=tasks.findForUpdate(id).orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND));
  if(!t.getAgentId().equals(agentId)) throw new ResponseStatusException(HttpStatus.FORBIDDEN);
  if(!"IN_PROGRESS".equals(t.getStatus())) return;
  String status=result.get("status"); if(!Set.of("SUCCESS","FAILED").contains(Objects.toString(status,""))) throw new IllegalArgumentException("Invalid task result.");
  if(t.getDeadline().isBefore(Instant.now())) { t.setStatus("FAILED"); t.setMessage("Late completion; inspect service state before retrying."); }
  else { t.setStatus(status); String message=Objects.toString(result.get("message"),""); t.setMessage(message.substring(0,Math.min(message.length(),131072))); }
  tasks.save(t);
 }
 @Transactional
 public void expire(UUID id) { var t=tasks.findForUpdate(id).orElseThrow(); if(Set.of("PENDING","IN_PROGRESS").contains(t.getStatus())) {t.setStatus("FAILED");t.setMessage("Agent task timed out; inspect host before retrying.");tasks.save(t);} }
}
