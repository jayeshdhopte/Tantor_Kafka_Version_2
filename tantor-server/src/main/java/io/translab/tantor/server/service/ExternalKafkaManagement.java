package io.translab.tantor.server.service;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import java.time.*;
import java.util.*;
@Service @RequiredArgsConstructor
public class ExternalKafkaManagement {
 private final ExternalClusterRepository clusters;
 private final ExternalClusterNodeRepository nodes;
 private final DiscoveryAgentRepository agents;
 private final JdbcTemplate jdbc;
 private final ObjectMapper mapper;
 public boolean external(UUID id){return clusters.existsById(id);}
 public boolean target(String host){return host!=null&&host.startsWith("external:");}
 public boolean task(UUID id){return Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from kf_external_kafka_tasks where id=?)",Boolean.class,id));}
 public ExternalClusterNode node(String target){if(!target(target))throw new IllegalArgumentException("External node target required");return nodes.findById(UUID.fromString(target.substring(9))).orElseThrow(()->new IllegalArgumentException("Discovered node missing"));}
 public Cluster resolve(UUID id){
  var c=clusters.findById(id).orElseThrow(()->new IllegalArgumentException("External cluster not found"));
  if("DELETED".equalsIgnoreCase(c.getStatus()))throw new IllegalStateException("Cluster was deleted");
  var result=new Cluster();result.setId(id);result.setName(c.getName());result.setMode(Objects.toString(c.getKafkaMode(),"kraft"));result.setKafkaVersion(c.getKafkaVersion());result.setStatus("SUCCESS");result.setConfigJson("{}");
  var services=new ArrayList<ClusterServiceAssignment>();
  for(var n:nodes.findByClusterId(id)){
   if(n.getNodeId()==null||n.getConfigFile()==null||n.getAgentId()==null||n.getSystemdService()==null)throw new IllegalStateException("Upgrade all discovery agents and refresh discovery to report service names and active config paths");
   var s=new ClusterServiceAssignment();s.setId(n.getId());s.setHostId("external:"+n.getId());s.setNodeId(n.getNodeId());s.setCluster(result);
   s.setRole(Boolean.TRUE.equals(n.getIsBroker())?Boolean.TRUE.equals(n.getIsController())?"broker_controller":"broker":"controller");
   s.setConfigJson(json(Map.of("config_path",n.getConfigFile(),"service_name",n.getSystemdService(),"hostname",n.getHost(),"kafka_install_dir",Objects.toString(n.getInstallDir(),""))));services.add(s);
  }
  if(services.isEmpty())throw new IllegalStateException("Upgrade discovery agents and refresh discovery to report service names and active config paths");
  result.setServices(services);return result;
 }
 public void require(String target,boolean write,boolean restart){
  var n=node(target);var a=agents.findById(n.getAgentId()).orElseThrow(()->new IllegalStateException("Discovery agent missing"));
  if(!Objects.equals(a.getClusterId(),n.getClusterId())||!Boolean.TRUE.equals(a.getCanExecuteTasks())||a.getVersion()==null||!a.getVersion().startsWith("3.4.0-")||a.getLastHeartbeat()==null||a.getLastHeartbeat().isBefore(OffsetDateTime.now().minusSeconds(120))||!"ONLINE".equalsIgnoreCase(a.getStatus()))throw new IllegalStateException("Upgrade to Discovery 3.4.0 and enable tasks on "+n.getHost());
  if(!Boolean.TRUE.equals(n.getCanManageKafka()))throw new IllegalStateException("Enable bearer authentication with backend ADMIN task access on "+n.getHost());
  if(write&&!Boolean.TRUE.equals(n.getCanWriteConfig()))throw new IllegalStateException("Enable configuration writes as root on "+n.getHost());
  if(restart&&!Boolean.TRUE.equals(n.getCanRestart()))throw new IllegalStateException("Enable service restarts and allowlist the discovered service on "+n.getHost());
 }
 @Transactional public UUID enqueue(UUID cluster,String target,String command,Map<String,Object> parameters){
  boolean write=Set.of("PATCH_KAFKA_CONFIG","BACKUP_KAFKA_CONFIG").contains(command);boolean restart="RESTART_SERVICE".equals(command)||"true".equals(parameters.get("restart"));require(target,write,restart);
  var n=node(target);if(!cluster.equals(n.getClusterId()))throw new IllegalArgumentException("Node belongs to another cluster");
  if(!Set.of("READ_KAFKA_PROPERTY","READ_KAFKA_PROPERTIES","PATCH_KAFKA_CONFIG","BACKUP_KAFKA_CONFIG","RESTART_SERVICE","VERIFY_KAFKA_SERVICE").contains(command))throw new IllegalArgumentException("Unsupported external Kafka task");
  var p=new LinkedHashMap<String,Object>(parameters);p.put("kafka_cluster_id",Objects.toString(clusters.findById(cluster).orElseThrow().getKafkaClusterId(),""));
  p.put("node_id",n.getNodeId().toString());p.put("config_path",n.getConfigFile());p.put("service_name",n.getSystemdService());
  UUID id=UUID.randomUUID();jdbc.update("insert into kf_external_kafka_tasks(id,cluster_id,agent_id,command,parameters,deadline) values(?,?,?,?,?,?)",id,cluster,n.getAgentId(),command,json(p),java.sql.Timestamp.from(Instant.now().plusSeconds(900)));return id;
 }
 @Transactional public Map<String,Object> claim(String agent){
  var a=agents.findById(agent).orElseThrow();if(!Boolean.TRUE.equals(a.getCanExecuteTasks())||a.getVersion()==null||!a.getVersion().startsWith("3.4.0-"))throw new IllegalStateException("Managed tasks require Discovery 3.4.0 with tasks enabled");
  jdbc.update("update kf_external_kafka_tasks set status='FAILED',message='Task deadline exceeded; inspect node before retry' where agent_id=? and status in ('PENDING','IN_PROGRESS') and deadline<now()",agent);
  var rows=jdbc.queryForList("select id,cluster_id,command,parameters,claim_token,status from kf_external_kafka_tasks where agent_id=? and status in ('PENDING','IN_PROGRESS') and deadline>now() order by created_at limit 1 for update skip locked",agent);if(rows.isEmpty())return Map.of();
  var row=rows.get(0);if(!Objects.equals(a.getClusterId(),row.get("cluster_id"))){jdbc.update("update kf_external_kafka_tasks set status='FAILED',message='Agent no longer linked to cluster' where id=?",row.get("id"));return Map.of();}String token=Objects.toString(row.get("claim_token"),UUID.randomUUID().toString());jdbc.update("update kf_external_kafka_tasks set status='IN_PROGRESS',claim_token=? where id=?",token,row.get("id"));
  return Map.of("id",row.get("id"),"command",row.get("command"),"claimToken",token,"parameters",read(String.valueOf(row.get("parameters"))));
 }
 @Transactional public void complete(String agent,UUID id,Map<String,String> result){
  if(!Set.of("SUCCESS","FAILED").contains(Objects.toString(result.get("status"),"")))throw new IllegalArgumentException("Invalid result");
  String message=Objects.toString(result.get("message"),"");if(message.length()>131072)message=message.substring(0,131072);
  int updated=jdbc.update("update kf_external_kafka_tasks set status=?,message=? where id=? and agent_id=? and claim_token=? and status='IN_PROGRESS' and deadline>now()",result.get("status"),message,id,agent,result.get("claimToken"));
  if(updated==0)throw new IllegalStateException("Unknown, expired or already completed task claim");
 }
 public void inspectHosts(Set<String> targets,Map<String,String> failures,double diskLimit,long heartbeat){
  for(String target:targets){try{var n=node(target);var a=agents.findById(n.getAgentId()).orElseThrow();if(!"ONLINE".equalsIgnoreCase(a.getStatus())||a.getLastHeartbeat()==null||a.getLastHeartbeat().isBefore(OffsetDateTime.now().minusSeconds(heartbeat)))throw new IllegalStateException("Discovery heartbeat stale");if(n.getDiskTotalGb()!=null&&n.getDiskUsedGb()!=null&&n.getDiskTotalGb()>0&&100.0*n.getDiskUsedGb()/n.getDiskTotalGb()>=diskLimit)failures.put("disk:"+target,"Disk pressure on "+n.getHost());}catch(RuntimeException e){failures.put("agent:"+target,e.getMessage());}}
 }
 public Map<String,Object> read(String s){try{return mapper.readValue(s,new TypeReference<>(){});}catch(Exception e){throw new IllegalArgumentException("Invalid management payload",e);}}
 public String json(Object s){try{return mapper.writeValueAsString(s);}catch(Exception e){throw new IllegalArgumentException(e);}}
}
