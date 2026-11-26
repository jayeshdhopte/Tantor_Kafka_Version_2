package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.OffsetDateTime;
import java.util.*;

/** External JDK receipts use the existing external task journal, never internal host foreign keys. */
@Service @RequiredArgsConstructor
public class ExternalJavaUpgradeService {
 private final ExternalKafkaManagement management;
 private final ExternalClusterRepository clusters;
 private final ExternalClusterNodeRepository nodes;
 private final AddNodeArtifactCatalog catalog;
 private final JdbcTemplate jdbc;
 private final JobService jobs;

 public boolean external(UUID id) { return management.external(id); }
 public Cluster cluster(UUID id) {
  ExternalCluster c=clusters.findById(id).orElseThrow(()->new IllegalArgumentException("External cluster not found."));
  if("DELETED".equalsIgnoreCase(c.getStatus()) || !"kraft".equalsIgnoreCase(c.getKafkaMode()) || c.getKafkaClusterId()==null || c.getKafkaClusterId().isBlank()) throw new IllegalArgumentException("A discovered KRaft cluster with a verified Kafka identity is required.");
  if(!"PLAINTEXT".equalsIgnoreCase(Objects.toString(c.getSecurityProtocol(),Objects.toString(c.getSecurity(),"PLAINTEXT")))) throw new IllegalArgumentException("Java upgrade currently requires PLAINTEXT Kafka health probes.");
  return management.resolve(id);
 }
 public ExternalClusterNode requireTarget(UUID cluster,String target,boolean restart) {
  var n=management.node(target);
  if(!cluster.equals(n.getClusterId())) throw new IllegalArgumentException("External host belongs to another cluster.");
  management.require(target,true,restart);
  if(!Objects.toString(management.agentVersion(target),"").contains(".java-upgrade.1")) throw new IllegalArgumentException("Install the Java-upgrade capable discovery agent first.");
  var peers=nodes.findByAgentId(n.getAgentId());
  if(peers.size()!=1) {
   boolean valid=peers.size()==2 && peers.stream().allMatch(peer->cluster.equals(peer.getClusterId()) && Objects.equals(n.getHost(),peer.getHost()) && peer.getSystemdService()!=null && peer.getConfigFile()!=null)
    && peers.stream().filter(peer->Boolean.TRUE.equals(peer.getIsBroker())&&!Boolean.TRUE.equals(peer.getIsController())).count()==1
    && peers.stream().filter(peer->Boolean.TRUE.equals(peer.getIsController())&&!Boolean.TRUE.equals(peer.getIsBroker())).count()==1
    && peers.stream().map(ExternalClusterNode::getSystemdService).distinct().count()==2
    && peers.stream().map(ExternalClusterNode::getConfigFile).distinct().count()==2;
   if(!valid) throw new IllegalArgumentException("VM-wide Java upgrade requires one Kafka process or a separate broker/controller pair from the same cluster.");
   if(!Objects.toString(management.agentVersion(target), "").contains(".java-separate.1")) throw new IllegalArgumentException("Install the separate-service Java upgrade agent first.");
   for(var peer:peers) management.require("external:"+peer.getId(),true,restart);
  }
  return n;
 }
 public void validateTargets(UUID cluster,List<String> targets) {
  var agents=new HashSet<String>();
  for(String target:targets) if(!agents.add(requireTarget(cluster,target,true).getAgentId())) throw new IllegalArgumentException("Select each external VM only once.");
 }
 public List<Map<String,Object>> hosts(UUID cluster) {
  List<Map<String,Object>> result=new ArrayList<>();
  Set<String> seenAgents=new HashSet<>();
  for(var n:cluster==null?nodes.findAll():nodes.findByClusterId(cluster)) {
   String target="external:"+n.getId();
   try {
    cluster(n.getClusterId()); requireTarget(n.getClusterId(),target,true);
    if(!seenAgents.add(n.getAgentId())) continue;
    result.add(Map.of("id",target,"hostId",target,"hostname",n.getHost(),"ipAddress",n.getHost(),"status","ONLINE","agentStatus","ONLINE","originType","EXTERNAL"));
   } catch(IllegalStateException|IllegalArgumentException ignored) { /* Ineligible agents cannot receive packages. */ }
  }
  return result;
 }
 public List<HostParcel> states() {
  return jdbc.query("""
    select distinct on (parameters::jsonb->>'target_id', parameters::jsonb->>'artifact_id')
      id, command, status, message, parameters, created_at, deadline
    from kf_external_kafka_tasks where command in ('STAGE_JAVA','REMOVE_JAVA')
    order by parameters::jsonb->>'target_id', parameters::jsonb->>'artifact_id', created_at desc, id desc
    """, (rs,index)-> {
      var p=management.read(rs.getString("parameters")); var state=new HostParcel();
      state.setId(rs.getObject("id",UUID.class)); state.setLastTaskId(state.getId());
      state.setHostId(String.valueOf(p.get("target_id"))); state.setArtifactId(UUID.fromString(String.valueOf(p.get("artifact_id"))));
      state.setServiceType("JAVA"); state.setVersion(String.valueOf(p.get("target_version"))); state.setChecksum(String.valueOf(p.get("checksum")));
      state.setParcelDir(String.valueOf(p.get("parcel_dir"))); state.setFileName(Objects.toString(p.get("file_name"),"")); state.setHostIp(Objects.toString(p.get("target_hostname"),""));
      String command=rs.getString("command"),status=rs.getString("status");
      if(Set.of("PENDING","IN_PROGRESS").contains(status) && rs.getObject("deadline",OffsetDateTime.class).isBefore(OffsetDateTime.now())) status="FAILED";
      state.setStatus("SUCCESS".equals(status)?("STAGE_JAVA".equals(command)?"DISTRIBUTED":"REMOVED"):"FAILED".equals(status)?"FAILED":"STAGE_JAVA".equals(command)?"DISTRIBUTING":"REMOVING");
      state.setAction("STAGE_JAVA".equals(command)?"DISTRIBUTE":"REMOVE"); state.setErrorMsg("FAILED".equals(status)?rs.getString("message"):null);
      state.setCreatedAt(rs.getObject("created_at",OffsetDateTime.class));state.setUpdatedAt(state.getCreatedAt());return state;
    });
 }
 @Transactional public List<HostParcel> distribute(UUID artifact,ParcelService.ParcelActionRequest request) { return parcelAction("STAGE_JAVA",artifact,request); }
 @Transactional public List<HostParcel> remove(UUID artifact,ParcelService.ParcelActionRequest request) { return parcelAction("REMOVE_JAVA",artifact,request); }
 private List<HostParcel> parcelAction(String command,UUID artifact,ParcelService.ParcelActionRequest request) {
  if(!"JAVA".equalsIgnoreCase(request.getServiceType())) throw new IllegalArgumentException("External distribution supports Java JDK packages only.");
  var approved=catalog.resolveType("JAVA",request.getVersion(),artifact.toString());
  List<HostParcel> result=new ArrayList<>();
  for(String target:new LinkedHashSet<>(request.getHostIds())) {
   var n=management.node(target);cluster(n.getClusterId());jobs.assertClusterMutable(n.getClusterId());requireTarget(n.getClusterId(),target,false);
   var previous=states().stream().filter(p->target.equals(p.getHostId())&&artifact.equals(p.getArtifactId())).findFirst();
   if(previous.isPresent()&&Set.of("DISTRIBUTING","REMOVING").contains(previous.get().getStatus())) throw new IllegalArgumentException("A JDK distribution task is already pending on this host.");
   if(command.equals("REMOVE_JAVA")&&previous.isEmpty())throw new IllegalArgumentException("No staged JDK exists for this host.");
   String directory=(request.getParcelDirs()==null?Map.<String,String>of():request.getParcelDirs()).getOrDefault(target,request.getParcelDir());
   if(command.equals("REMOVE_JAVA"))directory=previous.orElseThrow().getParcelDir();
   if(directory==null||directory.isBlank())directory="/srv/apps/tantor/parcels";
   if(!directory.matches("/(opt|var|data|srv|app|usr/lib/jvm|usr/java)(/[A-Za-z0-9_.+-]+)+")||directory.contains("/../"))throw new IllegalArgumentException("An approved absolute parcel directory is required.");
   var p=new LinkedHashMap<String,Object>();p.put("artifact_id",artifact.toString());p.put("target_version",approved.version());p.put("checksum",approved.checksum());p.put("parcel_dir",directory);p.put("target_id",target);p.put("target_hostname",n.getHost());p.put("file_name",Objects.toString(request.getFileName(),""));
   UUID task=management.enqueue(n.getClusterId(),target,command,p);
   var state=new HostParcel();state.setId(task);state.setLastTaskId(task);state.setHostId(target);state.setArtifactId(artifact);state.setServiceType("JAVA");state.setVersion(approved.version());state.setStatus(command.equals("STAGE_JAVA")?"DISTRIBUTING":"REMOVING");result.add(state);
  }
  return result;
 }
 public Map<String,Object> prepare(UUID cluster,String target,UUID artifact,String version) {
  cluster(cluster);var n=requireTarget(cluster,target,true);
  var approved=catalog.resolveType("JAVA",version,artifact.toString());
  var state=states().stream().filter(p->target.equals(p.getHostId())&&artifact.equals(p.getArtifactId())&&"DISTRIBUTED".equals(p.getStatus())&&version.equals(p.getVersion())&&approved.checksum().equalsIgnoreCase(p.getChecksum())).findFirst().orElseThrow(()->new IllegalArgumentException("Distribute the selected JDK to this external host first."));
  var receipt=jdbc.queryForMap("select parameters from kf_external_kafka_tasks where id=?",state.getLastTaskId());
  var p=new LinkedHashMap<>(management.read(String.valueOf(receipt.get("parameters"))));
  if(!Objects.equals(p.get("kafka_cluster_id"),clusters.findById(cluster).orElseThrow().getKafkaClusterId())||!Objects.equals(p.get("agent_id"),n.getAgentId())||!Objects.equals(p.get("service_name"),n.getSystemdService())||!Objects.equals(p.get("config_path"),n.getConfigFile())||!Objects.equals(String.valueOf(p.get("node_id")),String.valueOf(n.getNodeId())))throw new IllegalArgumentException("External service identity changed since JDK distribution; refresh and distribute again.");
  return p;
 }
 public UUID enqueue(UUID cluster,String target,UUID artifact,String version) {return management.enqueue(cluster,target,"JAVA_UPGRADE",prepare(cluster,target,artifact,version));}
}
