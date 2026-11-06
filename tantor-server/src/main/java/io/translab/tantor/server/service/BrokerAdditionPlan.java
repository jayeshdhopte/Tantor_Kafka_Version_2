package io.translab.tantor.server.service;

import io.translab.tantor.server.dto.AddNodeReadiness.Preview;
import java.util.*;
import java.util.regex.Pattern;

/** Frozen, broker-only contract. Never accepts arbitrary shell commands or Kafka properties. */
public record BrokerAdditionPlan(String schemaVersion, UUID operationId, String hostId, String contextHash,
        String kafkaClusterId, int nodeId, String hostIp, int brokerPort, String kafkaVersion,
        String runtimeUser, String javaHome, String parcelDir, String artifactChecksum,
        List<String> dataPaths, String metadataPath, String logPath, String installPath,
        String heapXmx, String quorumMode, List<String> controllerEndpoints, String quorumVoters,String brokerRack) {
    private static final Pattern PATH = Pattern.compile("/(?:opt|var|data|srv|app|usr/lib/jvm|usr/java)/[A-Za-z0-9_./-]+");
    public BrokerAdditionPlan forOperation(UUID id) {return new BrokerAdditionPlan(schemaVersion,id,hostId,contextHash,kafkaClusterId,nodeId,hostIp,brokerPort,kafkaVersion,runtimeUser,javaHome,parcelDir,artifactChecksum,dataPaths,metadataPath,logPath,installPath,heapXmx,quorumMode,controllerEndpoints,quorumVoters,brokerRack);}
    public static BrokerAdditionPlan freeze(Preview preview, UUID operationId, int nodeId, String voters) {
        if (!"PASS".equals(preview.status()) || !preview.expiresAt().isAfter(java.time.Instant.now()))
            throw new IllegalArgumentException("A fresh, complete passing readiness preview is required.");
        var required=new HashSet<String>();
        for(int i=1;i<=21;i++) required.add("C%02d".formatted(i));
        for(int i=1;i<=11;i++) required.add("B%02d".formatted(i));
        var seen=new HashSet<String>();
        if(preview.checks().stream().anyMatch(c -> !"PASS".equals(c.status()) || !seen.add(c.code())) || !seen.containsAll(required))
            throw new IllegalArgumentException("Complete, unique passing check evidence is required.");
        var p = preview.brokerPlan();
        if(!"PLAINTEXT".equals(value(p,"securityMode"))) throw new IllegalArgumentException("This broker contract currently supports inherited PLAINTEXT only.");
        var plan = new BrokerAdditionPlan("add-broker/v2", operationId, preview.hostId(), preview.contextHash(),
                value(p,"kafkaClusterId"), nodeId, value(p,"hostIp"), Integer.parseInt(value(p,"brokerPort")),
                value(p,"kafkaVersion"), value(p,"runtimeUser"), value(p,"javaHome"), value(p,"parcelDir"),
                value(p,"artifactChecksum"), strings(p,"dataPaths"), value(p,"metadataPath"), value(p,"logPath"),
                value(p,"installPath"), value(p,"heapXmx"), value(p,"quorumMode"), strings(p,"controllerEndpoints"), voters,value(p,"brokerRack"));
        plan.validate(); return plan;
    }
    public void validate() {
        if(brokerRack==null||!brokerRack.matches("[A-Za-z0-9_.-]{0,128}"))throw new IllegalArgumentException("Invalid candidate broker rack.");
        if (!("add-broker/v1".equals(schemaVersion)||"add-broker/v2".equals(schemaVersion)) || operationId == null || hostId == null || hostId.isBlank()
                || contextHash == null || !contextHash.matches("[a-f0-9]{64}") || nodeId < 1 || brokerPort < 1 || brokerPort > 65535
                || !kafkaClusterId.matches("[A-Za-z0-9_-]{22}") || !validIp(hostIp)
                || !kafkaVersion.matches("[0-9]+\\.[0-9]+\\.[0-9]+") || !artifactChecksum.matches("(?i)[a-f0-9]{64}")
                || !runtimeUser.matches("[a-z_][a-z0-9_-]*[$]?") || !heapXmx.matches("[1-9][0-9]*[mMgG]"))
            throw new IllegalArgumentException("Incomplete broker identity, artifact or runtime contract.");
        if (!Set.of("STATIC","DYNAMIC").contains(quorumMode) || controllerEndpoints.isEmpty() || controllerEndpoints.size()>128)
            throw new IllegalArgumentException("Existing controller discovery is required.");
        controllerEndpoints.forEach(BrokerAdditionPlan::endpoint);
        if ("STATIC".equals(quorumMode)) {
            if (quorumVoters == null || quorumVoters.isBlank()) throw new IllegalArgumentException("Static voter configuration is required.");
            var voterIds=new HashSet<Integer>(); var voterEndpoints=new HashSet<String>();
            for (String voter: quorumVoters.split(",")) {
                var parts = voter.split("@",-1);
                if (parts.length!=2 || !parts[0].matches("[0-9]+") || Integer.parseInt(parts[0])==nodeId)
                    throw new IllegalArgumentException("A broker must never enter controller voters.");
                endpoint(parts[1]);
                if(!voterIds.add(Integer.parseInt(parts[0])) || !voterEndpoints.add(parts[1])) throw new IllegalArgumentException("Duplicate controller voter identity or endpoint.");
            }
            if(!voterEndpoints.equals(new HashSet<>(controllerEndpoints))) throw new IllegalArgumentException("Static controller endpoints differ from the observed quorum.");
        }
        if (dataPaths.isEmpty() || dataPaths.size()>16) throw new IllegalArgumentException("Explicit persistent data paths are required.");
        var paths = new ArrayList<>(dataPaths); paths.addAll(List.of(logPath, installPath, parcelDir, javaHome));
        if (!metadataPath.isBlank()) paths.add(metadataPath);
        paths.forEach(BrokerAdditionPlan::path);
        var storage = new ArrayList<>(dataPaths); if(!metadataPath.isBlank() && !storage.contains(metadataPath)) storage.add(metadataPath);
        if(new HashSet<>(dataPaths).size()!=dataPaths.size()) throw new IllegalArgumentException("Duplicate storage paths.");
        // Never allow config, service logs or binaries to overlap a storage tree.
        for(String data:storage) for(String other:List.of(logPath,installPath,parcelDir,javaHome))
            if(overlaps(data,other)) throw new IllegalArgumentException("Storage overlaps a runtime or log path.");
        for(int i=0;i<storage.size();i++) for(int j=i+1;j<storage.size();j++)
            if(overlaps(storage.get(i),storage.get(j))) throw new IllegalArgumentException("Nested storage paths are unsupported.");
    }
    public String configFileName(){return "add-broker/v2".equals(schemaVersion)?"broker.properties":"server.properties";}
    private static boolean validIp(String ip) {
        if(ip==null || !ip.matches("[0-9a-fA-F:.]+")) return false;
        if(!ip.contains(":")) {
            var parts=ip.split("\\.",-1);
            if(parts.length!=4) return false;
            for(String part:parts) { try { if(part.isEmpty() || (part.length()>1 && part.startsWith("0")) || Integer.parseInt(part)>255 || Integer.parseInt(part)<0) return false; } catch(NumberFormatException e) {return false;} }
            return true;
        }
        try { return java.net.InetAddress.getByName(ip) instanceof java.net.Inet6Address; } catch(Exception e) { return false; }
    }
    private static boolean overlaps(String a,String b) { return a.equals(b)||a.startsWith(b+"/")||b.startsWith(a+"/"); }
    private static void path(String path) {
        if(!PATH.matcher(path).matches() || !java.nio.file.Path.of(path).normalize().toString().replace('\\','/').equals(path)
                || path.contains("/../") || path.endsWith("/")) throw new IllegalArgumentException("Unsafe broker path: "+path);
    }
    private static void endpoint(String endpoint) {
        if(!endpoint.matches("(?:[A-Za-z0-9.-]+|\\[[0-9a-fA-F:]+]):[0-9]{1,5}")) throw new IllegalArgumentException("Invalid controller endpoint.");
        int port=Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':')+1));
        if(port<1||port>65535) throw new IllegalArgumentException("Invalid controller port.");
    }
    private static String value(Map<String,Object> p,String key) { return Objects.toString(p.get(key),""); }
    private static List<String> strings(Map<String,Object> p,String key) {
        if(!(p.get(key) instanceof List<?> values) || values.stream().anyMatch(v -> !(v instanceof String)))
            throw new IllegalArgumentException("Missing list: "+key);
        return values.stream().map(String.class::cast).toList();
    }
}
