package io.translab.tantor.server.service;

import io.translab.tantor.server.config.AddNodeInspectionProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.springframework.stereotype.Service;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Controller-bootstrap observations keep static maintenance independent of stopped brokers. */
@Service @RequiredArgsConstructor
public class KafkaExpansionQuorum {
    private static final String ZERO_DIRECTORY = "AAAAAAAAAAAAAAAAAAAAAA";
    private final AddNodeInspectionProperties policy;

    public record Replica(int id, String directoryId, long logEndOffset, Long lastCaughtUpMillis) {}
    public record Snapshot(String clusterId, Map<String,Short> features, int leaderId, long highWatermark,
                           Map<Integer,Replica> voters, Map<Integer,Replica> observers, Map<Integer,List<String>> endpoints,
                           Instant observedAt) {
        public Snapshot { features=Map.copyOf(features);voters=Map.copyOf(voters);observers=Map.copyOf(observers);var copy=new TreeMap<Integer,List<String>>();endpoints.forEach((k,v)->copy.put(k,List.copyOf(v)));endpoints=Collections.unmodifiableMap(copy); }
    }
    public record Baseline(String clusterId, Map<String,Short> features, Map<Integer,String> voters, Set<Integer> knownNodeIds, boolean dynamic) {
        public Baseline {features=Map.copyOf(features);voters=Map.copyOf(voters);knownNodeIds=Set.copyOf(knownNodeIds);}
    }
    public record Candidate(int nodeId, String directoryId, String endpoint) {}
    public record Verification(boolean voter, String directoryEvidence, long lag, Instant observedAt) {}
    public static final class NotReady extends IllegalStateException { public NotReady(String message){super(message);} }

    public Snapshot observe(List<String> controllerEndpoints) {
        if(controllerEndpoints.isEmpty() || controllerEndpoints.size()>128)throw new IllegalArgumentException("Bounded controller bootstrap scope required.");
        controllerEndpoints.forEach(this::allowedEndpoint);
        Map<String,Object> config=new HashMap<>();config.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG,String.join(",",controllerEndpoints));
        config.put(AdminClientConfig.CLIENT_ID_CONFIG,"tantor-quorum-expansion");config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,15000);config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,30000);
        config.put("security.protocol","PLAINTEXT");
        try(Admin admin=Admin.create(config)){return read(admin);}
    }
    Snapshot read(Admin admin) {
        try {
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
            var cluster=admin.describeCluster();var features=admin.describeFeatures();var quorum=admin.describeMetadataQuorum();
            String id=cluster.clusterId().get(remaining(deadline),TimeUnit.NANOSECONDS);
            var levels=features.featureMetadata().get(remaining(deadline),TimeUnit.NANOSECONDS);
            var q=quorum.quorumInfo().get(remaining(deadline),TimeUnit.NANOSECONDS);
            var finalized=new TreeMap<String,Short>();levels.finalizedFeatures().forEach((name,range)->{if(range.minVersionLevel()!=range.maxVersionLevel())throw new IllegalStateException("Feature transition is in progress.");finalized.put(name,range.maxVersionLevel());});
            var endpoints=new TreeMap<Integer,List<String>>();q.nodes().forEach((node,value)->endpoints.put(node,value.endpoints().stream().map(e->hostPort(e.host(),e.port())).sorted().toList()));
            return new Snapshot(id,finalized,q.leaderId(),q.highWatermark(),replicas(q.voters()),replicas(q.observers()),endpoints,Instant.now());
        }catch(Exception e){throw new IllegalStateException("Controller quorum observation is incomplete.",e);}
    }
    public Verification caughtUp(Baseline baseline, Snapshot live, Candidate candidate, int maxLag, Instant now) {
        common(baseline,live,candidate,maxLag,now);
        if(!baseline.dynamic())throw new IllegalStateException("Static quorum cannot enter observer admission.");
        if(live.voters().containsKey(candidate.nodeId()))return member(baseline,live,candidate,maxLag,false,now);
        requireVoters(baseline,live,false,candidate.nodeId());
        var observer=live.observers().get(candidate.nodeId());
        if(observer==null)throw new NotReady("Candidate has not joined as an observer.");
        if(!candidate.directoryId().equals(observer.directoryId()))throw new IllegalStateException("Observer directory differs from formatted candidate storage.");
        long lag=lag(live,observer,maxLag);
        if(observer.lastCaughtUpMillis()==null || observer.lastCaughtUpMillis()<now.minusSeconds(60).toEpochMilli() || observer.lastCaughtUpMillis()>now.plusSeconds(5).toEpochMilli())
            throw new NotReady("Observer catch-up timestamp is unavailable or stale.");
        return new Verification(false,"OBSERVER_API_DIRECTORY",lag,live.observedAt());
    }
    public void healthyBaseline(Baseline baseline,Snapshot live,int maxLag,Instant now){
        identity(baseline,live,maxLag,now);requireVoters(baseline,live,false,-1);
        for(int observer:live.observers().keySet())if(!baseline.knownNodeIds().contains(observer))throw new IllegalStateException("An unrelated metadata observer appeared before expansion.");
        for(var voter:live.voters().values())lag(live,voter,maxLag);
    }
    public Verification member(Baseline baseline, Snapshot live, Candidate candidate, int maxLag, boolean staticConfigsVerified, Instant now) {
        common(baseline,live,candidate,maxLag,now);
        if(!live.voters().containsKey(candidate.nodeId())) {
            requireVoters(baseline,live,false,candidate.nodeId());
            throw new NotReady("Candidate is not a voting member yet.");
        }
        requireVoters(baseline,live,true,candidate.nodeId());
        var replica=live.voters().get(candidate.nodeId());
        String evidence="VOTER_API_DIRECTORY";
        if(!candidate.directoryId().equals(replica.directoryId())) {
            if(baseline.dynamic() || !ZERO_DIRECTORY.equals(replica.directoryId()) || !staticConfigsVerified)
                throw new IllegalStateException("Voter directory does not match the operation-owned candidate.");
            // Static protocols may not expose directory IDs: require independent
            // native storage and consistent expanded-config evidence, never invent an API value.
            evidence="NATIVE_STORAGE_AND_STATIC_VOTER_ID";
        }
        var endpoints=live.endpoints().get(candidate.nodeId());
        if(endpoints!=null && !endpoints.isEmpty() && !endpoints.contains(candidate.endpoint()))throw new IllegalStateException("Candidate voter endpoint changed.");
        if(baseline.dynamic() && (endpoints==null || !endpoints.contains(candidate.endpoint())))throw new IllegalStateException("Dynamic voter endpoint proof is required.");
        return new Verification(true,evidence,lag(live,replica,maxLag),live.observedAt());
    }
    private void common(Baseline baseline,Snapshot live,Candidate candidate,int maxLag,Instant now) {
        identity(baseline,live,maxLag,now);
        if(candidate.nodeId()<0 || candidate.directoryId()==null || !candidate.directoryId().matches("[A-Za-z0-9_-]{22}") || ZERO_DIRECTORY.equals(candidate.directoryId()) || baseline.knownNodeIds().contains(candidate.nodeId()))
            throw new IllegalStateException("Candidate identity collides with the frozen baseline.");
        for(int observer:live.observers().keySet())if(observer!=candidate.nodeId() && !baseline.knownNodeIds().contains(observer))throw new IllegalStateException("An unrelated metadata observer appeared during expansion.");
        for(var voter:live.voters().values())lag(live,voter,maxLag);
    }
    private void identity(Baseline baseline,Snapshot live,int maxLag,Instant now){
        if(maxLag<0 || !Objects.equals(baseline.clusterId(),live.clusterId()) || !baseline.features().equals(live.features()) || live.observedAt()==null
                || live.observedAt().isBefore(now.minusSeconds(60)) || live.observedAt().isAfter(now.plusSeconds(5)) || live.highWatermark()<0 || !live.voters().containsKey(live.leaderId()))
            throw new IllegalStateException("Quorum identity, finalized features, leader or freshness changed.");
    }
    private void requireVoters(Baseline baseline,Snapshot live,boolean expanded,int candidate) {
        var ids=new HashSet<>(baseline.voters().keySet());if(expanded)ids.add(candidate);
        if(!ids.equals(live.voters().keySet()))throw new IllegalStateException("Voter membership differs from the expected expansion stage.");
        baseline.voters().forEach((id,directory)->{if(!Objects.equals(directory,live.voters().get(id).directoryId()))throw new IllegalStateException("An existing voter directory identity changed.");});
    }
    private long lag(Snapshot live,Replica replica,int threshold) {
        var leader=live.voters().get(live.leaderId());
        if(leader==null || leader.logEndOffset()<live.highWatermark() || replica.logEndOffset()<live.highWatermark())throw new NotReady("Metadata replication is below the committed high watermark.");
        long lag=Math.max(0,leader.logEndOffset()-replica.logEndOffset());if(lag>threshold)throw new NotReady("Metadata replication lag exceeds the reviewed admission policy.");return lag;
    }
    private void allowedEndpoint(String endpoint) {
        if(endpoint==null || endpoint.contains("/") || endpoint.contains("@"))throw new IllegalArgumentException("Literal reviewed controller endpoint required.");
        int split=endpoint.lastIndexOf(':');if(split<1)throw new IllegalArgumentException("Invalid controller endpoint.");
        String host=endpoint.substring(0,split);if(host.startsWith("[")&&host.endsWith("]"))host=host.substring(1,host.length()-1);
        try {
            int port=Integer.parseInt(endpoint.substring(split+1));if(port<1||port>65535)throw new IllegalArgumentException();
            if(host.contains(":")){if(!host.matches("[A-Fa-f0-9:]+"))throw new IllegalArgumentException();}
            else {var parts=host.split("\\.",-1);if(parts.length!=4)throw new IllegalArgumentException();for(String part:parts)if(!part.matches("0|[1-9][0-9]{0,2}")||Integer.parseInt(part)>255)throw new IllegalArgumentException();}
            String canonical=literalAddress(host);
            for(String denied:policy.getExcludedHostIps())if(canonical.equals(literalAddress(denied)))throw new IllegalArgumentException("Excluded controller endpoint.");
        }catch(Exception e){throw new IllegalArgumentException("Invalid or excluded controller endpoint.");}
    }
    private static String literalAddress(String host) throws java.net.UnknownHostException {
        if(host==null)throw new IllegalArgumentException("Literal IP address required.");
        if(host.contains(":")){if(!host.matches("[A-Fa-f0-9:]+"))throw new IllegalArgumentException("Literal IPv6 address required.");}
        else {var parts=host.split("\\.",-1);if(parts.length!=4)throw new IllegalArgumentException("Literal IPv4 address required.");for(String part:parts)if(!part.matches("0|[1-9][0-9]{0,2}")||Integer.parseInt(part)>255)throw new IllegalArgumentException("Literal IPv4 address required.");}
        return java.net.InetAddress.getByName(host).getHostAddress();
    }
    private static Map<Integer,Replica> replicas(List<QuorumInfo.ReplicaState> input){var out=new TreeMap<Integer,Replica>();for(var r:input){if(out.containsKey(r.replicaId()))throw new IllegalStateException("Duplicate quorum replica.");out.put(r.replicaId(),new Replica(r.replicaId(),r.replicaDirectoryId().toString(),r.logEndOffset(),r.lastCaughtUpTimestamp().isPresent()?r.lastCaughtUpTimestamp().getAsLong():null));}return out;}
    private static long remaining(long deadline){long value=deadline-System.nanoTime();if(value<=0)throw new IllegalStateException("Quorum observation timed out.");return value;}
    private static String hostPort(String host,int port){return (host.contains(":")?"["+host+"]":host)+":"+port;}
}
