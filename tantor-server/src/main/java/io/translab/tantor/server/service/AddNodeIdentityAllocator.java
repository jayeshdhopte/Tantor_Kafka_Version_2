package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Cluster;
import java.util.*;

/** Matches add_host/common_preview.yml: broker max + 1, with cluster-wide collision protection. */
public final class AddNodeIdentityAllocator {
    private AddNodeIdentityAllocator() {}
    public record Allocation(int nodeId,boolean available,String reason) {}
    public static Allocation broker(Cluster cluster,Set<Integer> observedIds) {
        int last=cluster.getServices().stream().filter(s->s.getRole()!=null&&s.getRole().contains("broker"))
                .map(s->s.getNodeId()).filter(Objects::nonNull).mapToInt(Integer::intValue).max().orElse(0);
        if(last==Integer.MAX_VALUE)return new Allocation(-1,false,"Broker ID space is exhausted.");
        int next=last+1;
        var used=new HashSet<Integer>(observedIds);
        if(cluster.getNodeIds()!=null)cluster.getNodeIds().stream().filter(Objects::nonNull).forEach(used::add);
        cluster.getServices().stream().map(s->s.getNodeId()).filter(Objects::nonNull).forEach(used::add);
        return used.contains(next)?new Allocation(next,false,"Sequential broker ID "+next+" is already owned by a cluster node; resolve the collision without renumbering existing nodes.")
                :new Allocation(next,true,"Next broker ID "+next+" (highest existing broker ID "+last+" + 1).");
    }
}
