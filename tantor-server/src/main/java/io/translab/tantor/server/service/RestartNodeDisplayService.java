package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.JobStep;
import io.translab.tantor.server.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import java.net.InetAddress;
import java.util.*;

/** Display metadata only; host/target IDs used to dispatch tasks never change. */
@Service @RequiredArgsConstructor
public class RestartNodeDisplayService {
    private final HostRepository hosts;
    private final ExternalClusterNodeRepository nodes;
    private final DiscoveryAgentRepository agents;
    private final ObjectMapper mapper;

    public Map<String,String> identity(String target) {
        String hostname="",ip="";
        if (target!=null && target.startsWith("external:")) {
            try {
                var node=nodes.findById(UUID.fromString(target.substring(9))).orElse(null);
                if(node!=null) {
                    ip=literalIp(node.getHost());
                    hostname=ip.isEmpty()?Objects.toString(node.getHost(),""):"";
                    var agent=node.getAgentId()==null?null:agents.findById(node.getAgentId()).orElse(null);
                    if(agent!=null && Objects.equals(agent.getClusterId(),node.getClusterId())) {
                        if(agent.getHostname()!=null && !agent.getHostname().isBlank()) hostname=agent.getHostname();
                        if(ip.isEmpty()) ip=firstIp(agent.getIpAddresses());
                    }
                }
            } catch(IllegalArgumentException ignored) { /* deleted/legacy node: explicit unavailable labels */ }
        } else if(target!=null) {
            var host=hosts.findById(target).orElse(null);
            if(host!=null) {hostname=Objects.toString(host.getHostname(),"");ip=literalIp(host.getHostIp());}
        }
        return Map.of("hostname",hostname.isBlank()?"Hostname not reported":hostname,
                "ipAddress",ip.isBlank()?"IP not reported":ip);
    }

    private String firstIp(String value) {
        if(value==null) return "";
        try {
            for(String candidate:mapper.readValue(value,new TypeReference<List<String>>(){})) {
                String ip=literalIp(candidate);if(!ip.isEmpty()) return ip;
            }
        } catch(Exception ignored) {return literalIp(value);}
        return "";
    }
    private String literalIp(String value) {
        if(value==null) return "";
        String candidate=value.trim().replaceAll("^\\[|\\]$","");
        if(!candidate.matches("(?:[0-9]{1,3}\\.){3}[0-9]{1,3}|[0-9a-fA-F]*:[0-9a-fA-F:]+(?:%[a-zA-Z0-9]+)?")) return "";
        if(!candidate.contains(":")) for(String octet:candidate.split("\\.")) if(Integer.parseInt(octet)>255)return "";
        try {
            var ip=InetAddress.getByName(candidate); // numeric input only: never perform DNS lookups
            return ip.isLoopbackAddress()||ip.isAnyLocalAddress()||ip.isLinkLocalAddress()||ip.isMulticastAddress()?"":ip.getHostAddress();
        } catch(Exception ignored) {return "";}
    }
    public static String label(Map<String,?> payload) {
        return String.valueOf(payload.get("hostname"))+" ("+payload.get("ipAddress")+")";
    }
    public static String stepName(Map<String,?> payload) {
        return "Restart "+String.valueOf(payload.get("role")).replace('_','/')+" node "+payload.get("nodeId")+" - "+label(payload);
    }

    // Enrich old history on read without rewriting its execution data or DB rows.
    public JobStep historyView(JobStep original) {
        try {
            Map<String,Object> payload=mapper.readValue(original.getPayload(),new TypeReference<>(){});
            if(!payload.containsKey("role")||!payload.containsKey("nodeId")) return original;
            String target=Objects.toString(payload.get("hostId"),original.getTargetId());
            if(Objects.toString(payload.get("hostname"),"").isBlank() || Objects.toString(payload.get("ipAddress"),"").isBlank()) {
                identity(target).forEach((key,value)->{if(!payload.containsKey(key)||Objects.toString(payload.get(key),"").isBlank())payload.put(key,value);});
            }
            var view=new JobStep();BeanUtils.copyProperties(original,view);
            view.setPayload(mapper.writeValueAsString(payload));view.setName(stepName(payload));
            return view;
        } catch(Exception ignored) {return original;}
    }
}
