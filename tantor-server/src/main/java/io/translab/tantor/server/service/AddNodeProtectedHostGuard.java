package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.config.AddNodeInspectionProperties;
import io.translab.tantor.server.domain.*;
import io.translab.tantor.server.repository.HostRepository;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
import java.util.regex.Pattern;

/** Exclusions apply to every Add Node path before any Kafka connection or task dispatch. */
public final class AddNodeProtectedHostGuard {
    private AddNodeProtectedHostGuard() {}
    public static void host(Host host, AddNodeInspectionProperties policy) {
        if (policy.excludes(host.getId(), host.getHostIp())) throw blocked();
    }
    public static void hostId(String id, HostRepository hosts, AddNodeInspectionProperties policy) {
        if (policy.getExcludedHostIds().contains(id)) throw blocked();
        if (policy.getExcludedHostIps().isEmpty()) return;
        var host = hosts.findById(id).orElseThrow(AddNodeProtectedHostGuard::unknown);
        if (host.getHostIp() == null || host.getHostIp().isBlank()) throw unknown();
        host(host, policy);
    }
    public static void cluster(Cluster cluster, HostRepository hosts, AddNodeInspectionProperties policy, ObjectMapper mapper) {
        if (policy.getExcludedHostIds().isEmpty() && policy.getExcludedHostIps().isEmpty()) return;
        cluster.getServices().stream().map(ClusterServiceAssignment::getHostId).distinct().forEach(id -> hostId(id, hosts, policy));
        var routes = new ArrayList<String>(); routes.add(Objects.toString(cluster.getBootstrapServers(), ""));
        try {
            Map<String,Object> config = mapper.readValue(Objects.toString(cluster.getConfigJson(), "{}"), new TypeReference<>() {});
            if (config == null) throw unknown();
            for (String key : List.of("bootstrap_servers", "controller_quorum_voters", "quorum_voters", "controller_quorum_bootstrap_servers", "controller_bootstrap_servers", "controller_endpoints", "broker_endpoints"))
                routes.add(Objects.toString(config.get(key), ""));
        } catch (Exception e) { throw unknown(); }
        var denied = new ArrayList<>(policy.getExcludedHostIds()); denied.addAll(policy.getExcludedHostIps());
        for (String target : denied) {
            if (target == null || target.isBlank()) continue;
            var token = Pattern.compile("(^|[^A-Za-z0-9_.-])" + Pattern.quote(target) + "(?=$|[^A-Za-z0-9_.-])");
            if (routes.stream().anyMatch(route -> token.matcher(route).find())) throw blocked();
        }
    }
    private static ResponseStatusException blocked() { return new ResponseStatusException(HttpStatus.CONFLICT, "Add Node cannot contact an excluded candidate, participant, bootstrap endpoint or probe host. Choose an allowed scope."); }
    private static ResponseStatusException unknown() { return new ResponseStatusException(HttpStatus.CONFLICT, "Resolve participant addresses and routing metadata before Add Node checks can respect protected-host exclusions."); }
}
