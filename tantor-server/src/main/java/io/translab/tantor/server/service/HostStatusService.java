package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Host;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;

import io.translab.tantor.server.repository.HostRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class HostStatusService {

    @Autowired
    private HostRepository hostRepository;

    @Value("${tantor.hosts.heartbeat-timeout-seconds:90}")
    private long heartbeatTimeoutSeconds;

    public String effectiveStatus(Host host) {
        if (host == null) {
            return "OFFLINE";
        }
        if (Boolean.TRUE.equals(host.getRemoved())) {
            return "REMOVED";
        }

        String status = host.getStatus();
        if ("PENDING".equalsIgnoreCase(status)) {
            return "PENDING";
        }
        if ("OCCUPIED".equalsIgnoreCase(status)) {
            return "OCCUPIED";
        }

        OffsetDateTime lastHeartbeat = host.getLastHeartbeat();
        long timeoutSeconds = Math.max(heartbeatTimeoutSeconds, 1);
        if (lastHeartbeat != null && lastHeartbeat.isAfter(OffsetDateTime.now().minusSeconds(timeoutSeconds))) {
            return "ONLINE";
        }

        if (lastHeartbeat == null) {
            return "OFFLINE";
        }

        return "OFFLINE";
    }

    public boolean isOnline(Host host) {
        return "ONLINE".equalsIgnoreCase(effectiveStatus(host));
    }

    public String agentConnectivityStatus(Host host) {
        if (host == null || Boolean.TRUE.equals(host.getRemoved())) {
            return "OFFLINE";
        }
        OffsetDateTime lastHeartbeat = host.getLastHeartbeat();
        long timeoutSeconds = Math.max(heartbeatTimeoutSeconds, 1);
        if (lastHeartbeat != null && lastHeartbeat.isAfter(OffsetDateTime.now().minusSeconds(timeoutSeconds))) {
            return "ONLINE";
        }
        return "OFFLINE";
    }

    public boolean isDiscoveryAgent(Host host) {
        if (host == null) {
            return false;
        }
        String version = host.getAgentVersion();
        String id = host.getId();
        return (version != null && version.toLowerCase().contains("discovery"))
                || (id != null && (id.toLowerCase().startsWith("external-")
                || id.toLowerCase().startsWith("discovery-")));
    }

    public boolean isInfrastructureHost(Host host) {
        return !isDiscoveryAgent(host);
    }

    @Scheduled(fixedDelayString = "${tantor.hosts.offline-check-delay-ms:30000}")
    @Transactional
    public void syncOfflineStatus() {
        List<Host> hosts = hostRepository.findAll();
        for (Host host : hosts) {
            if ("ONLINE".equalsIgnoreCase(host.getStatus()) && "OFFLINE".equalsIgnoreCase(effectiveStatus(host))) {
                log.info("Marking host {} as OFFLINE in database due to missed heartbeats", host.getId());
                host.setStatus("OFFLINE");
                hostRepository.save(host);
            }
        }
    }
}
