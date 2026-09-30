package io.translab.tantor.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.dto.BrokerSummaryDto;
import io.translab.tantor.server.config.MonitoringProperties;
import io.translab.tantor.server.repository.HostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class BrokerMetricsCacheService {

    private final HostRepository hostRepository;
    private final KafkaAdminService kafkaAdminService;
    private final ExternalClusterService externalClusterService;
    private final HostStatusService hostStatusService;
    private final ObjectMapper objectMapper;
    private final MonitoringProperties monitoringProperties;
    private final RestTemplate restTemplate = new RestTemplateBuilder()
            .setConnectTimeout(Duration.ofSeconds(2))
            .setReadTimeout(Duration.ofSeconds(2))
            .build();

    private final Map<UUID, CachedBrokers> cache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 10000;

    public List<BrokerSummaryDto> getBrokerSummaries(Cluster cluster) {
        CachedBrokers cached = cache.get(cluster.getId());
        long now = System.currentTimeMillis();
        
        if (cached != null && (now - cached.timestamp < CACHE_TTL_MS)) {
            return cached.brokers;
        }

        // Cache miss or expired, fetch asynchronously
        List<CompletableFuture<BrokerSummaryDto>> futures = cluster.getServices() == null ? new ArrayList<>() : cluster.getServices().stream()
            .filter(svc -> isKafkaNodeRole(svc.getRole()))
            .map(svc -> CompletableFuture.supplyAsync(() -> fetchMetricsForBroker(svc)))
            .collect(Collectors.toList());

        List<BrokerSummaryDto> brokers = new ArrayList<>();
        for (CompletableFuture<BrokerSummaryDto> future : futures) {
            try {
                BrokerSummaryDto dto = future.get();
                if (dto != null) {
                    brokers.add(dto);
                }
            } catch (Exception e) {
                log.error("Failed to fetch broker metrics", e);
            }
        }

        cache.put(cluster.getId(), new CachedBrokers(brokers, now));
        return brokers;
    }

    public List<BrokerSummaryDto> getBrokerSummaries(ExternalCluster cluster) {
        CachedBrokers cached = cache.get(cluster.getId());
        long now = System.currentTimeMillis();
        
        if (cached != null && (now - cached.timestamp < CACHE_TTL_MS)) {
            return cached.brokers;
        }

        List<BrokerSummaryDto> brokers = fetchBootstrapOnlyExternalBrokers(cluster);
        cache.put(cluster.getId(), new CachedBrokers(brokers, now));
        return brokers;
    }

    private BrokerSummaryDto fetchMetricsForBroker(ClusterServiceAssignment svc) {
        Host host = hostRepository.findById(svc.getHostId()).orElse(null);
        if (host == null) return null;

        boolean heartbeatOk = "ONLINE".equalsIgnoreCase(hostStatusService.agentConnectivityStatus(host));
        
        BrokerSummaryDto.BrokerSummaryDtoBuilder builder = BrokerSummaryDto.builder()
            .brokerId(svc.getNodeId())
            .hostname(host.getHostname())
            .role(svc.getRole())
            .isController(isControllerRole(svc.getRole()))
            .lastHeartbeat(host.getLastHeartbeat())
            .hostMetricStatus(heartbeatOk ? "LIVE" : (host.getLastHeartbeat() == null ? "UNAVAILABLE" : "STALE"))
            .metricsTimestamp(System.currentTimeMillis());

        if (heartbeatOk) {
            builder.cpuUsagePct(host.getCpuUsagePct());
            builder.memoryTotalMb(host.getMemTotalMb());
            builder.memoryUsedMb(host.getMemUsedMb());
            builder.diskTotalGb(host.getDiskTotalGb());
            builder.diskUsedGb(host.getDiskUsedGb());
            builder.diskUsedBytes(gibibytesToBytes(host.getDiskUsedGb()));
            builder.diskTotalBytes(gibibytesToBytes(host.getDiskTotalGb()));
        }

        // Fetch JMX
        boolean jmxReachable = false;
        String targetIp = host.getHostIp();
        try {
            if ((targetIp == null || targetIp.isBlank())
                    && host.getIpAddresses() != null && !host.getIpAddresses().isBlank()) {
                List<String> ips = objectMapper.readValue(host.getIpAddresses(), new TypeReference<List<String>>() {});
                if (!ips.isEmpty()) {
                    targetIp = ips.get(0);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse IPs for host {}", host.getId());
        }

        if (targetIp != null) {
            try {
                String url = "http://" + targetIp + ":" + monitoringProperties.getJmxExporterPort() + "/metrics";
                String metricsText = restTemplate.getForObject(url, String.class);
                if (metricsText != null) {
                    jmxReachable = true;
                    parsePrometheusText(metricsText, builder);
                }
            } catch (Exception e) {
                log.warn("Failed to fetch JMX metrics from {}:{}: {}", targetIp, monitoringProperties.getJmxExporterPort(), e.getMessage());
            }
        }

        builder.isJmxReachable(jmxReachable);

        // Determine Health
        if (heartbeatOk && jmxReachable) {
            builder.brokerHealth("HEALTHY");
        } else if (heartbeatOk && !jmxReachable) {
            builder.brokerHealth("DEGRADED");
        } else {
            builder.brokerHealth("OFFLINE");
        }

        return builder.build();
    }

    private List<BrokerSummaryDto> fetchBootstrapOnlyExternalBrokers(ExternalCluster cluster) {
        return externalClusterService.brokerRecords(cluster).stream()
                .filter(record -> isKafkaNodeRole(record.getRole()))
                .map(record -> {
                    boolean live = record.isRunning();
                    return BrokerSummaryDto.builder()
                        .brokerId(record.getNodeId() != null ? record.getNodeId() : -1)
                        .hostname(record.getHostname() != null ? record.getHostname() : record.getBootstrap())
                        .role(record.getRole() != null ? record.getRole() : "broker")
                        .isController(isControllerRole(record.getRole()))
                        .brokerHealth(live ? "HEALTHY" : "DEGRADED")
                        .lastHeartbeat(record.getLastSeen() != null ? OffsetDateTime.parse(record.getLastSeen()) : null)
                        .hostMetricStatus(live ? "LIVE" : (record.getLastSeen() == null ? "UNAVAILABLE" : "STALE"))
                        .isJmxReachable(false)
                        .metricsTimestamp(System.currentTimeMillis())
                        .cpuUsagePct(live ? record.getCpuUsagePct() : null)
                        .memoryTotalMb(live ? record.getMemoryTotalMb() : null)
                        .memoryUsedMb(live ? record.getMemoryUsedMb() : null)
                        .diskTotalGb(live ? record.getDiskTotalGb() : null)
                        .diskUsedGb(live ? record.getDiskUsedGb() : null)
                        .diskUsedBytes(live ? resolveDiskBytes(record.getDiskUsedBytes(), record.getDiskUsedGb()) : null)
                        .diskTotalBytes(live ? resolveDiskBytes(record.getDiskTotalBytes(), record.getDiskTotalGb()) : null)
                        .messagesInPerSec(live ? record.getMessagesInPerSec() : 0.0)
                        .bytesInPerSec(live ? record.getBytesInPerSec() : 0.0)
                        .build();
                })
                .collect(Collectors.toList());
    }

    private Long resolveDiskBytes(Long exactBytes, Long legacyGiB) {
        return exactBytes != null ? exactBytes : gibibytesToBytes(legacyGiB);
    }

    private Long gibibytesToBytes(Long value) {
        if (value == null || value < 0) return null;
        long gibibyte = 1024L * 1024L * 1024L;
        return value > Long.MAX_VALUE / gibibyte ? Long.MAX_VALUE : value * gibibyte;
    }

    private boolean isKafkaNodeRole(String role) {
        return role != null && ("broker".equalsIgnoreCase(role)
                || "controller".equalsIgnoreCase(role)
                || "broker_controller".equalsIgnoreCase(role)
                || "broker_zookeeper".equalsIgnoreCase(role));
    }

    private boolean isControllerRole(String role) {
        return role != null && ("controller".equalsIgnoreCase(role)
                || "broker_controller".equalsIgnoreCase(role));
    }

    private void parsePrometheusText(String text, BrokerSummaryDto.BrokerSummaryDtoBuilder builder) {
        String[] lines = text.split("\n");
        for (String line : lines) {
            if (line.startsWith("#")) continue;
            String[] parts = line.split(" ");
            if (parts.length < 2) continue;
            
            String metric = parts[0].toLowerCase();
            try {
                double val = Double.parseDouble(parts[1]);
                if (metric.startsWith("kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate")
                        || namedOneMinuteRate(metric, "messagesinpersec")) {
                    builder.messagesInPerSec(val);
                } else if (metric.startsWith("kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate")
                        || namedOneMinuteRate(metric, "bytesinpersec")) {
                    builder.bytesInPerSec(val);
                } else if (metric.startsWith("kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate")
                        || namedOneMinuteRate(metric, "bytesoutpersec")) {
                    builder.bytesOutPerSec(val);
                } else if (metric.startsWith("kafka_controller_kafkacontroller_activecontrollercount_value")) {
                    builder.isController(val > 0);
                }
            } catch (Exception e) {
                // ignore unparseable
            }
        }
    }

    private boolean namedOneMinuteRate(String metric, String name) {
        return metric.startsWith("kafka_server_brokertopicmetrics_oneminuterate")
                && metric.contains("name=\"" + name + "\"");
    }

    private static class CachedBrokers {
        List<BrokerSummaryDto> brokers;
        long timestamp;

        CachedBrokers(List<BrokerSummaryDto> brokers, long timestamp) {
            this.brokers = brokers;
            this.timestamp = timestamp;
        }
    }
}
