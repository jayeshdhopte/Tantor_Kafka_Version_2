package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "kf_external_cluster_nodes")
@Data
public class ExternalClusterNode {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "cluster_id", nullable = false)
    private UUID clusterId;

    /**
     * Canonical identity of the parent cluster. The database derives this from
     * cluster_id; application code must not infer it from host or name fields.
     */
    @Column(name = "canonical_cluster_uuid", nullable = false, insertable = false, updatable = false)
    private UUID canonicalClusterUuid;

    @Column(name = "host", nullable = false)
    private String host;

    @Column(name = "node_id")
    private Integer nodeId;

    @Column(name = "is_broker")
    private Boolean isBroker;

    @Column(name = "is_controller")
    private Boolean isController;

    @Column(name = "cpu_usage_pct")
    private Double cpuUsagePct;

    @Column(name = "memory_used_mb")
    private Long memoryUsedMb;

    @Column(name = "memory_total_mb")
    private Long memoryTotalMb;

    @Column(name = "disk_used_gb")
    private Long diskUsedGb;

    @Column(name = "disk_total_gb")
    private Long diskTotalGb;

    @Column(name = "disk_used_bytes")
    private Long diskUsedBytes;

    @Column(name = "disk_total_bytes")
    private Long diskTotalBytes;

    @Column(name = "messages_in_per_sec")
    private Double messagesInPerSec;

    @Column(name = "bytes_in_per_sec")
    private Double bytesInPerSec;

    @Column(name = "last_seen")
    private OffsetDateTime lastSeen;

    @Column(name = "port")
    private Integer port;

    @Column(name = "jmx_exporter_port")
    private Integer jmxExporterPort;

    @Column(name = "install_dir")
    private String installDir;

    @Column(name = "config_file")
    private String configFile;

    @Column(name = "data_dirs")
    private String dataDirs;

    @Column(name = "log_dirs")
    private String logDirs;
}
