package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "kf_hosts")
@Getter
@Setter
public class Host {
    @Id
    private String id; // agent host_id

    @Column(nullable = false)
    private String hostname;

    @Column(name = "agent_name")
    private String agentName;

    @Transient
    public String getIpAddresses() {
        return hostIp == null ? "[]" : "[\"" + hostIp + "\"]";
    }

    @Column(name = "os_details")
    private String osDetails;

    @Column(name = "agent_version")
    private String agentVersion;

    @Column(name = "agent_path")
    private String agentPath;

    @Transient
    public String getAgentStatus() {
        return getStatus();
    }
    @Column(nullable = false)
    private String status;

    @Column(name = "last_heartbeat")
    private OffsetDateTime lastHeartbeat;

    @Column(name = "cpu_usage_pct")
    private Double cpuUsagePct;

    @Column(name = "mem_total_mb")
    private Long memTotalMb;

    @Column(name = "mem_used_mb")
    private Long memUsedMb;

    @Column(name = "disk_total_gb")
    private Long diskTotalGb;

    @Column(name = "disk_used_gb")
    private Long diskUsedGb;

    @Column(name = "java_version")
    private String javaVersion;

    @Column(name = "host_ip")
    private String hostIp;

    @Column(name = "removed")
    private Boolean removed = false;

    @Column(name = "action")
    private String action;

    @Column(name = "resource_type")
    private String resourceType;

    @Transient // user_name column was dropped in V58 migration — kept for API compatibility only
    private String user;

    @Column(name = "cluster_id")
    private UUID clusterId;

    @Column(name = "created_at", insertable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", insertable = false, updatable = false)
    private OffsetDateTime updatedAt;
}
