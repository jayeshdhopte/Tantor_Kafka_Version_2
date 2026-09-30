package io.translab.tantor.server.domain;

import jakarta.persistence.*;
import lombok.Data;
import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Entity
@Table(name = "kf_discovery_agents")
public class DiscoveryAgent {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "agent_name")
    private String agentName;

    @Column(name = "hostname")
    private String hostname;

    @Column(name = "ip_addresses", columnDefinition = "jsonb")
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    private String ipAddresses;

    @Column(name = "status")
    private String status;

    @Column(name = "last_heartbeat")
    private OffsetDateTime lastHeartbeat;

    @Column(name = "version")
    private String version;

    @Column(name = "can_execute_tasks")
    private Boolean canExecuteTasks;

    @Column(name = "can_deploy_services")
    private Boolean canDeployServices = false;

    @Column(name = "cluster_id")
    private UUID clusterId;

    @Column(name = "created_at", insertable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", insertable = false, updatable = false)
    private OffsetDateTime updatedAt;
}
