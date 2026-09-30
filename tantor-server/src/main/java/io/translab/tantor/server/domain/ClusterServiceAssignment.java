package io.translab.tantor.server.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.Data;
import java.util.UUID;

@Entity
@Table(name = "kf_cluster_services")
@Data
public class ClusterServiceAssignment {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id", nullable = false)
    @JsonIgnore
    private Cluster cluster;

    @Column(name = "host_id", nullable = false)
    private String hostId;

    @Column(name = "agent_id")
    private UUID agentId;

    @Column(nullable = false)
    private String role;

    @Column(name = "node_id")
    private Integer nodeId;

    @Column(name = "jmx_exporter_port")
    private Integer jmxExporterPort;

    @Column(name = "node_exporter_port")
    private Integer nodeExporterPort;

    @Column(name = "config_json", columnDefinition = "TEXT")
    private String configJson;

    @Column(name = "status")
    private String status;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;
}
