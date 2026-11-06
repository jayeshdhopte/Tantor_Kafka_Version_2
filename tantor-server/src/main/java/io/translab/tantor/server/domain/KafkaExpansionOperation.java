package io.translab.tantor.server.domain;

import io.translab.tantor.server.service.KafkaExpansionState;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.UUID;

@Entity @Table(name="kf_quorum_expansion_operation") @Getter @Setter
public class KafkaExpansionOperation {
    @Id private UUID id;
    @Column(name="cluster_id",nullable=false,updatable=false) private UUID clusterId;
    @Column(name="host_id",nullable=false,updatable=false) private String hostId;
    @JdbcTypeCode(SqlTypes.ARRAY) @Column(name="participant_hosts",columnDefinition="text[]",nullable=false,updatable=false) private String[] participantHosts;
    @Column(name="controller_id",nullable=false,updatable=false) private Integer controllerId;
    @Column(name="broker_id",updatable=false) private Integer brokerId;
    @JdbcTypeCode(SqlTypes.VARCHAR) @Column(name="job_id",nullable=false,updatable=false) private UUID jobId;
    @Column(name="request_key",nullable=false,updatable=false) private String requestKey;
    @Column(name="request_digest",nullable=false,updatable=false) private String requestDigest;
    @Column(nullable=false,updatable=false,columnDefinition="TEXT") private String plan;
    @Column(name="plan_digest",nullable=false,updatable=false) private String planDigest;
    @Column(nullable=false,columnDefinition="TEXT") private String checkpoints="{}";
    @Enumerated(EnumType.STRING) @Column(nullable=false) private KafkaExpansionState state=KafkaExpansionState.ACCEPTED;
    @Column(name="last_evidence",columnDefinition="TEXT") private String lastEvidence;
    @Column(name="last_error",columnDefinition="TEXT") private String lastError;
    @Column(name="created_at",nullable=false,updatable=false) private Instant createdAt=Instant.now();
    @Column(name="updated_at",nullable=false) private Instant updatedAt=Instant.now();
    @Version private long version;
    @PreUpdate void touched(){updatedAt=Instant.now();}
}
