package io.translab.tantor.server.domain;

import io.translab.tantor.server.service.BrokerAdditionState;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.UUID;

@Entity @Table(name="kf_broker_addition_operation") @Getter @Setter
public class BrokerAdditionOperation {
    @Id private UUID id;
    @Column(name="cluster_id",nullable=false,updatable=false) private UUID clusterId;
    @Column(name="host_id",nullable=false,updatable=false) private String hostId;
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(name="job_id",nullable=false,updatable=false) private UUID jobId;
    @Column(name="request_key",nullable=false,updatable=false) private String requestKey;
    @Column(name="request_digest",nullable=false,updatable=false) private String requestDigest;
    @Column(name="node_id",nullable=false,updatable=false) private Integer nodeId;
    @Enumerated(EnumType.STRING) @Column(nullable=false) private BrokerAdditionState state=BrokerAdditionState.ACCEPTED;
    @Column(nullable=false,columnDefinition="TEXT") private String plan;
    @Column(name="last_evidence",columnDefinition="TEXT") private String lastEvidence;
    @Column(name="last_error",columnDefinition="TEXT") private String lastError;
    @Column(name="created_at",nullable=false,updatable=false) private Instant createdAt=Instant.now();
    @Column(name="updated_at",nullable=false) private Instant updatedAt=Instant.now();
    @Version private long version;
    @PreUpdate void touched(){updatedAt=Instant.now();}
}
