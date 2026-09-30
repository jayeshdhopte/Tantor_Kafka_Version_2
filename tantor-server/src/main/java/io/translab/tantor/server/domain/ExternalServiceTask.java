package io.translab.tantor.server.domain;
import jakarta.persistence.*;
import lombok.Data;
import java.time.Instant;
import java.util.UUID;
@Entity @Table(name="kf_external_service_tasks") @Data
public class ExternalServiceTask {
 @Id @GeneratedValue(strategy=GenerationType.UUID) private UUID id;
 @Column(nullable=false) private UUID serviceId;
 @Column(nullable=false) private String agentId;
 @Column(nullable=false) private String command;
 @Column(nullable=false) private String status="PENDING";
 @Column(columnDefinition="text") private String message;
 @Column(nullable=false) private Instant createdAt=Instant.now();
 @Column(nullable=false) private Instant deadline=Instant.now().plusSeconds(1800);
}
