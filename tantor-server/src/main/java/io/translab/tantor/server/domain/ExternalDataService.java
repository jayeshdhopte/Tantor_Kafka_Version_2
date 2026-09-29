package io.translab.tantor.server.domain;
import jakarta.persistence.*;
import lombok.Data;
import java.util.UUID;
@Entity @Table(name="kf_external_services") @Data
public class ExternalDataService {
 @Id @GeneratedValue(strategy=GenerationType.UUID) private UUID id;
 @Column(nullable=false) private UUID clusterId;
 @Column(nullable=false) private String agentId;
 @Column(nullable=false) private String role;
 @Column(nullable=false) private String status;
 @Column(nullable=false, columnDefinition="text") private String configJson;
 @Column(columnDefinition="text") private String lastError;
}
