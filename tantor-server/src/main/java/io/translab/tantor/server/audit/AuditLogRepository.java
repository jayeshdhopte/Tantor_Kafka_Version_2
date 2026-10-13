package io.translab.tantor.server.audit;

import io.translab.tantor.server.domain.AuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import java.util.UUID;

public interface AuditLogRepository extends JpaRepository<AuditLog, UUID>, JpaSpecificationExecutor<AuditLog> {
    @org.springframework.data.jpa.repository.Query(value = "SELECT EXISTS(SELECT 1 FROM kf_audit_logs WHERE action = :action AND details->>'jobId' = :jobId AND details->>'attempt' = CAST(:attempt AS text))", nativeQuery = true)
    boolean existsDeletionEvent(@org.springframework.data.repository.query.Param("action") String action,
                               @org.springframework.data.repository.query.Param("jobId") String jobId,
                               @org.springframework.data.repository.query.Param("attempt") int attempt);
    long countByStatus(String status);
    long countByCategory(String category);

    @org.springframework.data.jpa.repository.Query(value = "SELECT host_ip, host_name FROM kf_artifact_audit_log WHERE artifact_id::text = :artifactId AND host_ip IS NOT NULL LIMIT 1", nativeQuery = true)
    java.util.List<Object[]> findArtifactHostInfo(@org.springframework.data.repository.query.Param("artifactId") String artifactId);

    @org.springframework.data.jpa.repository.Query(value = "SELECT host_ip, hostname FROM kf_hosts WHERE id = :hostId LIMIT 1", nativeQuery = true)
    java.util.List<Object[]> findHostInfo(@org.springframework.data.repository.query.Param("hostId") String hostId);

    @org.springframework.data.jpa.repository.Query(value = "SELECT agent_name, host_ip, hostname FROM kf_hosts WHERE id = :hostId LIMIT 1", nativeQuery = true)
    java.util.List<Object[]> findHostAgentInfo(@org.springframework.data.repository.query.Param("hostId") String hostId);

    @org.springframework.data.jpa.repository.Query(value = "SELECT cluster_name, bootstrap_servers, kafka_cluster_id FROM kf_clusters WHERE id = CAST(:clusterId AS UUID) LIMIT 1", nativeQuery = true)
    java.util.List<Object[]> findClusterInfo(@org.springframework.data.repository.query.Param("clusterId") String clusterId);
}
