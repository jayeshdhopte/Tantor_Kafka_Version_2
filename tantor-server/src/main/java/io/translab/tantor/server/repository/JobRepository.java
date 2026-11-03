package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.stereotype.Repository;

import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID> {
    @org.springframework.data.jpa.repository.Query(value = """
            SELECT newer.* FROM kf_jobs newer JOIN kf_jobs original ON original.id = CAST(:id AS text)
            WHERE newer.resource_key = original.resource_key AND newer.created_at > original.created_at
              AND newer.type IN ('TARGETED_CONFIG_CHANGE', 'CONFIG_CHANGE', 'ROLLING_CONFIG_UPDATE')
              AND newer.status <> 'ROLLED_BACK'
              AND (newer.status = 'SUCCESS' OR EXISTS (
                  SELECT 1 FROM kf_job_steps s WHERE s.job_id = newer.id
                    AND CAST(s.payload AS jsonb)->>'configMutationStarted' = 'true'))
            ORDER BY newer.created_at DESC LIMIT 1
            """, nativeQuery = true)
    Optional<Job> findNewerAppliedConfiguration(@org.springframework.data.repository.query.Param("id") UUID id);
    Optional<Job> findFirstByResourceKeyAndTypeOrderByCreatedAtDesc(String resourceKey, io.translab.tantor.server.domain.JobType type);
    org.springframework.data.domain.Page<Job> findByResourceKeyAndType(String resourceKey, io.translab.tantor.server.domain.JobType type, org.springframework.data.domain.Pageable pageable);
    List<Job> findByStatus(JobStatus status);
    List<Job> findAllByOrderByCreatedAtDesc();
    List<Job> findByResourceKeyAndStatusIn(String resourceKey, List<JobStatus> statuses);
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @org.springframework.data.jpa.repository.Query("select job from Job job where job.id = :id")
    Optional<Job> findForUpdateById(@org.springframework.data.repository.query.Param("id") UUID id);

    @org.springframework.data.jpa.repository.Query(value = """
            SELECT j.* FROM kf_jobs j
            WHERE j.type IN ('ROLLING_RESTART', 'CLUSTER_DELETION', 'TARGETED_CONFIG_CHANGE', 'CONFIG_CHANGE', 'ROLLING_CONFIG_UPDATE')
                AND public.cluster_operation_has_unfinished_tasks(j.id)
            """, nativeQuery = true)
    List<Job> findWithUnfinishedRestartTasks();

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Job> findByStatusOrderByCreatedAtAsc(JobStatus status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Job> findFirstByStatusOrderByCreatedAtAsc(JobStatus status);
}
