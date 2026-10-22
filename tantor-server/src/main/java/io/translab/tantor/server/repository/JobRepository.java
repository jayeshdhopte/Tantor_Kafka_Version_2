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
    org.springframework.data.domain.Page<Job> findByResourceKeyAndType(String resourceKey, io.translab.tantor.server.domain.JobType type, org.springframework.data.domain.Pageable pageable);
    List<Job> findByStatus(JobStatus status);
    List<Job> findAllByOrderByCreatedAtDesc();
    List<Job> findByResourceKeyAndStatusIn(String resourceKey, List<JobStatus> statuses);
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @org.springframework.data.jpa.repository.Query("select job from Job job where job.id = :id")
    Optional<Job> findForUpdateById(@org.springframework.data.repository.query.Param("id") UUID id);

    @org.springframework.data.jpa.repository.Query(value = """
            SELECT j.* FROM kf_jobs j
            WHERE j.type IN ('ROLLING_RESTART', 'CLUSTER_DELETION', 'TARGETED_CONFIG_CHANGE', 'CONFIG_CHANGE') AND (
                EXISTS (
                    SELECT 1 FROM kf_job_steps s JOIN kf_tasks t ON t.id = s.agent_task_id
                        OR CAST(t.id AS text) IN (CAST(s.payload AS jsonb)->>'configMutationTaskId', CAST(s.payload AS jsonb)->>'recoveryTaskId')
                    WHERE s.job_id = j.id AND t.status IN ('PENDING', 'IN_PROGRESS', 'RUNNING')
                ) OR EXISTS (
                    SELECT 1 FROM kf_job_steps s JOIN kf_external_kafka_tasks t ON t.id = s.agent_task_id
                        OR CAST(t.id AS text) IN (CAST(s.payload AS jsonb)->>'configMutationTaskId', CAST(s.payload AS jsonb)->>'recoveryTaskId')
                    WHERE s.job_id = j.id AND t.status IN ('PENDING', 'IN_PROGRESS')
                )
            )
            """, nativeQuery = true)
    List<Job> findWithUnfinishedRestartTasks();

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Job> findByStatusOrderByCreatedAtAsc(JobStatus status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Job> findFirstByStatusOrderByCreatedAtAsc(JobStatus status);
}
