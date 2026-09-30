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
    List<Job> findByStatus(JobStatus status);
    List<Job> findAllByOrderByCreatedAtDesc();

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Job> findFirstByStatusOrderByCreatedAtAsc(JobStatus status);
}
