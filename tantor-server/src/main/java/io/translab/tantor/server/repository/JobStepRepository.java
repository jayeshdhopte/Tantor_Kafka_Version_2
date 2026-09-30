package io.translab.tantor.server.repository;

import io.translab.tantor.server.domain.JobStep;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface JobStepRepository extends JpaRepository<JobStep, UUID> {
    List<JobStep> findByJobIdOrderByStepOrderAsc(UUID jobId);
}
