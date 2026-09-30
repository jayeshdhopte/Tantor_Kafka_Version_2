package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.Job;
import io.translab.tantor.server.domain.JobType;

public interface JobHandler {
    boolean supports(JobType type);
    void execute(Job job);

    default void rollback(Job job) {
        throw new UnsupportedOperationException("Rollback is not supported for " + job.getType());
    }
}
