package io.translab.tantor.server.domain;

public enum JobStatus {
    PENDING,
    IN_PROGRESS,
    SUCCESS,
    FAILED,
    PARTIAL_SUCCESS,
    ROLLBACK_PENDING,
    ROLLING_BACK,
    ROLLED_BACK,
    ROLLBACK_FAILED
}
