package io.translab.tantor.server.domain;

public enum JobStepStatus {
    PENDING,
    IN_PROGRESS,
    SUCCESS,
    FAILED,
    SKIPPED,
    ROLLED_BACK,
    ROLLBACK_FAILED
}
