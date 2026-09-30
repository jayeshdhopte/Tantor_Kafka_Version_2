package io.translab.tantor.server.domain;

public enum ConfigVersionStatus {
    PENDING_APPROVAL,
    VALIDATED,
    APPROVED,
    APPLYING,
    APPLIED,
    SUPERSEDED,
    FAILED
}
