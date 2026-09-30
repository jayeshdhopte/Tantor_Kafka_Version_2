package io.translab.tantor.server.domain;

public enum JobType {
    DEPLOYMENT,
    EXTERNAL_SERVICES,
    CONFIG_CHANGE,
    ROLLING_RESTART,
    MONITORING_ENABLEMENT,
    ADD_HOST,
    ONBOARDING,
    ROLLING_CONFIG_UPDATE
}
