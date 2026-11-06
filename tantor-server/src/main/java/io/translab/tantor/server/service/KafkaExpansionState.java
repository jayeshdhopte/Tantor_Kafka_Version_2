package io.translab.tantor.server.service;

public enum KafkaExpansionState {
    ACCEPTED, EXECUTING, RECOVERY_REQUIRED, SUCCEEDED, CANCELLED_SAFE;
    public boolean reserved(){return this!=SUCCEEDED&&this!=CANCELLED_SAFE;}
}
