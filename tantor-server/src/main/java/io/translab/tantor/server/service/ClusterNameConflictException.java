package io.translab.tantor.server.service;

public class ClusterNameConflictException extends RuntimeException {
    public ClusterNameConflictException(String message) {
        super(message);
    }
}
