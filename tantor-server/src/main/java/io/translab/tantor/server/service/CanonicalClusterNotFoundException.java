package io.translab.tantor.server.service;

public class CanonicalClusterNotFoundException extends RuntimeException {
    public CanonicalClusterNotFoundException(String message) {
        super(message);
    }
}
