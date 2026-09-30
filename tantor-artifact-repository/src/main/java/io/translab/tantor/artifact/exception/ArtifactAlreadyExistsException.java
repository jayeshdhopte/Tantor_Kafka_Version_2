package io.translab.tantor.artifact.exception;

/** Thrown when an artifact already exists for a (service, version, classifier). */
public class ArtifactAlreadyExistsException extends RuntimeException {
    public ArtifactAlreadyExistsException(String message) {
        super(message);
    }
}
