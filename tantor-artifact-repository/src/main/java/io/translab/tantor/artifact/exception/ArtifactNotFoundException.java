package io.translab.tantor.artifact.exception;

/** Thrown when an artifact id or coordinate cannot be found. */
public class ArtifactNotFoundException extends RuntimeException {
    public ArtifactNotFoundException(String message) {
        super(message);
    }
}
