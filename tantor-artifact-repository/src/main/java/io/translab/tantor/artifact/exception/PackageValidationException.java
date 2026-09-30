package io.translab.tantor.artifact.exception;

public class PackageValidationException extends RuntimeException {
    public PackageValidationException(String message) {
        super(message);
    }
    public PackageValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
