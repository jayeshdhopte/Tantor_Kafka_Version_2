package io.translab.tantor.artifact.exception;

/** Thrown when a declared checksum does not match the computed one. */
public class ChecksumMismatchException extends RuntimeException {
    public ChecksumMismatchException(String message) {
        super(message);
    }
}
