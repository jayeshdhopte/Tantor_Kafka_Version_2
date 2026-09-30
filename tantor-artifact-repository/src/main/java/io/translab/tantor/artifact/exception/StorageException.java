package io.translab.tantor.artifact.exception;

/** Wraps low-level filesystem failures from the storage layer. */
public class StorageException extends RuntimeException {
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageException(String message) {
        super(message);
    }
}
