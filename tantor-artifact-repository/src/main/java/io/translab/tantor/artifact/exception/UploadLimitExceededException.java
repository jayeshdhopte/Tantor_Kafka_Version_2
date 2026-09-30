package io.translab.tantor.artifact.exception;

/** Raised when an incoming artifact or bundle exceeds a configured safety limit. */
public class UploadLimitExceededException extends StorageException {
    public UploadLimitExceededException(String message) {
        super(message);
    }
}
