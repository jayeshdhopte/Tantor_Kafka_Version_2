package io.translab.tantor.artifact.web;

import io.translab.tantor.artifact.exception.ArtifactAlreadyExistsException;
import io.translab.tantor.artifact.exception.ArtifactNotFoundException;
import io.translab.tantor.artifact.exception.ChecksumMismatchException;
import io.translab.tantor.artifact.exception.StorageException;
import io.translab.tantor.artifact.exception.UploadLimitExceededException;
import io.translab.tantor.artifact.exception.PackageValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;

/** Translates domain exceptions into RFC 7807 problem responses. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ArtifactNotFoundException.class)
    public ProblemDetail handleNotFound(ArtifactNotFoundException ex) {
        return problem(HttpStatus.NOT_FOUND, "Artifact not found", ex.getMessage(), "artifact-not-found");
    }

    @ExceptionHandler(ArtifactAlreadyExistsException.class)
    public ProblemDetail handleConflict(ArtifactAlreadyExistsException ex) {
        return problem(HttpStatus.CONFLICT, "Artifact already exists", ex.getMessage(), "artifact-conflict");
    }

    @ExceptionHandler(ChecksumMismatchException.class)
    public ProblemDetail handleChecksum(ChecksumMismatchException ex) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "Checksum mismatch", ex.getMessage(), "checksum-mismatch");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleBadRequest(IllegalArgumentException ex) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", ex.getMessage(), "bad-request");
    }

    @ExceptionHandler(StorageException.class)
    public ProblemDetail handleStorage(StorageException ex) {
        log.error("Storage failure", ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Storage error", ex.getMessage(), "storage-error");
    }

    @ExceptionHandler(PackageValidationException.class)
    public ProblemDetail handlePackageValidation(PackageValidationException ex) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "Package validation failed", ex.getMessage(), "package-validation-failed");
    }

    @ExceptionHandler(UploadLimitExceededException.class)
    public ProblemDetail handleUploadLimit(UploadLimitExceededException ex) {
        return problem(HttpStatus.PAYLOAD_TOO_LARGE, "Upload limit exceeded", ex.getMessage(), "upload-limit-exceeded");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneric(Exception ex) {
        log.error("Unhandled error", ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error",
                "An unexpected error occurred", "internal-error");
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail, String type) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        pd.setTitle(title);
        pd.setType(URI.create("https://docs.translab.io/tantor/errors/" + type));
        return pd;
    }
}
