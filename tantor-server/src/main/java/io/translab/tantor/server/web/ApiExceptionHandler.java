package io.translab.tantor.server.web;

import io.translab.tantor.server.service.ClusterNameConflictException;
import io.translab.tantor.server.service.CanonicalClusterNotFoundException;
import io.translab.tantor.server.service.CanonicalIdentityException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.UUID;

@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler {

    @ExceptionHandler(CanonicalClusterNotFoundException.class)
    public ResponseEntity<ApiError> handleCanonicalClusterNotFound(
            CanonicalClusterNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error("NOT_FOUND", cleanMessage(ex)));
    }

    @ExceptionHandler(CanonicalIdentityException.class)
    public ResponseEntity<ApiError> handleCanonicalIdentity(CanonicalIdentityException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(error("CONFLICT", cleanMessage(ex)));
    }

    @ExceptionHandler(ClusterNameConflictException.class)
    public ResponseEntity<ApiError> handleClusterNameConflict(ClusterNameConflictException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(error("CLUSTER_NAME_CONFLICT", cleanMessage(ex)));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(error("BAD_REQUEST", cleanMessage(ex)));
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ApiError> handleDataIntegrity(DataIntegrityViolationException ex) {
        log.warn("Data integrity violation", ex);
        String cause = ex.getRootCause() != null ? ex.getRootCause().getMessage() : ex.getMessage();
        if (cause != null && cause.contains("ux_kf_clusters_active_name_ci")) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(error(
                    "DATA_CONFLICT", "A cluster with this name already exists. Choose a different name."));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT).body(error(
                "DATA_CONFLICT", "A data integrity conflict occurred. Please check your input."));
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ApiError> handleRuntime(RuntimeException ex) {
        log.error("Unhandled runtime exception", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(error("INTERNAL_ERROR", "The request could not be completed due to an internal error."));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleException(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(error("INTERNAL_ERROR", "An unexpected internal error occurred."));
    }

    private ApiError error(String errorCode, String message) {
        String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        return ApiError.builder()
                .errorCode(errorCode)
                .message(message)
                .correlationId(correlationId)
                .timestamp(Instant.now())
                .build();
    }

    private String cleanMessage(Exception ex) {
        String message = ex.getMessage();
        return message == null || message.isBlank() ? "The request could not be completed." : message;
    }
}
