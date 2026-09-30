package io.translab.tantor.server.web;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class JobConflictHandler {

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<Map<String, String>> handleDataConflict(DataIntegrityViolationException exception) {
        String cause = exception.getRootCause() != null
                ? exception.getRootCause().getMessage()
                : exception.getMessage();
        if (cause != null && cause.contains("ux_kf_clusters_active_name_ci")) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "error", "A cluster with this name already exists. Choose a different name."
            ));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "An active job already exists for this cluster or host. Wait for it to finish before starting another operation."
        ));
    }
}
