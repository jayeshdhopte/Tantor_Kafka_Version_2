package io.translab.tantor.server.web;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiError {
    private String errorCode;
    private String message;
    private String correlationId;
    private Instant timestamp;
}
