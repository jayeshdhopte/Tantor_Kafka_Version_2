package io.translab.tantor.server.service;

import io.translab.tantor.server.domain.DataServiceConnection;
import io.translab.tantor.server.dto.ConnectionResponse;
import io.translab.tantor.server.dto.SaveConnectionRequest;
import io.translab.tantor.server.repository.DataServiceConnectionRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.util.SslUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DataServiceConnectionService {

    private static final String FALLBACK_CONNECTION_NAME = "Default connection";
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(5);

    private final DataServiceConnectionRepository repository;
    private final EncryptionService encryptionService;
    private final io.translab.tantor.server.audit.AuditService auditService;

    // ── Public read API ────────────────────────────────────────────────────────

    /**
     * Returns all active saved connections for a cluster+service, ordered by name.
     * Used for the connection switcher dropdown. Safe DTO — no secrets returned.
     */
    public List<ConnectionResponse> listConnections(UUID clusterId, String serviceType) {
        return repository
                .findByClusterIdAndServiceTypeAndIsActiveTrueOrderByConnectionNameAsc(clusterId, serviceType)
                .stream()
                .map(this::toSafeResponse)
                .toList();
    }

    /**
     * Returns the safe DTO for the GET /connection endpoint.
     * Resolves by connectionId (validated against clusterId+serviceType), else default/first.
     */
    public Optional<ConnectionResponse> getConnectionResponse(UUID clusterId,
                                                               String serviceType,
                                                               UUID connectionId) {
        return resolveRawConnection(clusterId, serviceType, connectionId)
                .map(this::toSafeResponse);
    }

    // ── Upsert ────────────────────────────────────────────────────────────────

    /**
     * Create or update a named connection for cluster+service.
     *
     * Resolution rules for target row:
     *  1. If req.id is set → load by id (validated against cluster+serviceType).
     *  2. Else if connectionName is set → load by name.
     *  3. Else → create new.
     *
     * is_default rules:
     *  - If this is the FIRST active connection → auto-set is_default = true.
     *  - If req.isDefault = true → unset all other defaults, then set this one.
     *  - If req.isDefault = false → unset this one's default.
     *  - If req.isDefault = null → keep current value (no change).
     *
     * rest_endpoint is always recomputed server-side.
     * truststorePassword only overwritten when a new value is provided.
     * Connectivity test run after save; status/lastError/lastCheckedAt updated.
     */
    @Transactional
    public ConnectionResponse saveConnection(UUID clusterId,
                                             String serviceType,
                                             SaveConnectionRequest req,
                                             String callerUsername) {

        if (req.getProtocol() == null || req.getProtocol().isBlank()) {
            throw new IllegalArgumentException("protocol is required");
        }
        if (req.getHost() == null || req.getHost().isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        if (req.getPort() == null) {
            throw new IllegalArgumentException("port is required");
        }

        String connectionName = (req.getConnectionName() != null && !req.getConnectionName().isBlank())
                ? req.getConnectionName().trim()
                : FALLBACK_CONNECTION_NAME;

        // Count existing active rows BEFORE creating new one (for auto-default logic)
        long existingCount = repository.countByClusterIdAndServiceTypeAndIsActiveTrue(clusterId, serviceType);
        boolean isFirstConnection = (existingCount == 0);

        // Locate target row (by explicit id, by name, or create new)
        DataServiceConnection conn = loadTargetRow(clusterId, serviceType, req, connectionName);

        // Apply updates
        conn.setProtocol(req.getProtocol().trim().toLowerCase());
        conn.setHost(req.getHost().trim());
        conn.setPort(req.getPort());
        conn.setCertificateType(normalizeCertificateType(req.getCertificateType()));
        conn.setCertificateData(req.getCertificateData());

        conn.setConnectionName(connectionName);

        // Only overwrite encrypted password when a new plaintext password is provided
        if (req.getTruststorePassword() != null && !req.getTruststorePassword().isBlank()) {
            conn.setTruststorePasswordEncrypted(
                    encryptionService.encrypt(req.getTruststorePassword()));
        }

        // Determine is_default
        boolean makeDefault = Boolean.TRUE.equals(req.getIsDefault()) || isFirstConnection;
        if (makeDefault) {
            clearOtherDefaults(clusterId, serviceType, conn.getId());
            conn.setIsDefault(true);
        } else if (Boolean.FALSE.equals(req.getIsDefault())) {
            conn.setIsDefault(false);
        }
        // null → keep current isDefault

        // Always recompute rest_endpoint server-side
        conn.setRestEndpoint(buildRestEndpoint(conn.getProtocol(), conn.getHost(), conn.getPort()));
        conn.setUpdatedBy(callerUsername != null ? callerUsername : io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());

        boolean isNew = conn.getId() == null;

        conn = repository.save(conn);
        
        // Audit log
        try {
            String action = isNew ? "CREATE_CONNECTION" : "UPDATE_CONNECTION";
            String details = String.format("Connection: %s, Protocol: %s, Host: %s:%d",
                    conn.getConnectionName(), conn.getProtocol(), conn.getHost(), conn.getPort());
            auditService.recordAs(callerUsername, "DATA_SERVICES", null,
                    serviceType, action, "CONNECTION", conn.getId().toString(),
                    clusterId, "SUCCESS", null, null, null, details);
        } catch (Exception e) {
            log.warn("Failed to write audit log for connection save: {}", e.getMessage());
        }

        // Health check and update status
        ConnectivityResult result = testConnectivity(serviceType, conn.getRestEndpoint(), conn);
        conn.setStatus(result.status());
        conn.setLastError(result.error());
        conn.setLastCheckedAt(Instant.now());
        conn = repository.save(conn);

        return toSafeResponse(conn);
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    @Transactional
    public void deleteConnection(UUID clusterId, String serviceType, UUID connectionId, String callerUsername) {
        repository.findByIdAndClusterIdAndServiceTypeAndIsActiveTrue(connectionId, clusterId, serviceType)
                .ifPresent(conn -> {
                    conn.setIsActive(false);
                    conn.setUpdatedBy(callerUsername != null ? callerUsername : io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
                    repository.save(conn);
                    
                    // If the deleted connection was default, optionally elect a new default
                    if (Boolean.TRUE.equals(conn.getIsDefault())) {
                        repository.findByClusterIdAndServiceTypeAndIsActiveTrueOrderByConnectionNameAsc(clusterId, serviceType)
                                .stream()
                                .findFirst()
                                .ifPresent(newDefault -> {
                                    newDefault.setIsDefault(true);
                                    repository.save(newDefault);
                                });
                    }

                    // Audit log
                    try {
                        String details = String.format("Deleted connection: %s", conn.getConnectionName());
                        auditService.recordAs(callerUsername, "DATA_SERVICES", null,
                                serviceType, "DELETE_CONNECTION", "CONNECTION", conn.getId().toString(),
                                clusterId, "SUCCESS", null, null, null, details);
                    } catch (Exception e) {
                        log.warn("Failed to write audit log for connection delete: {}", e.getMessage());
                    }
                });
    }

    // ── Base URL resolution ────────────────────────────────────────────────────

    /**
     * Resolves the base URL for a data service request.
     *
     * Resolution order (highest to lowest priority):
     *  1. Request-level override params (handled by caller — if ip/protocol/port params present, caller wins)
     *  2. Saved connection identified by connectionId (validated against clusterId + serviceType)
     *  3. Saved default active connection for cluster+serviceType
     *  4. First active connection (alphabetical) — last DB fallback
     *  5. cluster.configJson lookup (handled by caller)
     *  6. Default host:port (handled by caller)
     *
     * Returns Optional.empty() when no saved row resolves, so caller falls through.
     */
    public Optional<String> resolveBaseUrlFromDb(UUID clusterId, String serviceType, UUID connectionId) {
        return resolveRawConnection(clusterId, serviceType, connectionId)
                .map(conn -> conn.getRestEndpoint() != null && !conn.getRestEndpoint().isBlank()
                        ? conn.getRestEndpoint()
                        : buildRestEndpoint(conn.getProtocol(), conn.getHost(), conn.getPort()));
    }

    /**
     * Returns the active DataServiceConnection entity for building SSL clients.
     * Resolves by connectionId (cluster+serviceType validated) if given, else default/first.
     */
    public Optional<DataServiceConnection> getActiveConnection(UUID clusterId,
                                                                String serviceType,
                                                                UUID connectionId) {
        return resolveRawConnection(clusterId, serviceType, connectionId);
    }

    /** Backward-compat overload when no connectionId is known. */
    public Optional<DataServiceConnection> getActiveConnection(UUID clusterId, String serviceType) {
        return resolveRawConnection(clusterId, serviceType, null);
    }

    // ── SSL client builder ─────────────────────────────────────────────────────

    /**
     * Builds an HttpClient for the saved connection's certificate configuration.
     * Returns null when no SSL cert is configured.
     */
    public HttpClient buildHttpClientForConnection(DataServiceConnection conn) {
        if (conn == null
                || conn.getCertificateType() == null
                || conn.getCertificateData() == null
                || conn.getCertificateData().isBlank()) {
            return null;
        }
        try {
            SSLContext sslContext = buildSslContext(conn);
            return HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .sslContext(sslContext)
                    .build();
        } catch (Exception e) {
            log.warn("Failed to build SSL context for saved connection {} — using plain client",
                    conn.getId(), e);
            return null;
        }
    }

    // ── Internal helpers ───────────────────────────────────────────────────────

    /**
     * Locate the target row to update, in order:
     *  1. By explicit req.id (validated against cluster+serviceType to prevent cross-contamination).
     *  2. By connectionName within the same cluster+serviceType.
     *  3. Create new.
     */
    private DataServiceConnection loadTargetRow(UUID clusterId, String serviceType,
                                                 SaveConnectionRequest req, String connectionName) {
        // 1. Explicit id supplied (e.g. from PUT /connections/{connectionId})
        if (req.getId() != null) {
            return repository
                    .findByIdAndClusterIdAndServiceTypeAndIsActiveTrue(req.getId(), clusterId, serviceType)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Connection " + req.getId() + " not found or does not belong to this cluster/service"));
        }

        // 2. Look up by name
        Optional<DataServiceConnection> byName = repository
                .findByClusterIdAndServiceTypeAndConnectionNameAndIsActiveTrue(
                        clusterId, serviceType, connectionName);
        if (byName.isPresent()) return byName.get();

        // 3. Create new row
        DataServiceConnection c = new DataServiceConnection();
        c.setClusterId(clusterId);
        c.setServiceType(serviceType);
        c.setConnectionName(connectionName);
        c.setIsActive(true);
        c.setIsDefault(false);
        c.setCreatedBy(io.translab.tantor.server.security.SecurityUtils.getCurrentUsername());
        return c;
    }

    /**
     * Core resolution logic (validated):
     *  1. By connectionId — VALIDATED against clusterId + serviceType.
     *     Logs a warning and falls through if the ID doesn't belong to this cluster/service.
     *  2. By is_default = true for this cluster+serviceType.
     *  3. By first active row (alphabetical).
     */
    private Optional<DataServiceConnection> resolveRawConnection(UUID clusterId,
                                                                   String serviceType,
                                                                   UUID connectionId) {
        if (connectionId != null) {
            Optional<DataServiceConnection> byId = repository
                    .findByIdAndClusterIdAndServiceTypeAndIsActiveTrue(connectionId, clusterId, serviceType);
            if (byId.isPresent()) return byId;
            log.warn("connectionId {} not found, not active, or does not belong to cluster {} / service {} — falling back to default",
                    connectionId, clusterId, serviceType);
        }

        Optional<DataServiceConnection> defaultConn =
                repository.findByClusterIdAndServiceTypeAndIsDefaultTrueAndIsActiveTrue(clusterId, serviceType);
        if (defaultConn.isPresent()) return defaultConn;

        // Last resort: first active row alphabetically
        return repository
                .findByClusterIdAndServiceTypeAndIsActiveTrueOrderByConnectionNameAsc(clusterId, serviceType)
                .stream()
                .findFirst();
    }

    /**
     * Unmark is_default on all OTHER active rows for this cluster+service.
     * excludeId is the row that will be becoming default (may be null for new rows).
     */
    private void clearOtherDefaults(UUID clusterId, String serviceType, UUID excludeId) {
        repository
                .findByClusterIdAndServiceTypeAndIsActiveTrueOrderByConnectionNameAsc(clusterId, serviceType)
                .forEach(c -> {
                    if (Boolean.TRUE.equals(c.getIsDefault())
                            && (excludeId == null || !c.getId().equals(excludeId))) {
                        c.setIsDefault(false);
                        repository.save(c);
                    }
                });
    }

    private String normalizeCertificateType(String certificateType) {
        if (certificateType == null || certificateType.isBlank()) {
            return null;
        }

        String normalized = certificateType.trim().toUpperCase(Locale.ROOT);
        if (!"PEM".equals(normalized) && !"PKCS12".equals(normalized)) {
            throw new IllegalArgumentException("certificateType must be PEM or PKCS12");
        }
        return normalized;
    }

    private String buildRestEndpoint(String protocol, String host, int port) {
        return protocol + "://" + host + ":" + port;
    }

    private SSLContext buildSslContext(DataServiceConnection conn) throws Exception {
        String password = null;
        if (conn.getTruststorePasswordEncrypted() != null
                && !conn.getTruststorePasswordEncrypted().isBlank()) {
            password = encryptionService.decrypt(conn.getTruststorePasswordEncrypted());
        }
        return SslUtils.createSslContext(
                conn.getCertificateType(), conn.getCertificateData(), password);
    }

    private ConnectivityResult testConnectivity(String serviceType,
                                                String restEndpoint,
                                                DataServiceConnection conn) {
        String healthPath = "KAFKA_CONNECT".equals(serviceType) ? "/connectors" : "/subjects";
        String url = restEndpoint + healthPath;

        try {
            HttpClient client;
            // If a certificate is configured, we MUST use it — never silently fall back
            // to the plain default client, because that would use the JDK truststore which
            // does not trust self-signed certs, producing a misleading SunCertPathBuilderException.
            if (conn != null
                    && conn.getCertificateData() != null
                    && !conn.getCertificateData().isBlank()) {
                try {
                    HttpClient built = buildHttpClientForConnection(conn);
                    if (built == null) {
                        return new ConnectivityResult("ERROR",
                                "Certificate is configured but SSL context could not be built. "
                                + "Check that the certificate format matches the selected type (PEM / PKCS12).");
                    }
                    client = built;
                } catch (Exception ex) {
                    return new ConnectivityResult("ERROR",
                            "Failed to initialize SSL context from certificate: " + sanitize(ex.getMessage()));
                }
            } else {
                client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(4)).build();
            }

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(HEALTH_CHECK_TIMEOUT)
                    .GET()
                    .build();

            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            int status = response.statusCode();

            if (status >= 200 && status < 300) {
                return new ConnectivityResult("ONLINE", null);
            } else if (status >= 400 && status < 500) {
                return new ConnectivityResult("ONLINE",
                        "Endpoint returned HTTP " + status + " — service reachable but check auth/path");
            } else {
                return new ConnectivityResult("ERROR", "Endpoint returned HTTP " + status);
            }
        } catch (java.net.ConnectException | java.net.NoRouteToHostException e) {
            return new ConnectivityResult("OFFLINE", "Connection refused: " + sanitize(e.getMessage()));
        } catch (java.net.http.HttpTimeoutException e) {
            return new ConnectivityResult("OFFLINE", "Connection timed out");
        } catch (Exception e) {
            return new ConnectivityResult("ERROR", sanitize(e.getMessage()));
        }
    }

    private String sanitize(String msg) {
        if (msg == null) return "Unknown error";
        return msg.length() > 300 ? msg.substring(0, 300) + "…" : msg;
    }

    private ConnectionResponse toSafeResponse(DataServiceConnection conn) {
        return ConnectionResponse.builder()
                .id(conn.getId())
                .connectionName(conn.getConnectionName())
                .protocol(conn.getProtocol())
                .host(conn.getHost())
                .port(conn.getPort())
                .restEndpoint(conn.getRestEndpoint())
                .certificateType(conn.getCertificateType())
                .certificateConfigured(conn.getCertificateData() != null
                        && !conn.getCertificateData().isBlank())

                .truststoreConfigured(conn.getTruststorePasswordEncrypted() != null
                        && !conn.getTruststorePasswordEncrypted().isBlank())

                .status(conn.getStatus())
                .lastError(conn.getLastError())
                .lastCheckedAt(conn.getLastCheckedAt())
                .isDefault(Boolean.TRUE.equals(conn.getIsDefault()))
                .build();
    }

    private record ConnectivityResult(String status, String error) {}
}
