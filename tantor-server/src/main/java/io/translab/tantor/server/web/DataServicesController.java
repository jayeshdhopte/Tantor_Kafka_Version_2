package io.translab.tantor.server.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.domain.ExternalCluster;
import io.translab.tantor.server.domain.Host;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ExternalClusterRepository;
import io.translab.tantor.server.security.EncryptionService;
import io.translab.tantor.server.repository.HostRepository;
import io.translab.tantor.server.service.DataServiceConnectionService;
import io.translab.tantor.server.dto.ConnectionResponse;
import io.translab.tantor.server.dto.SaveConnectionRequest;
import io.translab.tantor.server.util.RoleAuthenticationUtil;
import io.translab.tantor.server.util.SslUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/data-services")
@RequiredArgsConstructor
public class DataServicesController {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(8);

    private final ClusterRepository clusterRepository;
    private final ExternalClusterRepository externalClusterRepository;
    private final HostRepository hostRepository;
    private final ObjectMapper objectMapper;
    private final DataServiceConnectionService dataServiceConnectionService;
    private final EncryptionService encryptionService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;
    private final HttpClient defaultHttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    // ── HTTP client resolution ───────────────────────────────────────────────

    /**
     * Builds client from a request-scoped X-Custom-Certificate header (backward-compat override).
     * certType may be "PKCS12", the legacy alias "PKCS12_JKS", or "PEM".
     */
    private HttpClient getHttpClient(String encodedCert) {
        if (encodedCert == null || encodedCert.isBlank()) return defaultHttpClient;
        try {
            org.springframework.web.context.request.ServletRequestAttributes attrs =
                (org.springframework.web.context.request.ServletRequestAttributes)
                    org.springframework.web.context.request.RequestContextHolder.currentRequestAttributes();
            String certType = attrs.getRequest().getHeader("X-Custom-Certificate-Type");
            String certPassword = attrs.getRequest().getHeader("X-Custom-Certificate-Password");

            javax.net.ssl.SSLContext sslContext =
                    SslUtils.createSslContext(certType, encodedCert, certPassword);
            return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).sslContext(sslContext).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SSL context from provided certificate: " + e.getMessage(), e);
        }
    }

    /**
     * Client resolution order:
     *  1. Request header cert (backward-compat per-request override)
     *  2. Saved connection cert (connectionId validated against clusterId+serviceType)
     *  3. defaultHttpClient
     */
    private HttpClient getHttpClientForCluster(String encodedCert, UUID clusterId,
                                                String serviceType, UUID connectionId) {
        if (encodedCert != null && !encodedCert.isBlank()) return getHttpClient(encodedCert);
        return dataServiceConnectionService.getActiveConnection(clusterId, serviceType, connectionId)
                .map(conn -> {
                    // No certificate configured — use plain HTTP client
                    if (conn.getCertificateData() == null || conn.getCertificateData().isBlank())
                        return defaultHttpClient;
                    // Certificate IS configured — MUST use it. Never silently fall back to the
                    // plain JDK default client, which would fail for self-signed certs with a
                    // misleading SunCertPathBuilderException instead of a clear cert error.
                    try {
                        String pwd = conn.getTruststorePasswordEncrypted() != null
                                ? encryptionService.decrypt(conn.getTruststorePasswordEncrypted()) : null;
                        javax.net.ssl.SSLContext sslContext = SslUtils.createSslContext(
                                conn.getCertificateType(), conn.getCertificateData(), pwd);
                        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).sslContext(sslContext).build();
                    } catch (Exception e) {
                        // Propagate as RuntimeException so caller gets a clear error message
                        // instead of silently connecting without SSL (which fails for self-signed certs).
                        throw new RuntimeException(
                                "Failed to initialize SSL context from saved certificate: " + e.getMessage(), e);
                    }
                })
                .orElse(defaultHttpClient);
    }

    private String callerUsername() {
        return "admin"; // Placeholder — replace with SecurityContextHolder principal
    }

    // ── Persisted connection CRUD ────────────────────────────────────────────

    @GetMapping("/schema-registry/connections")
    public ResponseEntity<?> listSchemaRegistryConnections(@PathVariable UUID clusterId) {
        return ResponseEntity.ok(dataServiceConnectionService.listConnections(clusterId, "SCHEMA_REGISTRY"));
    }

    @GetMapping("/kafka-connect/connections")
    public ResponseEntity<?> listKafkaConnectConnections(@PathVariable UUID clusterId) {
        return ResponseEntity.ok(dataServiceConnectionService.listConnections(clusterId, "KAFKA_CONNECT"));
    }

    @DeleteMapping("/schema-registry/connections/{connectionId}")
    public ResponseEntity<?> deleteSchemaRegistryConnection(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID connectionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        dataServiceConnectionService.deleteConnection(clusterId, "SCHEMA_REGISTRY", connectionId, callerUsername());
        return ResponseEntity.ok(Map.of("message", "Connection deleted successfully"));
    }

    @DeleteMapping("/kafka-connect/connections/{connectionId}")
    public ResponseEntity<?> deleteKafkaConnectConnection(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID connectionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        dataServiceConnectionService.deleteConnection(clusterId, "KAFKA_CONNECT", connectionId, callerUsername());
        return ResponseEntity.ok(Map.of("message", "Connection deleted successfully"));
    }


    /** GET single connection — by connectionId if given, else default/first. */
    @GetMapping("/schema-registry/connection")
    public ResponseEntity<?> getSchemaRegistryConnection(
            @PathVariable UUID clusterId,
            @RequestParam(required = false) UUID connectionId) {
        return dataServiceConnectionService.getConnectionResponse(clusterId, "SCHEMA_REGISTRY", connectionId)
                .<ResponseEntity<?>>map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
    }

    /** Create new SR connection, or upsert by name. */
    @PutMapping("/schema-registry/connection")
    public ResponseEntity<?> saveSchemaRegistryConnection(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody SaveConnectionRequest req) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        return ResponseEntity.ok(dataServiceConnectionService.saveConnection(clusterId, "SCHEMA_REGISTRY", req, callerUsername()));
    }

    /** Update existing SR connection by connectionId. Prevents rename creating duplicate rows. */
    @PutMapping("/schema-registry/connections/{connectionId}")
    public ResponseEntity<?> updateSchemaRegistryConnectionById(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID connectionId,
            @RequestBody SaveConnectionRequest req) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        req.setId(connectionId); // inject path param into request so service resolves by id
        return ResponseEntity.ok(dataServiceConnectionService.saveConnection(clusterId, "SCHEMA_REGISTRY", req, callerUsername()));
    }

    @GetMapping("/kafka-connect/connection")
    public ResponseEntity<?> getKafkaConnectConnection(
            @PathVariable UUID clusterId,
            @RequestParam(required = false) UUID connectionId) {
        return dataServiceConnectionService.getConnectionResponse(clusterId, "KAFKA_CONNECT", connectionId)
                .<ResponseEntity<?>>map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
    }

    @PutMapping("/kafka-connect/connection")
    public ResponseEntity<?> saveKafkaConnectConnection(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestBody SaveConnectionRequest req) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        return ResponseEntity.ok(dataServiceConnectionService.saveConnection(clusterId, "KAFKA_CONNECT", req, callerUsername()));
    }

    /** Update existing KC connection by connectionId. */
    @PutMapping("/kafka-connect/connections/{connectionId}")
    public ResponseEntity<?> updateKafkaConnectConnectionById(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable UUID connectionId,
            @RequestBody SaveConnectionRequest req) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        req.setId(connectionId);
        return ResponseEntity.ok(dataServiceConnectionService.saveConnection(clusterId, "KAFKA_CONNECT", req, callerUsername()));
    }

    // ── Legacy URL summary ───────────────────────────────────────────────────

    @GetMapping("/connections")
    public ResponseEntity<?> connections(@PathVariable UUID clusterId) {
        Cluster cluster = getCluster(clusterId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schemaRegistryUrl", configuredServiceBaseUrl(cluster, ServiceKind.SCHEMA_REGISTRY));
        result.put("kafkaConnectUrl", configuredServiceBaseUrl(cluster, ServiceKind.KAFKA_CONNECT));
        return ResponseEntity.ok(result);
    }

    @GetMapping("/schema-registry/discover")
    public ResponseEntity<?> discoverSchemaRegistry(@PathVariable UUID clusterId) {
        return ResponseEntity.ok(discoverServiceEndpoint(getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY));
    }

    @GetMapping("/kafka-connect/discover")
    public ResponseEntity<?> discoverKafkaConnect(@PathVariable UUID clusterId) {
        return ResponseEntity.ok(discoverServiceEndpoint(getCluster(clusterId), ServiceKind.KAFKA_CONNECT));
    }

    // ── Schema Registry live-fetch endpoints ─────────────────────────────────

    /** List subjects and their latest schema metadata. */
    @GetMapping("/schema-registry/summary")
    public ResponseEntity<?> schemaRegistrySummary(
            @PathVariable UUID clusterId,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        Cluster cluster = getCluster(clusterId);
        String baseUrl = customBaseUrl(protocol, ip, port, cluster, ServiceKind.SCHEMA_REGISTRY, connectionId);
        JsonNode subjectsNode = requestJson(baseUrl, "GET", "/subjects", null,
                encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);

        List<Map<String, Object>> subjects = new ArrayList<>();
        for (JsonNode subjectNode : subjectsNode) {
            String subject = subjectNode.asText();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("subject", subject);
            row.put("type", subjectType(subject));
            try {
                JsonNode latest = requestJson(baseUrl, "GET",
                        "/subjects/" + pathSeg(subject) + "/versions/latest",
                        null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
                row.put("version", latest.path("version").asInt(0));
                row.put("id", latest.path("id").asInt(0));
                row.put("schemaType", latest.path("schemaType").asText("AVRO"));
                row.put("schema", latest.path("schema").asText(""));
            } catch (RuntimeException ignored) {
                row.put("version", 0); row.put("id", 0); row.put("schemaType", "UNKNOWN"); row.put("schema", "");
            }
            subjects.add(row);
        }

        long keySubjects = subjects.stream().filter(i -> "KEY".equals(i.get("type"))).count();
        long valueSubjects = subjects.stream().filter(i -> "VALUE".equals(i.get("type"))).count();

        return ResponseEntity.ok(Map.of(
                "connection", baseUrl, "subjects", subjects,
                "totalSubjects", subjects.size(), "keySubjects", keySubjects, "valueSubjects", valueSubjects));
    }

    /** Get global compatibility config. */
    @GetMapping("/schema-registry/config")
    public ResponseEntity<?> getGlobalCompatibility(
            @PathVariable UUID clusterId,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId),
                "GET", "/config", null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
    }

    /** Update global compatibility. */
    @PutMapping("/schema-registry/config")
    public ResponseEntity<?> updateGlobalCompatibility(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId,
            @RequestBody JsonNode body) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId),
                "PUT", "/config", body, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
    }

    /** Get all versions for a subject, latest, and compatibility. */
    @GetMapping("/schema-registry/subjects/{subject}/details")
    public ResponseEntity<?> getSubjectDetails(
            @PathVariable UUID clusterId,
            @PathVariable String subject,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        String baseUrl = customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId);
        String seg = pathSeg(subject);

        JsonNode latest = requestJson(baseUrl, "GET", "/subjects/" + seg + "/versions/latest",
                null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
        JsonNode versionsNode = requestJson(baseUrl, "GET", "/subjects/" + seg + "/versions",
                null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);

        List<Map<String, Object>> versions = new ArrayList<>();
        for (JsonNode vNode : versionsNode) {
            int v = vNode.asInt();
            try {
                JsonNode vDetail = requestJson(baseUrl, "GET", "/subjects/" + seg + "/versions/" + v,
                        null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
                versions.add(Map.of(
                        "version", vDetail.path("version").asInt(v),
                        "id", vDetail.path("id").asInt(0),
                        "schemaType", vDetail.path("schemaType").asText("AVRO"),
                        "schema", vDetail.path("schema").asText("")));
            } catch (RuntimeException ignored) {}
        }

        String compatibility = null;
        try {
            JsonNode compatNode = requestJson(baseUrl, "GET", "/config/" + seg,
                    null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
            if (compatNode != null) {
                compatibility = compatNode.path("compatibilityLevel")
                        .asText(compatNode.path("compatibility").asText(null));
            }
        } catch (RuntimeException ignored) {
            // A missing subject-level override is expected; inherit the global config below.
        }

        if (compatibility == null || compatibility.isBlank()) {
            JsonNode globalCompatNode = requestJson(baseUrl, "GET", "/config",
                    null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
            compatibility = globalCompatNode.path("compatibilityLevel")
                    .asText(globalCompatNode.path("compatibility").asText("BACKWARD"));
        }

        return ResponseEntity.ok(Map.of(
                "subject", subject,
                "latest", Map.of(
                        "version", latest.path("version").asInt(0),
                        "id", latest.path("id").asInt(0),
                        "schemaType", latest.path("schemaType").asText("AVRO"),
                        "schema", latest.path("schema").asText("")),
                "versions", versions,
                "compatibility", compatibility));
    }

    /** Register a new schema version. */
    @PostMapping("/schema-registry/subjects/{subject}/versions")
    public ResponseEntity<?> createSchemaVersion(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String subject,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId,
            @RequestBody JsonNode body) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId),
                "POST", "/subjects/" + pathSeg(subject) + "/versions",
                body, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
    }

    /** Delete a subject and all its versions. */
    @DeleteMapping("/schema-registry/subjects/{subject}")
    public ResponseEntity<?> deleteSubject(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String subject,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId),
                "DELETE", "/subjects/" + pathSeg(subject),
                null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
    }

    /** Delete a specific schema version. */
    @DeleteMapping("/schema-registry/subjects/{subject}/versions/{version}")
    public ResponseEntity<?> deleteSchemaVersion(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String subject,
            @PathVariable String version,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId),
                "DELETE", "/subjects/" + pathSeg(subject) + "/versions/" + version,
                null, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
    }

    /** Update subject-level compatibility. */
    @PutMapping("/schema-registry/subjects/{subject}/config")
    public ResponseEntity<?> updateSubjectCompatibility(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String subject,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId,
            @RequestBody JsonNode body) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId),
                "PUT", "/config/" + pathSeg(subject),
                body, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
    }

    /** Check schema compatibility against a subject. */
    @PostMapping("/schema-registry/subjects/{subject}/versions/{version}/compatibility")
    public ResponseEntity<?> checkCompatibility(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String subject,
            @PathVariable String version,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId,
            @RequestBody JsonNode body) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.SCHEMA_REGISTRY_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.SCHEMA_REGISTRY, connectionId),
                "POST", "/compatibility/subjects/" + pathSeg(subject) + "/versions/" + version,
                body, encodedCert, clusterId, "SCHEMA_REGISTRY", connectionId);
    }

    // ── Kafka Connect live-fetch endpoints ───────────────────────────────────

    /** Summary: version, connector list with status, plugins. */
    @GetMapping("/kafka-connect/summary")
    public ResponseEntity<?> kafkaConnectSummary(
            @PathVariable UUID clusterId,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        Cluster cluster = getCluster(clusterId);
        String baseUrl = customBaseUrl(protocol, ip, port, cluster, ServiceKind.KAFKA_CONNECT, connectionId);

        JsonNode root = requestJson(baseUrl, "GET", "/", null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
        JsonNode connectorsNode = requestJson(baseUrl, "GET", "/connectors", null,
                encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
        JsonNode pluginsNode = requestJson(baseUrl, "GET", "/connector-plugins", null,
                encodedCert, clusterId, "KAFKA_CONNECT", connectionId);

        List<Map<String, Object>> connectors = new ArrayList<>();
        int runningTasks = 0, totalTasks = 0, runningConnectors = 0, pausedConnectors = 0, failedConnectors = 0;

        for (JsonNode connNode : connectorsNode) {
            String name = connNode.asText();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", name);
            JsonNode config = requestJson(baseUrl, "GET", "/connectors/" + pathSeg(name) + "/config",
                    null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
            JsonNode status = requestJson(baseUrl, "GET", "/connectors/" + pathSeg(name) + "/status",
                    null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
            String state = status.path("connector").path("state").asText("UNKNOWN");
            if ("RUNNING".equalsIgnoreCase(state)) runningConnectors++;
            else if ("PAUSED".equalsIgnoreCase(state)) pausedConnectors++;
            else failedConnectors++;

            int cRunning = 0, cTotal = 0;
            for (JsonNode task : status.path("tasks")) {
                cTotal++;
                if ("RUNNING".equalsIgnoreCase(task.path("state").asText())) cRunning++;
            }
            runningTasks += cRunning; totalTasks += cTotal;

            row.put("class", config.path("connector.class").asText(""));
            row.put("state", state); row.put("tasks", cTotal); row.put("runningTasks", cRunning);
            row.put("config", config); row.put("status", status);
            connectors.add(row);
        }

        List<Map<String, Object>> plugins = objectMapper.convertValue(pluginsNode, new TypeReference<>() {});

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connection", baseUrl);
        result.put("cluster", root);
        result.put("version", root.path("version").asText("Unknown"));
        result.put("connectors", connectors);
        result.put("plugins", plugins);
        result.put("connectorCount", connectors.size());
        result.put("taskCount", totalTasks);
        result.put("runningTasks", runningTasks);
        result.put("runningConnectors", runningConnectors);
        result.put("pausedConnectors", pausedConnectors);
        result.put("failedConnectors", failedConnectors);
        return ResponseEntity.ok(result);
    }

    /** Get connector plugins list. */
    @GetMapping("/kafka-connect/connector-plugins")
    public ResponseEntity<?> getConnectorPlugins(
            @PathVariable UUID clusterId,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.KAFKA_CONNECT, connectionId),
                "GET", "/connector-plugins", null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
    }

    /** Create connector. */
    @PostMapping("/kafka-connect/connectors")
    public ResponseEntity<?> createConnector(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId,
            @RequestBody JsonNode body) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.KAFKA_CONNECT, connectionId),
                "POST", "/connectors", body, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
    }

    /** Update connector config. */
    @PutMapping("/kafka-connect/connectors/{name}/config")
    public ResponseEntity<?> updateConnectorConfig(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String name,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId,
            @RequestBody JsonNode body) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.KAFKA_CONNECT, connectionId),
                "PUT", "/connectors/" + pathSeg(name) + "/config",
                body, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
    }

    /** Pause / resume / restart a connector. */
    @PutMapping("/kafka-connect/connectors/{name}/{action:pause|resume|restart}")
    public ResponseEntity<?> connectorAction(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String name,
            @PathVariable String action,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        String upstreamMethod = "restart".equals(action) ? "POST" : "PUT";
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.KAFKA_CONNECT, connectionId),
                upstreamMethod, "/connectors/" + pathSeg(name) + "/" + action,
                null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
    }

    /** Delete a connector. */
    @DeleteMapping("/kafka-connect/connectors/{name}")
    public ResponseEntity<?> deleteConnector(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String name,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.KAFKA_CONNECT, connectionId),
                "DELETE", "/connectors/" + pathSeg(name),
                null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
    }

    /** Get tasks for a connector. */
    @GetMapping("/kafka-connect/connectors/{name}/tasks")
    public ResponseEntity<?> getConnectorTasks(
            @PathVariable UUID clusterId,
            @PathVariable String name,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.KAFKA_CONNECT, connectionId),
                "GET", "/connectors/" + pathSeg(name) + "/tasks",
                null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
    }

    /** Restart a specific task. */
    @PostMapping("/kafka-connect/connectors/{name}/tasks/{taskId}/restart")
    public ResponseEntity<?> restartTask(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable UUID clusterId,
            @PathVariable String name,
            @PathVariable String taskId,
            @RequestHeader(value = "X-Custom-Certificate", required = false) String encodedCert,
            @RequestParam(required = false) String protocol,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) Integer port,
            @RequestParam(required = false) UUID connectionId) {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.KAFKA_CONNECT_CHANGE)) {
            return unauthorized();
        }
        return forwardJson(customBaseUrl(protocol, ip, port, getCluster(clusterId), ServiceKind.KAFKA_CONNECT, connectionId),
                "POST", "/connectors/" + pathSeg(name) + "/tasks/" + taskId + "/restart",
                null, encodedCert, clusterId, "KAFKA_CONNECT", connectionId);
    }

    // ── Forwarding / request helpers ─────────────────────────────────────────

    private ResponseEntity<?> forwardJson(String baseUrl, String method, String path,
                                           JsonNode body, String encodedCert,
                                           UUID clusterId, String serviceType, UUID connectionId) {
        return ResponseEntity.ok(requestJson(baseUrl, method, path, body, encodedCert, clusterId, serviceType, connectionId));
    }

    private JsonNode requestJson(String baseUrl, String method, String path, JsonNode body,
                                  String encodedCert, UUID clusterId, String serviceType, UUID connectionId) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(normalizeBaseUrl(baseUrl) + path))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Accept", MediaType.APPLICATION_JSON_VALUE);

            if (body == null) {
                builder.method(method, HttpRequest.BodyPublishers.noBody());
            } else {
                builder.header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .method(method, HttpRequest.BodyPublishers.ofString(
                                objectMapper.writeValueAsString(body)));
            }

            HttpClient client = getHttpClientForCluster(encodedCert, clusterId, serviceType, connectionId);
            HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new DataServiceException(response.statusCode(), response.body());
            }
            String responseBody = response.body();
            if (responseBody == null || responseBody.isBlank()) return objectMapper.createObjectNode();
            return objectMapper.readTree(responseBody);
        } catch (DataServiceException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(userFacingConnectionError(baseUrl, e), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while calling " + baseUrl + path, e);
        }
    }

    @ExceptionHandler(DataServiceException.class)
    public ResponseEntity<Map<String, Object>> handleDataServiceException(DataServiceException ex) {
        return ResponseEntity.status(HttpStatusCode.valueOf(Math.min(Math.max(ex.statusCode(), 400), 599)))
                .body(Map.of("message", ex.message(), "backendStatus", ex.statusCode()));
    }

    @ExceptionHandler(DataServiceConfigurationException.class)
    public ResponseEntity<Map<String, Object>> handleDataServiceConfigurationException(
            DataServiceConfigurationException ex) {
        return ResponseEntity.unprocessableEntity().body(Map.of("message", ex.getMessage()));
    }

    // ── Base URL resolution ──────────────────────────────────────────────────

    /**
     * Resolution order — MUST follow this priority:
     *  1. ?ip= / ?protocol= / ?port= request params → manual override, always wins
     *  2. connectionId param → validated DB lookup for this cluster+serviceType
     *  3. Saved default / first-active DB connection
     *  4. cluster.configJson key lookup
     *
     * An unverified Kafka host + conventional port is deliberately not used here.
     * Schema Registry and Kafka Connect are separate services and are not exposed
     * by Kafka metadata. The UI must run the discovery endpoint or save a connection.
     */
    private String customBaseUrl(String protocol, String ip, Integer port,
                                  Cluster cluster, ServiceKind kind, UUID connectionId) {
        // PRIORITY 1: manual override params
        String scheme = (protocol != null && !protocol.isBlank()) ? protocol : "http";
        if (ip != null && !ip.isBlank() && port != null) return scheme + "://" + ip + ":" + port;
        if (ip != null && !ip.isBlank())                 return scheme + "://" + ip + ":" + kind.defaultPort();

        // PRIORITY 2+3: DB lookup (by connectionId → default → first active)
        String dbUrl = dataServiceConnectionService
                .resolveBaseUrlFromDb(cluster.getId(), kind.name(), connectionId)
                .orElse(null);
        if (dbUrl != null) return dbUrl;

        // PRIORITY 4: explicit configJson URL
        String configuredUrl = configuredServiceBaseUrl(cluster, kind);
        if (configuredUrl != null) return configuredUrl;

        throw new DataServiceConfigurationException(
                "No verified " + kind.displayName() + " connection is configured. "
                        + "Fetch again to detect an endpoint, or add a connection manually.");
    }

    private String configuredServiceBaseUrl(Cluster cluster, ServiceKind kind) {
        Map<String, Object> config = readConfig(cluster);
        for (String key : kind.configKeys()) {
            Object value = config.get(key);
            if (value != null && !String.valueOf(value).isBlank())
                return normalizeBaseUrl(String.valueOf(value));
        }
        return null;
    }

    /**
     * Detects a data-service endpoint without persisting it. Candidates are
     * accepted only after the service-specific health resource returns JSON.
     */
    ServiceDiscoveryResult discoverServiceEndpoint(Cluster cluster, ServiceKind kind) {
        Set<String> candidates = new LinkedHashSet<>();

        dataServiceConnectionService.resolveBaseUrlFromDb(cluster.getId(), kind.name(), null)
                .ifPresent(candidates::add);
        String configuredUrl = configuredServiceBaseUrl(cluster, kind);
        if (configuredUrl != null) candidates.add(configuredUrl);

        for (String host : clusterHosts(cluster)) {
            candidates.add("https://" + host + ":" + kind.defaultPort());
            candidates.add("http://" + host + ":" + kind.defaultPort());
        }

        ServiceDiscoveryResult certificateCandidate = null;
        for (String candidate : candidates) {
            ServiceDiscoveryResult result = probeServiceEndpoint(cluster.getId(), candidate, kind);
            if (result.detected()) return result;
            if (result.certificateRequired() && certificateCandidate == null) certificateCandidate = result;
        }
        if (certificateCandidate != null) return certificateCandidate;

        String fallbackHost = clusterHosts(cluster).stream().findFirst().orElse(null);
        return new ServiceDiscoveryResult(false, false, false, null, fallbackHost,
                kind.defaultPort(), null,
                "No " + kind.displayName() + " endpoint could be detected. "
                        + "Add a connection to configure its protocol, host, and port.");
    }

    private ServiceDiscoveryResult probeServiceEndpoint(UUID clusterId, String candidate, ServiceKind kind) {
        String baseUrl = normalizeBaseUrl(candidate);
        URI uri;
        try {
            uri = URI.create(baseUrl);
            if (uri.getScheme() == null || uri.getHost() == null) return ServiceDiscoveryResult.notDetected();
        } catch (IllegalArgumentException ex) {
            return ServiceDiscoveryResult.notDetected();
        }

        try {
            HttpClient client = dataServiceConnectionService
                    .getActiveConnection(clusterId, kind.name(), null)
                    .filter(conn -> conn.getRestEndpoint() != null
                            && baseUrl.equals(normalizeBaseUrl(conn.getRestEndpoint())))
                    .map(dataServiceConnectionService::buildHttpClientForConnection)
                    .orElse(defaultHttpClient);
            if (client == null) client = defaultHttpClient;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + kind.discoveryPath()))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Accept", MediaType.APPLICATION_JSON_VALUE)
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                return ServiceDiscoveryResult.notDetected();
            }
            JsonNode body = objectMapper.readTree(response.body());
            if (!body.isArray()) return ServiceDiscoveryResult.notDetected();
            return new ServiceDiscoveryResult(true, false, false, uri.getScheme(), uri.getHost(),
                    effectivePort(uri, kind.defaultPort()), baseUrl, null);
        } catch (javax.net.ssl.SSLHandshakeException ex) {
            if (!isCertificateTrustFailure(ex)) return ServiceDiscoveryResult.notDetected();
            return new ServiceDiscoveryResult(false, true, false, uri.getScheme(), uri.getHost(),
                    effectivePort(uri, kind.defaultPort()), baseUrl,
                    kind.displayName() + " appears to use HTTPS, but its certificate is not trusted. "
                            + "Add the service certificate to continue.");
        } catch (IOException | InterruptedException | RuntimeException ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            if (looksLikeTlsOnHttp(ex)) {
                return new ServiceDiscoveryResult(false, false, true, "https", uri.getHost(),
                        effectivePort(uri, kind.defaultPort()),
                        "https://" + uri.getHost() + ":" + effectivePort(uri, kind.defaultPort()),
                        kind.displayName() + " appears to require HTTPS. Verifying the secure endpoint failed; "
                                + "add its certificate if it is self-signed.");
            }
            return ServiceDiscoveryResult.notDetected();
        }
    }

    private List<String> clusterHosts(Cluster cluster) {
        Set<String> hosts = new LinkedHashSet<>();
        if (cluster.getServices() != null) {
            for (ClusterServiceAssignment service : cluster.getServices()) {
                String host = resolveHostAddress(service.getHostId());
                if (host != null && !host.isBlank()) hosts.add(host);
            }
        }
        String bootstrap = cluster.getBootstrapServers();
        if (bootstrap != null) {
            for (String entry : bootstrap.split(",")) {
                String value = entry.trim();
                if (value.contains("://")) value = value.substring(value.indexOf("://") + 3);
                try {
                    URI parsed = URI.create("dummy://" + value);
                    if (parsed.getHost() != null && !parsed.getHost().isBlank()) hosts.add(parsed.getHost());
                } catch (IllegalArgumentException ignored) { }
            }
        }
        return new ArrayList<>(hosts);
    }

    private int effectivePort(URI uri, int defaultPort) {
        return uri.getPort() > 0 ? uri.getPort() : defaultPort;
    }

    private boolean looksLikeTlsOnHttp(Throwable error) {
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("STATUS_LINE")
                    || message.toLowerCase().contains("status line"))) return true;
            current = current.getCause();
        }
        return false;
    }

    private boolean isCertificateTrustFailure(Throwable error) {
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase();
                if (normalized.contains("pkix") || normalized.contains("certificate")
                        || normalized.contains("certpath") || normalized.contains("subject alternative")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private String userFacingConnectionError(String baseUrl, IOException error) {
        if (baseUrl.toLowerCase().startsWith("http://") && looksLikeTlsOnHttp(error)) {
            return "The endpoint " + baseUrl + " appears to require HTTPS. "
                    + "Update the connection protocol to HTTPS and provide its certificate if required.";
        }
        return "Unable to connect to " + baseUrl + ". Check the service host, protocol, port, and certificate.";
    }

    // ── Cluster / host helpers ───────────────────────────────────────────────

    private Cluster getCluster(UUID clusterId) {
        return clusterRepository.findById(clusterId).orElseGet(() -> {
            ExternalCluster ext = externalClusterRepository.findById(clusterId).orElse(null);
            if (ext != null) {
                Cluster dummy = new Cluster();
                dummy.setId(ext.getId());
                dummy.setBootstrapServers(ext.getBootstrapServers());
                return dummy;
            }
            throw new IllegalArgumentException("Cluster not found.");
        });
    }

    private String resolveHostAddress(String hostId) {
        Host host = hostRepository.findById(hostId).orElse(null);
        if (host == null) return hostId;
        if (host.getIpAddresses() != null && !host.getIpAddresses().isBlank()
                && !"[]".equals(host.getIpAddresses())) {
            try {
                List<String> ips = objectMapper.readValue(host.getIpAddresses(), new TypeReference<>() {});
                if (!ips.isEmpty() && ips.get(0) != null && !ips.get(0).isBlank()) return ips.get(0);
            } catch (Exception ignored) {
                String first = host.getIpAddresses().replaceAll("\\[|\\]|\"", "").split(",")[0].trim();
                if (!first.isBlank()) return first;
            }
        }
        return host.getHostname() == null || host.getHostname().isBlank() ? hostId : host.getHostname();
    }

    private Map<String, Object> readConfig(Cluster cluster) {
        if (cluster.getConfigJson() == null || cluster.getConfigJson().isBlank()) return new HashMap<>();
        try { return objectMapper.readValue(cluster.getConfigJson(), new TypeReference<>() {}); }
        catch (Exception e) { return new HashMap<>(); }
    }

    private String normalizeBaseUrl(String baseUrl) {
        String n = baseUrl.trim();
        while (n.endsWith("/")) n = n.substring(0, n.length() - 1);
        return n;
    }

    private String subjectType(String subject) {
        String s = subject.toLowerCase();
        if (s.endsWith("-key")) return "KEY";
        // Schema Registry does not store key/value as subject metadata. This UI creates
        // value schemas by default, so any subject not explicitly named as a key is VALUE.
        return "VALUE";
    }

    private String pathSeg(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private ResponseEntity<?> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
    }

    enum ServiceKind {
        SCHEMA_REGISTRY(8081, List.of(
                "schemaRegistryUrl", "schema_registry_url", "schema.registry.url", "schemaRegistryRestUrl"),
                "/subjects", "Schema Registry"),
        KAFKA_CONNECT(8083, List.of(
                "kafkaConnectUrl", "kafka_connect_url", "connectRestUrl", "connect_rest_url", "connect.rest.url"),
                "/connectors", "Kafka Connect");

        private final int defaultPort;
        private final List<String> configKeys;
        private final String discoveryPath;
        private final String displayName;

        ServiceKind(int defaultPort, List<String> configKeys, String discoveryPath, String displayName) {
            this.defaultPort = defaultPort;
            this.configKeys = configKeys;
            this.discoveryPath = discoveryPath;
            this.displayName = displayName;
        }

        int defaultPort() { return defaultPort; }
        List<String> configKeys() { return configKeys; }
        String discoveryPath() { return discoveryPath; }
        String displayName() { return displayName; }
    }

    record ServiceDiscoveryResult(boolean detected, boolean certificateRequired,
                                  boolean httpsRequired, String protocol, String host,
                                  Integer port, String endpoint, String message) {
        static ServiceDiscoveryResult notDetected() {
            return new ServiceDiscoveryResult(false, false, false, null, null, null, null, null);
        }
    }

    private static class DataServiceConfigurationException extends RuntimeException {
        DataServiceConfigurationException(String message) { super(message); }
    }

    private static class DataServiceException extends RuntimeException {
        private final int statusCode;

        DataServiceException(int statusCode, String body) {
            // Upstream REST responses can contain connector passwords, tokens,
            // JAAS configuration, or other secrets. Keep the raw response out
            // of exception messages, API responses, and application logs.
            super("The native REST API returned HTTP " + statusCode + ".");
            this.statusCode = statusCode;
        }

        int statusCode() { return statusCode; }
        String message() {
            return getMessage();
        }
    }
}
