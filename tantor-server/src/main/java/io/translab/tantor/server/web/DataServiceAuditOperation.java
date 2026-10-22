package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** Identifies operations without copying connector configurations or credentials into audit storage. */
record DataServiceAuditOperation(String action, String resourceType, String resource, String service) {
    static DataServiceAuditOperation identify(String service, String method, String path, JsonNode body) {
        if ("GET".equals(method) || "DELETE".equals(method)) return null; // Deletions have their own soft/hard audit details.
        String clean = path.split("\\?", 2)[0];
        String[] parts = clean.substring(1).split("/");
        if ("KAFKA_CONNECT".equals(service) && parts[0].equals("connectors")) {
            String name = parts.length > 1 ? decode(parts[1]) : body == null ? "" : body.path("name").asText("");
            String operation = parts.length == 1 ? "CREATE" : parts.length > 3 && parts[2].equals("tasks") ? "TASK_RESTART"
                    : parts.length > 2 && parts[2].equals("config") ? "CONFIG_UPDATE"
                    : parts.length > 2 ? parts[2].toUpperCase(Locale.ROOT) : "UPDATE";
            return new DataServiceAuditOperation("KAFKA_CONNECT_CONNECTOR_" + operation, "KAFKA_CONNECT_CONNECTOR", name, service);
        }
        if ("SCHEMA_REGISTRY".equals(service)) {
            if (parts[0].equals("config") || parts[0].equals("mode")) {
                String scope = parts.length == 1 ? "GLOBAL" : "SUBJECT";
                String name = parts.length == 1 ? "Global" : decode(parts[1]);
                return new DataServiceAuditOperation("SCHEMA_REGISTRY_" + scope + "_" + (parts[0].equals("mode") ? "MODE" : "COMPATIBILITY") + "_UPDATE", "SCHEMA_" + scope, name, service);
            }
            if (parts[0].equals("subjects") && parts.length > 1)
                return new DataServiceAuditOperation("SCHEMA_REGISTRY_VERSION_REGISTER", "SCHEMA_SUBJECT", decode(parts[1]), service);
            if (parts[0].equals("compatibility") && parts.length > 2)
                return new DataServiceAuditOperation("SCHEMA_REGISTRY_COMPATIBILITY_CHECK", "SCHEMA_SUBJECT", decode(parts[2]), service);
        }
        return null;
    }
    private static String decode(String value) { return URLDecoder.decode(value, StandardCharsets.UTF_8); }
}
