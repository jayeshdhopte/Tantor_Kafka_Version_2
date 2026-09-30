package io.translab.tantor.server.dto;

import lombok.Data;

import java.util.UUID;

/**
 * PUT body for saving/updating a Schema Registry or Kafka Connect connection.
 *
 * Rules:
 *  - If `id` is provided on PUT /connections/{connectionId}, this is an update of an existing row
 *    identified by that UUID. Name changes are applied directly without creating a new row.
 *  - If `id` is absent (POST / PUT /connection), the row is looked up by connectionName.
 *    If not found, a new row is created.
 *  - connectionName identifies this logical connection (e.g. "Team A Registry").
 *    Defaults to "Default connection" if omitted.
 *  - truststorePassword is only written when non-blank; null/blank keeps existing encrypted value.
 *  - isDefault, when true, marks this row as the auto-selected connection for its
 *    cluster+service_type (only one default allowed at a time).
 */
@Data
public class SaveConnectionRequest {
    /**
     * UUID of an existing connection row to update.
     * When supplied via PUT /connections/{connectionId}, this is populated by the controller.
     * Client may also supply it explicitly for idempotent upserts.
     */
    private UUID id;

    /** Logical label for this connection, e.g. "Team A Registry". Defaults to "Default connection". */
    private String connectionName;
    private String protocol;
    private String host;
    private Integer port;
    /** 'PEM' or 'PKCS12'. Null = no certificate. */
    private String certificateType;
    /** Base64-encoded certificate content (PEM text or PKCS12 binary). */
    private String certificateData;
    /** Plaintext password — encrypted before persistence. Null = keep existing. */
    private String truststorePassword;
    /**
     * When true, makes this the default auto-selected connection.
     * Null means do not change the current default status.
     */
    private Boolean isDefault;
}
