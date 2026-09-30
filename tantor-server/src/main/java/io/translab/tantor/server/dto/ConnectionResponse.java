package io.translab.tantor.server.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

/**
 * Safe read-only view of a saved data service connection.
 * Never includes certificate data, raw passwords, or encrypted passwords.
 * The `id` field is exposed so the UI can pass ?connectionId=<uuid> in live-fetch requests.
 */
@Data
@Builder
public class ConnectionResponse {
    /** UUID of this row — pass as ?connectionId in live-fetch requests. */
    private UUID id;
    private String connectionName;
    private String protocol;
    private String host;
    private Integer port;
    private String restEndpoint;
    private String certificateType;
    /** True when certificate data has been stored (content is not returned). */
    private boolean certificateConfigured;

    /** True when a truststore password has been stored (value is not returned). */
    private boolean truststoreConfigured;

    private String status;
    private String lastError;
    private Instant lastCheckedAt;
    /** True when this connection is the default for its cluster+service_type. */
    private boolean isDefault;
}
