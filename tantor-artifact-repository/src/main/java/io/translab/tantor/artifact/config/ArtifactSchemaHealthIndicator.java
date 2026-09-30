package io.translab.tantor.artifact.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Read-only readiness guard for the schema owned by tantor-server Flyway.
 *
 * <p>Migration V67 is the latest migration that establishes the Artifact
 * Repository's current {@code kf_artifact} contract. Requiring both that
 * successful Flyway history entry and the table prevents this service from
 * reporting ready against an empty or partially initialized database.</p>
 */
@Component("artifactSchema")
public class ArtifactSchemaHealthIndicator implements HealthIndicator {

    private static final String SCHEMA_READY_SQL = """
            SELECT to_regclass('public.kf_artifact') IS NOT NULL
               AND EXISTS (
                   SELECT 1
                     FROM flyway_schema_history
                    WHERE version = ?
                      AND success = TRUE
               )
            """;

    private final JdbcTemplate jdbcTemplate;
    private final String requiredMigration;

    public ArtifactSchemaHealthIndicator(
            JdbcTemplate jdbcTemplate,
            @Value("${tantor.repository.required-schema-migration:67}") String requiredMigration) {
        this.jdbcTemplate = jdbcTemplate;
        this.requiredMigration = requiredMigration;
    }

    @Override
    public Health health() {
        try {
            Boolean ready = jdbcTemplate.queryForObject(
                    SCHEMA_READY_SQL,
                    Boolean.class,
                    requiredMigration);
            if (Boolean.TRUE.equals(ready)) {
                return Health.up()
                        .withDetail("schema", "public.kf_artifact")
                        .withDetail("requiredMigration", requiredMigration)
                        .build();
            }
        } catch (DataAccessException ignored) {
            // Missing Flyway history/table and connection failures are all unready.
            // Do not expose SQL or connection details through the health response.
        }

        return Health.down()
                .withDetail("schema", "Artifact Repository schema is not initialized")
                .withDetail("requiredMigration", requiredMigration)
                .build();
    }
}
