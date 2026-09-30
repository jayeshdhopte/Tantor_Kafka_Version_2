package io.translab.tantor.artifact.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ArtifactSchemaHealthIndicatorTest {

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final ArtifactSchemaHealthIndicator indicator =
            new ArtifactSchemaHealthIndicator(jdbcTemplate, "67");

    @Test
    void reportsUpOnlyWhenRequiredTableAndMigrationExist() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Boolean.class), eq("67")))
                .thenReturn(true);

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void reportsDownWhenSchemaContractIsAbsent() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Boolean.class), eq("67")))
                .thenReturn(false);

        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void reportsDownWithoutLeakingDatabaseFailureDetails() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Boolean.class), eq("67")))
                .thenThrow(new DataAccessResourceFailureException("sensitive connection detail"));

        var health = indicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails().toString()).doesNotContain("sensitive connection detail");
    }
}
