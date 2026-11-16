package io.translab.tantor.server.service;

import java.util.*;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ExternalBrokerAdditionGuard {
  private final JdbcTemplate jdbc;

  public void assertMutable(UUID clusterId, UUID ownerJob) {
    var rows =
        jdbc.queryForList(
            "select job_id from kf_external_broker_additions where cluster_id=? and state not in"
                + " ('SUCCEEDED','CANCELLED_SAFE')",
            clusterId);
    if (rows.stream()
        .anyMatch(
            row ->
                !Objects.equals(row.get("job_id"), ownerJob == null ? null : ownerJob.toString())))
      throw new IllegalStateException(
          "An external broker addition retains this cluster reservation. Verify/reconcile the same"
              + " operation first.");
  }

  public boolean reservedAgent(String agent) {
    return agent != null
        && Boolean.TRUE.equals(
            jdbc.queryForObject(
                "select exists(select 1 from kf_external_broker_additions where agent_id=? and"
                    + " state not in ('SUCCEEDED','CANCELLED_SAFE'))",
                Boolean.class,
                agent));
  }

  public boolean reservedAgentExcept(String agent, UUID ownerJob) {
    return agent != null
        && Boolean.TRUE.equals(
            jdbc.queryForObject(
                "select exists(select 1 from kf_external_broker_additions where agent_id=? and"
                    + " state not in ('SUCCEEDED','CANCELLED_SAFE') and (cast(? as text) is null or"
                    + " job_id<>?))",
                Boolean.class,
                agent,
                ownerJob == null ? null : ownerJob.toString(),
                ownerJob == null ? null : ownerJob.toString()));
  }

  public List<UUID> jobs() {
    return jdbc.query(
        "select job_id from kf_external_broker_additions where state not in"
            + " ('SUCCEEDED','CANCELLED_SAFE')",
        (rs, n) -> UUID.fromString(rs.getString(1)));
  }
}
