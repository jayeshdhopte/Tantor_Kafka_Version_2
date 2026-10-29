# Config-change ER persistence

New configuration jobs now write to the V90 ER tables alongside their existing storage. This covers `TARGETED_CONFIG_CHANGE`, versioned `CONFIG_CHANGE` and legacy `ROLLING_CONFIG_UPDATE` jobs. No migration, column, table or constraint is added in this phase.

The original job engine, health checks, Kafka/agent commands, checkpoints, JSON payloads, UI/API responses and existing config-version behavior remain authoritative. The new tables are additional history/projections, not a replacement execution engine.

## When rows are written

| Table | Data recorded |
|---|---|
| kf_config_catalog | Observed property keys with role/execution-policy context, reused when the context matches; not a fabricated full Kafka master catalog |
| kf_config_change | One row per existing configuration job ID; scope/classification/requester and lifecycle timestamps/status |
| kf_config_change_detail | One row per property; common desired value, or NULL when targets have different desired values |
| kf_config_change_target_node | Planned targets, inventory UUID when unambiguous, frozen roles, order and job-step outcome |
| kf_config_change_node_value | Per-target desired and previous values; immutable checkpoint observations, rollback operation and actual verification results |
| kf_cluster_operation_lock | A projection of the accepted job's operation reservation and last observed guard state; no new fencing/lease enforcement |
| kf_health_check | Actual preview/execution health snapshots and outcomes; not inferred from job completion |
| kf_config_execution_attempt | Per-property/target management-job attempts, APPLY/ROLLBACK phases, original step identity and status/error evidence |
| kf_config_backup | Before-state checkpoints and agent-reported artifact paths; only reports actual available metadata |
| kf_rolling_restart_operation | Static/file changes only: aggregate job lifecycle and completed-target count |
| kf_rolling_restart_node_step | Static/file changes only: planned order, node-step status and observed attempt count |

Creation of the job and its initial ER records is one transaction. Step start, checkpoints, mutation binding, verification, completion/failure, retry and recovery update the corresponding records. Catalog observation creation is serialized only when a matching catalog record is absent. Stable identifiers prevent duplicate detail/value/backup/attempt records during repeated callbacks. Rollback retries use both job and step counters so failed-attempt evidence is not overwritten when only the job counter advances.

Dynamic changes do **not** create rows in the two config-change rolling-restart tables. They also do not write to the standalone menu restart table `kf_rolling_restart_table`. Add Service persistence remains separate.

## Accuracy and unavailable fields

- `target_node.node_id` is the existing inventory UUID from `kf_nodes.id`, not the integer Kafka broker/node number. An absent or ambiguous inventory match stays NULL.
- Initial before values come from the accepted plan. Runtime immutable checkpoints update the before-state evidence; Retry does not replace the original checkpoint with a later value.
- After values/verification status are written from actual Kafka/file checks or the agent's explicit successful file-value verification. An acknowledgement alone is not treated as property verification.
- A before-state checkpoint can exist before a physical backup task finishes. Its backup URI remains NULL until a real path is reported. Reported disk/dynamic artifacts are linked to their target node, without inventing property/chunk membership from a plain-text path line.
- Sensitive keys and Kafka-sensitive checkpoint values are redacted in the added value/payload storage. No secret reference is invented, and existing JSON is not moved or rewritten by the projection.
- Catalog versions, topology versions, update-mode/type metadata, secret references, hashes, backup checksums/ownership/mode, controller epochs and precise process/restart timestamps remain NULL where the existing backend does not provide them.
- Job/step timestamps describe the existing combined operation. They are not claimed to be exact internal agent restart timestamps. Attempt counts describe management-job/step execution attempts, not agent lease retries.
- The lock table is an observational summary of the existing job guard at transitions. Existing locking remains in place; fencing tokens, leases and heartbeat timestamps are not fabricated.

The existing `kf_config_versions` table remains flow-specific: versioned file operations use it; targeted dynamic/static jobs currently do not manufacture a version record there. The new ER records supply history for those targeted paths. Existing audit flows are unchanged in this phase.

## Scope and use

No historical jobs are backfilled. Rebuild/restart the updated local backend, then submit a **new** configuration change. Rejected previews and requests that create no job do not produce an accepted-change record. Jobs that fail before a backup/verification do not fabricate those later records.

Inspect the added data in pgAdmin using the existing connection on port 5433:

```sql
SELECT c.change_id, c.execution_type, c.status, d.config_key,
       n.node_id AS kafka_node_id, n.host_id,
       v.value_before, v.desired_value, v.value_after,
       v.source_before, v.source_after, v.verification_status
FROM kf_config_change c
JOIN kf_config_change_detail d ON d.change_id = c.change_id
JOIN kf_config_change_node_value v ON v.detail_id = d.detail_id
JOIN kf_config_change_target_node t ON t.target_id = v.target_id
LEFT JOIN kf_nodes n ON n.id = t.node_id
ORDER BY c.requested_at DESC, t.planned_order, d.sequence_no;
```

Local verification includes seven opt-in database scenarios against `127.0.0.1:5433/tantor`. They run inside rollback-only transactions, issue no audit DELETE and create no database. Legacy empty-string deletion markers are normalized to REMOVE; ambiguous old empty-string values do not fabricate a presence flag. Set `TANTOR_LOCAL_CONFIG_MAPPING_DB_TEST=true` and the ignored local password environment variable, then run `mvn '-Duser.timezone=UTC' '-Dtest=ConfigChangeMappingDatabaseTest' test`. PostgreSQL SERIAL identifier sequences can advance during rolled-back tests; no fixture rows remain and sequence values are not reset.

No Git push, server deployment or live Kafka config mutation is included in this implementation task.
