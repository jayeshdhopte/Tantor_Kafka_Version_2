> This document describes the original V92 summary mapping. Later releases added foreign keys and shared operation history. For current standalone safety-policy and individual health-check persistence, see [V102 safety evidence](rolling-restart-safety-evidence.md).

# Standalone rolling restart mapping

The cluster three-dot menu creates a standalone `ROLLING_RESTART` job. V92 adds one summary table, `public.kf_rolling_restart_table`, to the existing local database at `127.0.0.1:5433`, database `tantor`. It does not replace job, step, task or audit storage and does not backfill older jobs.

## Diagram mapping

The diagram lists `updated_at` twice; it is included once, giving 18 columns. UUID identifiers and TIMESTAMPTZ timestamps are used. Host IDs and roles per node are JSONB arrays to represent a multi-node restart in one row. Other fields are TEXT. Only the primary key is required; there are no defaults, foreign keys, CHECK constraints or changes to existing table definitions.

| Column | Source/meaning |
|---|---|
| unique_id | Existing restart job ID, not a separate generated identifier |
| cluster_id | Cluster being restarted |
| cluster_name | Inventory name captured when the request is created |
| status | Existing job status, including PARTIAL_SUCCESS |
| type | New for managed inventory; Existing for external/discovered inventory |
| mode | Inventory Kafka mode |
| env | Inventory environment; NULL if absent |
| bootstrap_server_ip | Complete inventory bootstrap address string; not the selected-target list |
| user | Original application requester from `kf_jobs.requested_by`; not the OS service account |
| updated_by | Original requester, then authenticated retry requester; worker/recovery transitions preserve it |
| created_at | Existing job creation time; preserved across retries |
| updated_at | Initial job update time, then actual summary status-transition time |
| role_per_node | Array associating each selected process with step ID, host ID, node ID, role, service name and available display identity |
| log_path | NULL; log content remains in existing job/step/task columns |
| host_id | Array of distinct selected target IDs, preserving IDs such as `external:<node UUID>` |
| old_config_path | NULL; standalone restart neither changes nor backs up configuration |
| action | Existing operation type, ROLLING_RESTART |
| resource_type | CLUSTER |

For external clusters, the name, mode, environment and bootstrap string come from external inventory. Credentials, configuration JSON and security settings are not copied.

## Lifecycle and audit

Creation is in the same transaction as the existing restart job and steps. Summary creation uses the existing job ID and ignores duplicates, preserving its original metadata and creation time.

The existing job service updates the summary on claim/start, terminal outcome, retry/resume and recovery after a backend restart. Status updates affect existing summaries only; they never implicitly backfill historical jobs. Unchanged status does not produce another audit event. Other job types, including config-change jobs, do not write to this table.

After a summary transaction commits, the existing `AuditService` inserts an event into `kf_audit_logs`. No additional audit table or audit column is created. Actions include ROLLING_RESTART_REQUESTED, ROLLING_RESTART_STARTED, ROLLING_RESTART_COMPLETED, ROLLING_RESTART_FAILED, ROLLING_RESTART_PARTIAL_SUCCESS, ROLLING_RESTART_RETRY_REQUESTED and ROLLING_RESTART_RECOVERED.

Audit details include the job ID, cluster name, selected hosts/roles and exact restart status. Audit SUCCESS on a requested/started/retried event means that action succeeded; it does not mean the restart has completed. Failed/partially successful job outcomes have audit status FAILED. Rolled-back summary writes generate no audit event. After-commit audit-storage failures are logged and do not turn an already committed operation into an API failure.

The existing UI, API responses, execution order, health checks, process commands, per-node logs and retry evidence continue using their current code/storage. No existing entity/DTO column mapping or SQL select list was changed. The two config-change ER restart tables from V90 are separate and are not written by this implementation.

## Inspection

Refresh the existing pgAdmin connection on port 5433, then public Tables. New requests through the updated local backend populate the table:

```sql
SELECT unique_id, cluster_name, status, type, mode, env, "user",
       updated_by, host_id, role_per_node, created_at, updated_at
FROM public.kf_rolling_restart_table
ORDER BY created_at DESC;

SELECT created_time, action, status, created_by, details
FROM public.kf_audit_logs
WHERE event_category = 'RESTART'
  AND action LIKE 'ROLLING_RESTART_%'
ORDER BY created_time DESC;
```

The opt-in `RollingRestartMappingDatabaseTest` connects only to `127.0.0.1:5433/tantor`. It verifies real summary and audit INSERTs, retry identity and duplicate suppression inside one rollback-only transaction. It directly invokes the real audit writer within that test transaction; separate tests verify production AFTER_COMMIT delivery and rollback suppression. It performs no DELETE and respects the existing append-only audit trigger. No new database is created. Enable it with `TANTOR_LOCAL_RESTART_DB_TEST=true` and the ignored local database password environment variable, then run `mvn '-Duser.timezone=UTC' '-Dtest=RollingRestartMappingDatabaseTest' test` in `tantor-server`. UTC avoids the local Java/PostgreSQL disagreement over the Asia/Calcutta timezone alias, without changing application or database settings.

No Git push or server deployment is part of this change. Rebuild/restart the local backend to use the updated application code.
