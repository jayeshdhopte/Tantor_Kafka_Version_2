# Configuration change audit mapping

The shared audit ledger is `kf_audit_logs`, which also supplies the UI Audit Trail. `kf_config_change_audit` is a configuration-focused view of the same records. The ER diagram's `KA_AUDIT` is represented by this mapping rather than a second independent audit table or `kf_cluster_audit_log`.

| Design field | Stored field |
| --- | --- |
| change_id | `kf_audit_logs.change_id`, `resource_id` |
| cluster_id | `cluster_id` (Tantor UUID); native ID is separate |
| actor_id | `created_by`; full initiating identity in `details.actor_id` |
| event_type | `action` |
| before_state / after_state | redacted JSON in `details` and corresponding view columns |
| correlation_id | `correlation_id`, using the configuration change/job UUID |
| execution outcome and time | `status`, `created_time`; per-node status and verification in `details` |

Future accepted changes and actual status transitions generate audit entries in the same transaction as their configuration history. Deferred triggers capture the complete plan and verification projection at transaction completion. Identical status updates do not generate extra events. A rolled-back database transaction leaves neither a committed change nor a phantom audit event. The initiating user remains the actor during asynchronous execution; `details.executor` records the management server separately.

Each event includes affected host/node identities, property names, requested values, before values and sources, and observed values and verification status where recorded. A requested value is not treated as proof of application: unverified observed values stay null. Sensitive property names and explicit sensitivity flags both suppress values. Raw task payloads, credentials, and task logs are not copied into audit records.

V98 reconstructs one current-state snapshot per pre-existing mapped change, including successful jobs that had no audit event. These entries use the stored event/completion time and explicitly record `reconstructed=true`, the evidence source, and the actual capture time. It does not invent missing intermediate transitions or modify original job, configuration, or audit rows. Replaying the same snapshot ID cannot overwrite an event.

`previous_hash` and `event_hash` are not implemented. The existing API reports integrity as `NOT_ENABLED`; this change does not claim cryptographic tamper evidence. Audit history remains read-only in the UI.
