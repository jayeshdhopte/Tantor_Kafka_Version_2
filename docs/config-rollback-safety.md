# Configuration rollback safety

Configuration retries and rollback requests are rejected with HTTP 409 when a newer configuration job on the same cluster has succeeded or has dispatched a configuration mutation without completing its rollback. This deliberately blocks recovery of the whole older job, including when newer values happen to equal the older values. Use a fresh reviewed change based on the current configuration instead.

The job details response exposes `configRecoveryBlockedReason` for failed configuration jobs. The UI keeps refreshing these jobs, disables retry and rollback when blocked, and displays the reason with the newer job ID. Historical job status, logs, and checkpoints are retained.

Recovery checks supersession again before execution. Each affected node also retains its live value/source checks, backup checks, and health verification. Values changed outside the job are rejected. Already restored nodes are verified without rewriting overrides, and untouched nodes are left unchanged. A failed recovery remains failed; it is not presented as a successful restore.

The reported November 1 failure involved a leading space in `message.max.bytes`: the requested value was `" 15728679"`, while Kafka returned `"15728679"`. Exact comparisons incorrectly failed both forward verification and ownership validation during recovery. Node 1 was changed; Nodes 2 and 3 had not received a mutation. The later retry after v3 succeeded was stopped by the existing live-value guard, but the UI still offered it.

The integer byte limits `message.max.bytes` and `log.segment.bytes` now use canonical decimal values in preview, writes, verification, and legacy rollback ownership comparisons. Other properties retain exact string semantics; whitespace in string settings is not silently removed. Existing checkpoint values and historical request records are not rewritten.

Regression coverage includes stale request rejection before state changes, execution-time rejection before broker access, actual PostgreSQL selection of newer applied jobs, numeric readback, legacy whitespace recovery, live-value conflicts, idempotent recovery, and UI action guards and refresh. PostgreSQL tests require an isolated database URL containing `_check_`; fixtures are rolled back.
