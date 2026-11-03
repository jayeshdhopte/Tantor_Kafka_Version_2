# Shared Config Change and Rolling Restart protection

The implemented mode is **JOB_RESOURCE_SERIALIZATION**, not a distributed cluster lease or monotonic fencing-token protocol. `fencing_token` and `lease_expires_at` remain null. `attempt_number` identifies retries within a job and must not be interpreted as a fencing token.

## Execution protection

1. Cluster-row locking and `kf_jobs.uq_jobs_active_resource` serialize conflicting jobs from their PENDING state through execution and rollback. Legacy external-service resource keys are checked by the same application gate.
2. A terminal job with unfinished agent work continues to block another cluster mutation. Both internal task bindings and `external_kafka_task_id` are checked, together with durable forward/recovery task references. Standalone restarts and configuration jobs use the same task-drain function.
3. The live operation ledger additionally permits only one ACTIVE/WAITING_AGENT owner per cluster. A timeout does not expire cluster ownership while agent work can still be running. Agent-task claim tokens are separate task/callback ownership controls, not cluster fencing tokens.
4. Configuration and restart dispatch boundaries validate the persisted executing job and expected forward/rollback phase before creating a mutating task. Superseded configuration recovery is rejected, and existing live property/source checks prevent old rollback values from replacing newer configuration.

This protects supported Tantor mutation workflows. It does not claim to fence an arbitrary stale process under a distributed lease design, or coordinate direct SSH/database writes outside those workflows. Full distributed fencing would require an enforced token contract at every mutation executor, including agents and direct Kafka Admin API writes; this release does not populate an unenforced token to imply that guarantee.

## Lock-history persistence

`kf_cluster_operation_lock.job_id` identifies both operation families. `change_id` remains a genuine Config Change FK and is null for standalone Rolling Restart. Forward execution and rollback use separate `execution_phase` values; each job retry gets a separate `attempt_number`. A retry never moves a previously closed interval's release time.

Live acquisition is recorded when accepted ownership is observed inside its transaction, not backdated to when a job object was constructed. `acquired_by` retains the job's initiating user; retry-specific actors remain available in their audit history.

ACTIVE intervals become WAITING_AGENT if the job ends while its agent work remains unsettled. Task callbacks release that interval when the work becomes terminal. A resume of the same job transfers ownership into its next attempt without admitting another job. Job and task/step bindings commit atomically with their ledger updates.

V101 reconstructs historical attempt intervals from timestamped job logs and available job completion times. These rows are explicitly marked `RECONSTRUCTED_JOB_LOG`; they describe recorded execution intervals rather than asserting an historical distributed lease. Existing aggregate lock rows are preserved verbatim in `legacy_snapshot` before correction. Unrecoverable history is not assigned invented fencing tokens.

The old v2/v3 overlap resulted from one mutable aggregate row: v2 kept its original acquisition time while a later rollback retry moved its release time beyond v3. Separate intervals expose the actual sequence instead. Original jobs, logs, configuration history, restart summaries, and audit events are not rewritten by reconstruction.

Cluster cleanup retains parents referenced by operation history, alongside Add Service, Config Change, and standalone restart history. FKs remain validated and nullable metadata stays nullable.
