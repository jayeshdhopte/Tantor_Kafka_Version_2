# External Kafka execution evidence retention

V104 separates disposable registration ownership from historical execution identity.
Previously the external-cluster foreign key cascaded task deletion, and the job-step
foreign key then cleared its reference. Successful jobs remained without their task evidence.

External cluster removal now detaches `kf_external_kafka_tasks.cluster_id` instead
of deleting tasks. Agent registration removal similarly detaches `agent_id`.
`source_cluster_id` and `source_agent_id` retain the original identities as immutable
historical values, deliberately without foreign keys to disposable registrations.
Job steps retain their original `external_kafka_task_id`; the task's command,
parameters, result message, status and creation/deadline timestamps remain queryable.
There is no new task timestamp that claims to be its original completion time.

Detachment sets `archived_at`, removes the claim token, and makes execution evidence
immutable. Pending/in-progress tasks become FAILED with an explicit archival message,
not a fabricated success or confirmation that host-side work stopped. Already
running host processes still require inspection. Archived tasks cannot be claimed,
reassigned or completed by late callbacks. Removing the other registration later is
allowed without changing evidence. A job-step FK rejects direct deletion of referenced
tasks rather than silently clearing history.

Retention is indefinite with retained job history; there is no automatic TTL or
archived-task purge. A future approved retention purge must handle dependent job
history explicitly. PostgreSQL owners can bypass triggers or TRUNCATE tables; these
are not supported application cleanup procedures. Do not truncate execution evidence
when resetting registration inventory.

The migration populates provenance only from existing task rows. It cannot recover
rows or references already deleted before V104. Recover those only from a backup
containing the exact task and matching retained job-step IDs; never infer task IDs
from timestamps or recreate successful execution evidence from guesses.

For retained evidence:

```sql
SELECT s.job_id, s.id AS step_id, s.status AS step_status,
       t.id AS task_id, t.source_cluster_id, t.source_agent_id,
       t.command, t.status AS task_status, t.message, t.archived_at
FROM kf_job_steps s
JOIN kf_external_kafka_tasks t ON t.id = s.external_kafka_task_id;
```

Internal agent execution and external data-service tasks are separate tables and
are outside this migration. No agent binary changes are needed.
