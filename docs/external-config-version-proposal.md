# External config-version storage proposal — not implemented

## Proven schema gap

`kf_config_versions.cluster_id` is UUID NOT NULL with `config_versions_cluster_id_fkey` referencing `kf_clusters(id)`. External clusters are stored in `kf_external_clusters`. `ExternalKafkaManagement.resolve()` constructs an in-memory Cluster using the external ID; it does not insert that ID into `kf_clusters`. Therefore writing the actual external ID into the existing config-version table fails its foreign key and would roll back the accepted configuration job.

The internal mapping now explicitly leaves external targets in their existing ER history. It does not insert a substitute cluster, change a cluster ID or bypass any constraint. No migration is included in this proposal.

## Recommended additive change

Create **one new table**, `kf_external_config_versions`, for job-linked changed-property revisions. This is a targeted-change history table, not a full-file version/restore engine.

| Proposed column | Type | Source / meaning |
|---|---|---|
| id | UUID primary key | Stable revision identifier derived from existing job and target identity |
| cluster_id | UUID | Actual `kf_external_clusters.id` from job payload.clusterId |
| host_id | VARCHAR(100) | Accepted step payload.hostId (`external:<discovered-node UUID>`) |
| component | VARCHAR(100) | Accepted step payload.role |
| config_file_name | VARCHAR(500) | Accepted step payload.configPath; informational path for dynamic changes |
| config_version | INTEGER | Next revision for this cluster/host/component/path |
| old_config | JSONB | Changed properties' before-values, refined from immutable checkpoints; sensitive or unavailable values NULL |
| new_config | JSONB | Requested changed properties; NULL denotes removal or redaction, distinguished by metadata |
| status | VARCHAR(50) | Existing step lifecycle mapped to the same revision statuses as internal targeted history |
| validation_result | JSONB | Partial-history marker, execution type, node/step identity, before sources, redacted keys; no invented validation result |
| job_id | VARCHAR(36) | Actual `kf_jobs.id` |
| created_by | VARCHAR(255) | Original job requester |
| created_at | TIMESTAMPTZ | Original job creation time |
| applied_at | TIMESTAMPTZ | Actual successful node-step completion time, otherwise NULL |

All columns except the primary key would be nullable, with no business-data defaults. Cluster, host and job IDs would be plain columns, with no FK or CHECK on existing tables. A uniqueness rule on the **new table only** for `(cluster_id,host_id,component,config_file_name,config_version)` and an index on `job_id` would support safe numbering and lookup. Transactional namespace locking and stable revision IDs would make callbacks/retries idempotent.

`service_id`, approval fields and `rollback_version` are omitted: this flow has no persisted internal service assignment, no approval stage, and recovery belongs to the original job/checkpoints. The external node UUID is already preserved by host_id. No invented values or duplicate ownership structures are needed.

## Follow-on implementation, only after approval

Route external targeted revisions and lifecycle/checkpoint updates to the new table; keep internal writes in `kf_config_versions`. Add an external history read path with the existing authorization model, without treating changed-property snapshots as full configuration files or changing internal version queries. Continue using the current config engine, Kafka/agent commands, audit and ER history.

Test dynamic/static changes, removals, retries, partial failure, rollback, redaction and actual external IDs against the existing local Docker DB in rollback-only transactions. Compare existing suites and existing data/schema before and after. No historical backfill, remote deployment or Git push is included.

Adding only `external_cluster_id` to `kf_config_versions` is insufficient: its existing cluster_id would still be required and constrained to internal IDs. Making that field nullable or replacing its FK conflicts with the additive-only rules. Creating shadow internal clusters would fabricate inventory. Neither is proposed.

**Approval required before adding this table, a migration or the external history API.**
