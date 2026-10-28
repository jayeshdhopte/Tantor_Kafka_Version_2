# Local configuration-change database

The working checkout is `tantor_kafka_VERSION_2_LATEST`. Its isolated PostgreSQL
16.14 container is `tantor-kafka-v2-db-local`, published at `127.0.0.1:5433`.
The database is `tantor`; credentials are in the ignored `.env.db.local` file.

From the repository root, start the database and apply pending migrations:

```powershell
docker compose --env-file .env.db.local -f compose.db.local.yml -p tantor-kafka-v2-local up -d database
docker compose --env-file .env.db.local -f compose.db.local.yml -p tantor-kafka-v2-local run --rm migrate
```

V89 adds 11 configuration workflow tables with all 167 column names and types
from the Draw.io `Config_Change` page. The local migration runner uses the same
Flyway 10.10.0 version as Spring Boot 3.3.0 in this project. It does not seed a
Kafka property catalog or start a configuration-change
worker. UI/API, catalog population, dynamic application, file writes, restart
execution, recovery and audit event writing still need application changes.

## Inventory and request data

The existing tables describe installed/discovered resources:

| Table | One row represents | Identity |
|---|---|---|
| `kf_clusters` | One cluster | `id` UUID |
| `kf_nodes` | One Kafka node in a cluster, including its Kafka node number, role, host and paths | `id` UUID; `node_id` is the separate integer Kafka node number |
| `kf_hosts` | One host/agent, including address, heartbeat and resource metrics | `id` VARCHAR |

Selecting a cluster reads its node records and follows each node's `host_id` to
the host/agent. It does not copy cluster data into nodes or hosts. One host can
have more than one service/node record; a node may be broker-only,
controller-only or a combined process. Property metadata in `kf_config_catalog`
supplies the version/role-specific update capability used to derive the
execution type. Inventory itself does not classify a property as dynamic/static.

For two properties selected on three nodes, the request has one
`kf_config_change`, two `kf_config_change_detail` rows, three
`kf_config_change_target_node` rows and six `kf_config_change_node_value` rows.
Dynamic and static changes share these tables. Static changes can additionally
link to restart-operation/node-step rows.

## New tables

| Table | Purpose |
|---|---|
| `kf_config_catalog` | Versioned property definitions, applicability, update capability, rules and sensitivity |
| `kf_config_change` | Request identity, selected cluster/scope, overall state and requester |
| `kf_config_change_detail` | One property operation and its catalog snapshot |
| `kf_config_change_target_node` | One selected node and its frozen role/order/status |
| `kf_config_change_node_value` | One property × node: before, desired and verified values with value sources |
| `kf_cluster_operation_lock` | Lease history and fencing tokens for cluster operations |
| `kf_health_check` | Cluster/node validation observations and evidence |
| `kf_config_execution_attempt` | Execution phases, retries, idempotency keys, responses and errors |
| `kf_config_backup` | Property/file recovery data, backup location, permissions and checksums |
| `kf_rolling_restart_operation` | Restart operation associated with one configuration change |
| `kf_rolling_restart_node_step` | Planned/actual order and lifecycle of one restart target |

## Corrections to the diagram

- Cluster foreign keys use `kf_clusters.id`; target-node foreign keys use
  `kf_nodes.id` (UUID), never the integer Kafka node number.
- Uniqueness is scoped: `(change_id, config_key)`, `(change_id, node_id)` and
  `(detail_id, target_id)` permit multiple properties and nodes per request.
  Restart order is unique inside an operation, not across the whole database.
- Composite foreign keys prevent a value, attempt, backup, health check or
  restart step from linking to another request's detail/target. Node membership
  is checked when targets are selected or reassigned. Changing a request's
  cluster with incompatible existing targets is rejected. Later inventory
  movement does not rewrite historical target snapshots.
- Cluster leases use partial uniqueness for unreleased rows, so released lease
  history can be retained and a later operation can acquire another lease.
  An expired lease requires explicit recovery/release. The schema does not yet
  coordinate these leases with the existing `kf_jobs` resource locks or enforce
  fencing in workers/agents.
- Sensitive property values use secret references rather than plaintext fields.
  Request/response JSON and backup files still require redaction/encryption by
  the future application implementation.
- Mutable workflow rows with `updated_at` have a timestamp trigger.
  `version_no` is an application-managed optimistic concurrency counter.
- Statuses and execution policies remain VARCHAR fields as drawn. The final
  state machine, transition rules, version-range validation and catalog
  immutability rules still need agreement and application enforcement.

## Audit mapping

The diagram labels audit as `KA_AUDIT`. Existing `kf_audit_logs` remains the
audit store, with nullable `change_id` and `correlation_id` added for new events.
Existing events and writers are compatible because these columns are optional.
`kf_config_change_audit` is a view exposing all the diagram's audit field names:

| Diagram field | Existing storage/view mapping |
|---|---|
| `audit_id` | `kf_audit_logs.id` (existing UUID retained instead of BIGSERIAL) |
| `cluster_id`, `change_id`, `correlation_id` | Direct columns |
| `actor_id` | `created_by` |
| `event_type` | `action` |
| `entity_type`, `entity_id` | `resource_type`, `resource_id` |
| `event_payload` | `details` JSONB |
| `before_state`, `after_state` | Optional redacted entries in `details` |
| `previous_hash`, `event_hash` | Optional entries in `details` |
| `occurred_at` | `created_time` |

The view generates no audit events or hashes and does not validate or enforce
hash-chain uniqueness. That behavior requires the future audit event writer;
these fields are NULL until supplied. New audit links must match the change's
cluster. No second audit dataset is created.

The diagram's informal names `kf_config_properties`, `kf_target_node_value`,
`kf_cluster_lock`, `kf_cluster_health` and `kf_backup` map to the catalog,
node-value, operation-lock, health-check and config-backup tables above.
Recovery attempts use execution-attempt rows and captured backups; agent
heartbeat/status remains in host inventory. No separate `kf_rollback` or
`kf_agent_status_rolling` table is added. Existing `kf_config_versions`,
`kf_jobs` and `kf_job_steps` remain in place while application integration is
designed; this migration does not route existing writes into the new tables.

## Inspect in pgAdmin

Refresh `tantor` → Schemas → public → Tables after migration. The audit alias is
under Views. Column comments explain the inventory ID mapping and table roles.
All new tables start empty; existing server data is not copied into this local DB.

```sql
SELECT version, description, success
FROM flyway_schema_history ORDER BY installed_rank DESC LIMIT 1;

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND (table_name LIKE 'kf_config_%'
       OR table_name IN ('kf_cluster_operation_lock', 'kf_health_check',
                        'kf_rolling_restart_operation', 'kf_rolling_restart_node_step'))
ORDER BY table_name, ordinal_position;
```

## Schema validation

The validation script exercises two properties across three nodes, repeat
requests for the same node/property, cross-request reference rejection,
sensitive-value storage checks, cluster/node checks, cluster-wide retries,
file/property backups, restart ordering, lease exclusion/history/fencing tokens,
timestamp triggers and audit aliases. It wraps fixture records in a transaction
and rolls them all back. PostgreSQL sequence numbers may advance despite the
rollback; gaps are expected.

```powershell
Get-Content -Raw scripts/validate-config-change-schema.sql | docker exec -i tantor-kafka-v2-db-local psql -X -v ON_ERROR_STOP=1 -U tantor_dev -d tantor
```
