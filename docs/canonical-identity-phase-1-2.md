# Canonical Cluster and Node Identity — Phase 1 + 2

Status: implemented in source only; not deployed and not applied to a running database.

## Scope boundary

This phase defines the identity contract and adds an additive database binding.
It deliberately does not change resolvers, services, APIs, monitoring queries or
the frontend. Those integrations belong to Phase 3 and later.

## Canonical contract

### Cluster

| Field | Meaning |
| --- | --- |
| `clusterUuid` | Tantor's immutable UUID for this cluster |
| `kafkaClusterId` | Kafka's cluster id; may be pending during enrollment |
| `type` | `INTERNAL` or `EXTERNAL` |
| `mode` | `KRAFT`, `ZOOKEEPER` or `UNKNOWN` |

### Node

| Field | Meaning |
| --- | --- |
| `nodeId` | Kafka node id |
| `host` | Display/discovery value only |
| `role` | `BROKER`, `CONTROLLER` or `BROKER_CONTROLLER` |
| `agentStatus` | `ONLINE`, `OFFLINE`, `NOT_ENROLLED` or `UNKNOWN` |
| `telemetryStatus` | `LIVE`, `STALE`, `UNAVAILABLE` or `UNKNOWN` |

The complete and only node matching key is:

```text
clusterUuid + kafkaClusterId + nodeId + role
```

IP address, hostname, cluster name and agent name are excluded from identity.
They can change without creating a different canonical node.

## Migration preview

The Phase 2 migration is additive:

1. Add nullable `canonical_cluster_uuid UUID` to `kf_clusters`.
2. Add nullable `canonical_cluster_uuid UUID` to
   `kf_external_cluster_nodes`.
3. Backfill `kf_clusters.canonical_cluster_uuid = kf_clusters.id`.
4. Backfill each external node from its parent cluster:
   `node.canonical_cluster_uuid = cluster.canonical_cluster_uuid`, joined only
   by `node.cluster_id = cluster.id`.
5. Abort the migration if a cluster or external node remains unbound, or if a
   duplicate canonical cluster UUID exists.
6. Make both columns non-null.
7. Add a unique constraint on the cluster canonical UUID and a foreign key from
   external nodes to it.
8. Add database triggers for new rows. A cluster receives its existing `id` as
   its canonical UUID; an external node receives the canonical UUID of its
   `cluster_id` parent. Identity is never inferred from IP, hostname or name.

The migration does not change or fabricate `kafka_cluster_id`, `node_id`, role,
host, IP address, status, or any existing primary key.

## Exact read-only backfill inventory

The following inventory was read from the target database on 2026-08-18 using
SELECT queries only. The planned assignments are deterministic; no random UUID
needs to be generated for these records.

### `kf_clusters`

| Existing id | Name | Type | Kafka cluster id | Assigned canonical cluster UUID |
| --- | --- | --- | --- | --- |
| `734212c0-3bfc-42e1-928a-94b6858ee558` | `test-ext` | EXTERNAL | `Z4yJfDCjQfGewRMtOjkt8Q` | `734212c0-3bfc-42e1-928a-94b6858ee558` |
| `08d89c70-b299-445e-9614-45d602525971` | `tstkj` | EXTERNAL | `-DQUUFP3R7alxUkBdn3frw` | `08d89c70-b299-445e-9614-45d602525971` |

### `kf_external_cluster_nodes`

| Record id | Parent cluster id | Node id | Derived role | Host | Assigned canonical cluster UUID |
| --- | --- | ---: | --- | --- | --- |
| `4ec2c193-cb6d-49c3-a88b-9c1334c80f7a` | `08d89c70-b299-445e-9614-45d602525971` | 1 | BROKER_CONTROLLER | `192.168.3.150` | `08d89c70-b299-445e-9614-45d602525971` |
| `e832e072-8388-45ac-9d17-d2880647e3ae` | `734212c0-3bfc-42e1-928a-94b6858ee558` | 1 | BROKER | `192.168.3.229` | `734212c0-3bfc-42e1-928a-94b6858ee558` |
| `a4459675-77ea-482c-b889-6fa330204837` | `734212c0-3bfc-42e1-928a-94b6858ee558` | 2 | BROKER | `192.168.3.164` | `734212c0-3bfc-42e1-928a-94b6858ee558` |
| `22f9e673-25d5-48fb-ba7c-b83a8e163908` | `734212c0-3bfc-42e1-928a-94b6858ee558` | 3 | BROKER | `192.168.3.191` | `734212c0-3bfc-42e1-928a-94b6858ee558` |
| `ee53d12a-8cae-4843-a038-e97c69cdedaa` | `734212c0-3bfc-42e1-928a-94b6858ee558` | 101 | CONTROLLER | `192.168.3.229` | `734212c0-3bfc-42e1-928a-94b6858ee558` |
| `a48e7a26-70af-4ba6-b605-93d2c452dfd7` | `734212c0-3bfc-42e1-928a-94b6858ee558` | 102 | CONTROLLER | `192.168.3.164` | `734212c0-3bfc-42e1-928a-94b6858ee558` |
| `fbf7d7be-220c-4973-a561-d4548f374bbf` | `734212c0-3bfc-42e1-928a-94b6858ee558` | 103 | CONTROLLER | `192.168.3.191` | `734212c0-3bfc-42e1-928a-94b6858ee558` |

Preflight results for these active records:

- duplicate non-empty Kafka cluster ids: 0
- active clusters missing Kafka cluster id: 0 of 2
- external nodes missing node id: 0 of 7
- external nodes missing a derivable role: 0 of 7
- external nodes missing their mirrored `kf_clusters` parent: 0 of 7

For future records, the canonical UUID is assigned by relationship, not by
heuristic matching:

- new `kf_clusters` record: reuse its generated primary key `id`
- new external node: inherit the canonical UUID of its `cluster_id` parent
- an unresolvable parent: reject the write rather than guess using IP/name

## Rollback note

This is an additive migration, so the safest application rollback is to deploy
the previous application build and leave the unused columns in place. No old
column or table is removed, and existing readers remain compatible.

If a full schema rollback is required during a maintenance window:

1. Roll the application back first.
2. Drop the external-node canonical UUID trigger and its function.
3. Drop the cluster canonical UUID trigger and its function.
4. Drop the external-node foreign key and canonical UUID index.
5. Drop the cluster unique constraint.
6. Drop `canonical_cluster_uuid` from `kf_external_cluster_nodes`, then from
   `kf_clusters`.
7. Reconcile the Flyway schema-history entry using the team's controlled
   database release procedure; do not manually delete it during live traffic.

No business data needs to be reconstructed because this migration only copies
existing UUID relationships into new columns.

After the previous application build is active and writes are stopped, the
schema portion can be undone with:

```sql
DROP TRIGGER IF EXISTS trg_assign_external_node_canonical_uuid
    ON kf_external_cluster_nodes;
DROP FUNCTION IF EXISTS assign_external_node_canonical_uuid();

DROP TRIGGER IF EXISTS trg_assign_kf_cluster_canonical_uuid ON kf_clusters;
DROP FUNCTION IF EXISTS assign_kf_cluster_canonical_uuid();

ALTER TABLE kf_external_cluster_nodes
    DROP CONSTRAINT IF EXISTS fk_external_nodes_canonical_cluster_uuid;
DROP INDEX IF EXISTS ix_external_nodes_canonical_cluster_uuid;

ALTER TABLE kf_clusters
    DROP CONSTRAINT IF EXISTS uq_kf_clusters_canonical_cluster_uuid;

ALTER TABLE kf_external_cluster_nodes
    DROP COLUMN IF EXISTS canonical_cluster_uuid;
ALTER TABLE kf_clusters
    DROP COLUMN IF EXISTS canonical_cluster_uuid;
```

This SQL is a rollback note, not an automatic Flyway down migration. The
schema-history reconciliation must remain a deliberate release operation.
