# Native Kafka cluster IDs alongside Tantor UUIDs

V93 adds nullable TEXT `kafka_cluster_id` columns to all 19 existing public tables that have a `cluster_id` UUID. The UUID columns, primary keys, foreign keys, checks and existing application queries/entity mappings remain unchanged. Existing inventory sources `kf_clusters.kafka_cluster_id` and `kf_external_clusters.kafka_cluster_id` are reused.

| Table | UUID relationship | Added native identifier |
|---|---|---|
| kf_activity_logs | cluster_id | kafka_cluster_id |
| kf_add_service | cluster_id | kafka_cluster_id |
| kf_alerts | cluster_id | kafka_cluster_id |
| kf_audit_logs | cluster_id | kafka_cluster_id |
| kf_cluster_audit_log | cluster_id | kafka_cluster_id |
| kf_cluster_operation_lock | cluster_id | kafka_cluster_id |
| kf_cluster_services | cluster_id | kafka_cluster_id |
| kf_config_change | cluster_id | kafka_cluster_id |
| kf_config_versions | cluster_id | kafka_cluster_id |
| kf_data_service_connections | cluster_id | kafka_cluster_id |
| kf_discovery_agents | cluster_id | kafka_cluster_id |
| kf_external_cluster_nodes | cluster_id | kafka_cluster_id |
| kf_external_kafka_tasks | cluster_id | kafka_cluster_id |
| kf_external_services | cluster_id | kafka_cluster_id |
| kf_host_alerts | cluster_id | kafka_cluster_id |
| kf_hosts | cluster_id | kafka_cluster_id |
| kf_nodes | cluster_id | kafka_cluster_id |
| kf_rolling_restart_table | cluster_id | kafka_cluster_id |
| kf_tasks | cluster_id | kafka_cluster_id |

The 11 ER tables retain their original 167 fields, plus these two additions: kf_config_change.kafka_cluster_id and kf_cluster_operation_lock.kafka_cluster_id (169 columns total across the 11 tables). V93 brings Add Service to 17 columns; V94 adds cluster_origin for a current total of 18. Standalone restart has 19.

## Population

A BEFORE INSERT / UPDATE OF cluster_id,kafka_cluster_id trigger resolves the native ID from saved inventory. This covers JPA writes, JDBC writes and audit events created by existing SQL triggers. It does not require changes to existing Java entities, DTOs, select-list order or API responses.

When an external inventory row exists, its native ID is authoritative, even if blank; a stale internal mirror is not substituted. Otherwise the internal cluster record is used. Blank or missing native IDs, missing inventory and NULL cluster associations produce NULL. A caller cannot populate an unrelated or invented ID by passing a string: the trigger resolves it from the UUID association.

Resolution is VOLATILE so nested audit-trigger chains can see inventory changes made by their calling statement. New audit events capture the ID available when inserted. They remain append-only and retain that snapshot if inventory later changes. The original immutable trigger is unchanged and enabled.

## Existing records and unknown IDs

There is **no historical backfill**. Migration V93 performs no business-table UPDATE. This preserves immutable audits and avoids invoking existing inventory update triggers just to annotate old records. Existing rows therefore initially have NULL in the added columns. New records resolve the current known ID; later writes that include cluster_id or kafka_cluster_id refresh the field on mutable rows. Status-only SQL updates do not refresh it. Learning a Kafka ID later does not bulk-update older records or audit events.

The ID comes from actual saved Kafka/discovery observations; no cluster name, UUID or bootstrap address is substituted. Tables without a cluster_id column do not receive an invented cluster association. Their existing relationships remain available for joins.

## Verification and deployment

The existing local DB is 127.0.0.1:5433/tantor. V93 is applied there with Flyway validation enabled. Database scenarios run in rollback-only transactions, including audit insertion and rejected update/delete checks. No fixtures are committed and no audit cleanup or trigger bypass is used.

Only the migration, tests and documentation change in this phase. The two old opt-in tests that count SELECT * columns are updated for the explicitly added column (16→17 and 18→19); their prior field assertions remain. No application Java source or frontend source changes. No remote DB, deployment or Git push is included.
