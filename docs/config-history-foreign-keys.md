# Config Change and Rolling Restart foreign keys

V100 adds **23 direct foreign keys** and **11 ownership foreign keys** to the existing history tables. It preserves primary keys, nullable fields, identifiers, and all existing rows. The complete machine-readable relationship list is [config-history-foreign-keys.json](config-history-foreign-keys.json).

| Child | Enforced parents |
| --- | --- |
| kf_config_change | kf_clusters |
| kf_config_change_detail | Config Change, config catalog |
| kf_config_change_target_node | Config Change |
| kf_config_change_node_value | Config Change, detail, target |
| kf_config_execution_attempt | Config Change, detail, target |
| kf_config_backup | Config Change, detail, target |
| kf_cluster_operation_lock | cluster, Config Change |
| kf_health_check | Config Change, target |
| kf_rolling_restart_operation | Config Change, current target |
| kf_rolling_restart_node_step | Config Change, operation, target |
| kf_rolling_restart_table | cluster |

Direct FKs protect every populated reference, even when other nullable fields are unresolved. Composite FKs additionally require a detail, target, restart operation, or cluster association to belong to the same change. A valid detail UUID from another change is rejected. Nullable fields remain nullable; this migration does not invent missing facts or impose the prototype's old NOT NULL rules.

The FKs use `ON DELETE NO ACTION`, `ON UPDATE NO ACTION`, and `DEFERRABLE INITIALLY DEFERRED`. Configuration projection code creates some children before their operation parent within one transaction; validation occurs at transaction completion. Unresolved references still reject commit. New constraints are validated against the existing data before migration succeeds. Child-side lookup indexes are added only where an existing non-partial index does not already cover the leading columns.

Cluster cleanup clears live host, connection, node, and service assignments. When Add Service, Config Change, or standalone Rolling Restart history references a cluster, the parent remains with status DELETED rather than being physically removed. Historical records and original cluster UUIDs remain intact. Active inventory excludes these retired parents, and external clusters can be registered again under a new UUID. Parents with no such history continue to be physically purged.

`kf_config_change_target_node.node_id` remains a nullable historical inventory UUID snapshot, rather than a foreign key to ephemeral `kf_nodes`. Cleanup deliberately removes live inventory nodes; frozen roles and job/task payloads retain historical identity. The stable history target itself (`target_id`) is protected by the new FKs. Likewise, the immutable global audit ledger is not made a dependent child of purgeable configuration history in this migration. These two independent historical references are intentionally documented rather than represented as live-inventory FKs.

The missing enforcement originated when V90 replaced the prototype with the nullable projection schema without recreating its FKs. Normal application writes produced consistent records, but the database did not reject invalid references. V100 restores physical enforcement for the relationships above; it neither repairs nor deletes inconsistent records if validation fails.
