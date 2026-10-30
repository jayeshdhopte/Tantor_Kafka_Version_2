# External config-version proposal — superseded

A separate external version table is **not required** for external clusters with the existing inventory mirror.

The earlier proposal correctly identified that `kf_config_versions.cluster_id` references `kf_clusters.id`, but overlooked the database mirror introduced in V49 and repaired in V65. `sync_external_cluster_to_cluster()` mirrors external inventory into `kf_clusters` with the same UUID and EXTERNAL origin. `ExternalKafkaManagement.resolve()` creates an in-memory object, while the database trigger independently maintains this persisted mirror.

The mapping now permits external targeted dynamic/static revisions when that mirror exists, reusing `kf_config_versions`. It retains the real external UUID, host identifier, native Kafka ID, request/checkpoint history, retry idempotence and lifecycle synchronization. No new table, migration or relaxed constraint is needed. The changed-property marker and full-file apply/rollback guards remain in place.

When the mirror is absent, the mapping retains ER history and skips the incompatible version insertion. It does not fabricate inventory, alter an existing FK or fail the config job merely to write history. Investigate inventory mirroring if this fallback occurs.

No historical version backfill is included. Rebuild/restart the updated backend and submit a new external targeted change. The prior 14-column separate-table design is withdrawn; do not implement it based on the old proposal.
