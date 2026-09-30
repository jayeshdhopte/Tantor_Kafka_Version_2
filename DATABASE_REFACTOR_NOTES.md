# Deferred Database Refactor Requirements

These requirements are recorded for later implementation. No database or application changes should be made until the table-mapping review is complete.

## Artifact table

Retain:

- `id`
- `service_type`
- `version`
- `file_name`
- `relative_path`
- `full_file_path`
- `file_size_bytes`
- `content_type`
- `checksum_sha256`
- `checksum_md5`
- `status`
- `created_by`
- `created_at`
- `updated_by`
- `updated_at`

Remove later:

- `name`
- `classifier`
- `manifest`
- `description`
- `version_lock`

### Path requirement

- Keep `relative_path` as the portable repository-relative location.
- Add and display `full_file_path` as the complete resolved filesystem path.
- The full path must never be hardcoded.
- Resolve it dynamically from the configured artifact repository base path plus `relative_path`.
- Update upload/storage handling so both values remain consistent.

### Artifact uniqueness and append-only lifecycle

- The `artifact` table will operate as an append-only artifact action ledger.
- Every completed action creates a new row; existing rows must never be overwritten.
- Add an `action` column to distinguish `UPLOAD`, `DOWNLOAD`, and `DELETE` rows.
- Add a stable artifact/root reference so all action rows for the same binary can be grouped together.
- The first valid upload creates the initial `UPLOAD` row.
- Reject a later binary upload when the same `service_type` and `version`, or the same SHA-256 checksum, already exists as an uploaded artifact.
- Rejected duplicate uploads do not create another successful upload row; their rejection may be recorded as an audit event.
- Every download creates a new `DOWNLOAD` row linked to the original artifact/root reference.
- Every delete creates a new `DELETE` row linked to the original artifact/root reference.
- Deletion must not update or remove earlier upload/download rows.

## Artifact audit log

Retain:

- Audit log `id`
- `actor`
- `event_category`
- `action`
- `resource_type`
- `resource_id`
- `status`
- `details`

Remove later from both database and UI:

- `old_value`
- `new_value`
- `approval`
- `ip_address`
- `source`
- `previous_hash`
- `record_hash`

### Mapping

- One artifact may have many audit events.
- The audit event ID identifies the event.
- `resource_id` identifies the related artifact.
- Hash chaining is intentionally deferred/removed for the simplified initial audit design.

## Artifact download log

- Remove the separate `artifact_download_log` table later.
- Move its required download information into append-only `artifact` action rows.
- Add `downloaded_by`, `downloaded_at`, and `verified_checksum` to the artifact ledger as nullable action-specific columns.
- Remove `remote_address` completely.

## Host parcels

Current fields under review:

- `host_id`
- `artifact_id`
- `service_type`
- `version`
- `file_name`
- `artifact_url`
- `checksum`
- `parcel_dir`
- `status`
- `active`
- `last_task_id`
- `created_at`
- `updated_at`
- `updated_by`
- `host_ip`

### Artifact URL requirement

- `/api/v1/artifacts/{artifactId}/download` is an HTTP API route, not a host filesystem path.
- Do not hardcode an artifact repository hostname, IP address, port, or `localhost` in `host_parcels`.
- Remove `artifact_url` from `host_parcels`; `artifact_id` is the stable database reference.
- Generate the complete download URL dynamically when dispatching a task.
- Build it from the configured artifact repository base URL plus `/api/v1/artifacts/{artifactId}/download`.
- The generated URL must be reachable from the target Linux agent, for example `http://<repository-host>:8081/api/v1/artifacts/{artifactId}/download`.
- Keep repository configuration environment-specific through configuration/environment variables.

### Updated-by requirement

- Add `updated_by` to `host_parcels`.
- Default it to `system` for now.
- When authentication is implemented, populate it with the authenticated username/service identity.

### Append-only parcel history

- Add `created_by` and `updated_by` to `host_parcels`, defaulting both to `system` until authentication is implemented.
- Every parcel action or state change must create a new `host_parcels` history row; do not overwrite the earlier row.
- Preserve distribution, activation, deactivation, removal, failure, and retry records separately.

### Distribution destination behavior

- Artifact upload must allow an authorized repository destination directory to be selected through the UI/server configuration.
- The upload destination must be dynamic and must not be hardcoded.
- Restrict selectable upload destinations to configured safe repository roots to prevent arbitrary filesystem writes.
- The artifact distribution panel must provide one master destination-path input above the host list.
- The master path applies to every selected host unless that host has its own override.
- Every host row must also provide its own destination-path input.
- A host-specific path overrides the master path only for that host.
- Store the effective destination path in each append-only parcel action row.

### Distribution target selection

- Show every eligible host/node under the expanded artifact row.
- Add a checkbox beside each host.
- Support selecting any subset of hosts and distributing to only those checked hosts.
- Provide select-all/clear-all behavior when useful.
- Keep the individual host Distribute action.
- `Distribute All` must work with one, two, or any number of eligible hosts.
- `Distribute All` must use the master destination path, except where a host-specific override is present.
- Fix current eligibility/state logic so failed, removed, or retryable parcel states do not incorrectly prevent distribution.

### Artifact deployment visibility

- Artifact deployment history must show the numeric host ID, agent ID, hostname, host IP/bootstrap address, destination path, action, and status for every target node.
- Because one artifact can target many hosts, deployment-host information must be stored per append-only artifact/parcel action rather than as one overwriteable value on the artifact upload record.

## Hosts

Target identity model:

- Change `hosts.id` to a numeric generated primary key.
- Add an immutable, unique `agent_id` generated during enrollment.
- Keep a unique `agent_name`, such as `agent-node-208`, permanently linked to that `agent_id`.
- Remove the currently generated metadata value such as `tantor-agent@broker1.translab.io`; it is not the required agent name.
- Agent registration, heartbeat, polling, and task delivery use `agent_id`.
- The UI displays both `agent_id` and `agent_name`.
- Database relationships should use the numeric host `id` as their single foreign key.
- Do not duplicate both `agent_id` and `agent_name` into every related table; resolve them through the host relationship to avoid inconsistent copies.
- Remove the obsolete/unclear `is_monitored` column if it exists in a deployed database; it is not present or used in the current entity/migration code.
- Migrate every current string host relationship safely: `tasks.host_id`, `host_parcels.host_id`, `cluster_services.host_id`, configuration references, and agent endpoints.
- Do not perform this as a one-column type change because current foreign keys and server lookups depend on the string agent identity.

UI requirement implemented during mapping review:

- Remove `Remove node` from the three-dot action menu for connected hosts.

### Host lifecycle history

- Host registration/entry and host removal/disconnection must create separate append-only database history entries.
- Do not overwrite an earlier lifecycle event when the host is re-registered, enabled, made unavailable, made available, disconnected, or removed.
- Default lifecycle-event actor fields to `system` until authentication is implemented.

## Clusters

Retain:

- `id`
- `cluster_name`
- `created_at`
- `updated_at`
- `kafka_version`
- `mode`
- `environment`
- `bootstrap_servers`
- required port fields
- `status`
- `deleted_at`
- `origin_type`
- `kafka_cluster_id`
- `install_directory`
- `config_directory`
- `data_directory`
- `log_directory`

Remove later:

- `config_json`
- `external_broker_hosts_json`

Required additions/refactor:

- Add `artifact_id` as a foreign key/reference to the Kafka artifact binary used to deploy the cluster.
- Display this artifact ID as the deployed binary/resource ID in cluster details.
- Create a separate `external_clusters` table for external-cluster-specific information.
- Move external broker/host information out of `clusters` into normalized external-cluster tables rather than JSON.
- Removing `config_json` requires moving the currently used deployment settings into explicit columns or normalized configuration tables before dropping it.
- Add one cluster-level field containing the complete list of host IP addresses participating in the cluster, for example IP1, IP2, and IP3.
- Populate that host-IP list from the selected deployment hosts and keep it synchronized when nodes are added or removed.
- Each participating `hosts` row must also retain the corresponding `cluster_id`; this relationship already exists and must be verified/enforced.
- The exact database type/name for the cluster host-IP list will be decided during implementation.

## Cluster services

- Convert `host_id` from the current string agent identity to the numeric host foreign key.
- Add/store the linked immutable `agent_id` discussed in the host identity model.
- Preserve the service assignment relationship to cluster, host, role, and Kafka node ID.
- Validate `host_id` and `agent_id` against the same host during writes so they cannot point to different agents.

## Management audit logs

Remove later from the database and UI:

- `ip_address`
- `previous_hash`
- `record_hash`
- approval fields, as already requested globally

Integration clarification:

- Management audit logs and artifact audit logs are separate tables.
- They are exposed by separate backend endpoints/services.
- The Audits UI fetches both sources and combines them into one displayed event list.
- They do not currently write to one common endpoint.
