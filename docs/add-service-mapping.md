# Add Service database mapping

Local implementation for Schema Registry and Kafka Connect on `VERSION_2_LATEST`.

## Storage

V91 adds `public.kf_add_service` to the existing local database (`127.0.0.1:5433`, database `tantor`). It has the diagram's 16 fields. Only `add_service_id` is required. There are no foreign keys, CHECK constraints or defaults. The diagram did not specify SQL types: identifiers use UUID, `created_time` uses TIMESTAMPTZ, `secured` uses BOOLEAN and other fields use TEXT.

V93 adds `kafka_cluster_id` and V94 adds nullable TEXT `cluster_origin`, bringing the current total to 18 fields. `cluster_origin` is `EXTERNAL` when the UUID belongs to external-cluster inventory; otherwise the saved internal inventory `origin_type` supplies `INTERNAL` or `EXTERNAL`. Unclassifiable/missing inventory stays NULL. External inventory takes precedence over its internal mirror.

V94 fills the new origin field for existing rows whose inventory proves their classification. No prior field or audit row is changed. An insert/update trigger also resolves it for future writes and cluster reassignment; status/retry updates preserve it. Both Schema Registry and Kafka Connect use this behavior, without changing existing Java entities, application SQL, API responses or deployment commands.

The diagram's `host_id/fqdn` label is represented by `host_id`. The following meanings were explicitly approved:

| Field | Meaning/source |
|---|---|
| add_service_id | Existing `kf_cluster_services.id` or `kf_external_services.id`; one mapping per deployed instance |
| cluster_id | Existing assignment's cluster ID, stored as a plain ID |
| kafka_cluster_id | Actual native Kafka ID from saved cluster inventory; V93 snapshot rules apply |
| cluster_origin | INTERNAL / EXTERNAL from saved inventory; NULL when unknown |
| created_time | Actual time the new assignment is mapped; preserved on retry |
| status | Existing assignment's deployment status |
| host_id | Managed host ID or external discovery agent ID |
| broker_ip | Full `config_json.bootstrap_servers` value, including multiple endpoints and any protocol prefix |
| user | OS account from `config_json.service_user` |
| created_by | Authenticated requester |
| updated_by | Requester, or the job's `requested_by` on later status writes |
| secured | NULL; service security semantics not supplied by the current Add Service payload |
| security_protocol | NULL; no explicit service security setting in the current Add Service payload |
| security_mechanism | NULL |
| trust_store_location | NULL |
| user_security | NULL |
| password_security | NULL; credentials are not copied from host or cluster inventory |
| process_role | `schema_registry` or `kafka_connect` |

Broker inventory security fields are not treated as proof of an individual service's security configuration. This change does not add TLS/SASL support to service deployment.

## Write paths

- Managed Add Service and optional services in initial managed deployment: persist existing assignments, flush to obtain their existing IDs, then insert the additional mapping in the same transaction.
- External Add Service: insert a mapping immediately after each existing service assignment is persisted, in the same transaction.
- Managed deployment/retry/rollback: update the matching mapping when the existing assignment is marked DEPLOYING, ONLINE, FAILED or ROLLED_BACK.
- External deployment/retry: update the matching mapping when the assignment is marked DEPLOYING, ONLINE or FAILED. External service cleanup records DELETED on confirmed cleanup.
- Status remains the last recorded deployment/cleanup outcome; it is not a new live health probe. Managed cluster deletion uses its existing cluster/host cleanup history; this phase does not add per-service managed deletion tracking.

Primary-key conflicts are ignored on creation, preserving original creation time and creator. Status writes update existing mappings only. Existing assignments are not backfilled.

The existing `kf_audit_logs` table receives SERVICE_ADDED and SERVICE_STATUS_CHANGED events through `AuditService` after the mapping transaction commits. Audit details contain service ID, host ID, role and status; they contain no credentials or service configuration JSON. Rolled-back writes produce no audit entry. Audit-storage failures after commit are logged without turning a committed deployment into an API failure.

Existing jobs, steps, tasks, service assignments, REST connections, JSON payloads, API responses and UI reads retain their existing storage and behavior. No existing entity/DTO column mapping or existing SQL select list was changed. No migration or data backfill touches the 11 config-change ER tables.

## Verification

Run the complete server suite with Java 21 and Maven: `mvn -B -ntp verify` in `tantor-server`.

`AddServiceMappingDatabaseTest` is explicitly opt-in. It connects only to the named existing local Docker database from its Docker network, inserts fixtures only into `kf_add_service`, and rolls back all fixture writes. It requires `TANTOR_LOCAL_MAPPING_DB_TEST=true` and `TANTOR_LOCAL_DB_PASSWORD` from the ignored local environment file. It cannot be redirected to the server database through a URL variable.

Refresh **Schemas → public → Tables** in pgAdmin on the port 5433 connection. The new table remains empty until a new service is added. For inspection:

```sql
SELECT add_service_id, cluster_id, created_time, status, host_id,
       broker_ip, "user", created_by, updated_by, process_role
FROM public.kf_add_service
ORDER BY created_time DESC;
```

No server deployment or Git push is included.
