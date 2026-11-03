# Add Service database mapping

Schema Registry and Kafka Connect mapping on `version2_db_mapping`.

## Storage

V91 originally added the diagram's 16 fields without foreign keys. Identifiers use UUID, created_time uses TIMESTAMPTZ, secured uses BOOLEAN and other original fields use TEXT. Only add_service_id is NOT NULL.

V97 adds the validated database constraint fk_add_service_cluster:

    FOREIGN KEY (cluster_id) REFERENCES public.kf_clusters(id)
    ON UPDATE NO ACTION ON DELETE RESTRICT

This is the physical relationship shown as cluster_id (FK) in the diagram; the
actual parent table is kf_clusters. NULL remains permitted for an unassociated
record. Unknown non-NULL cluster IDs are rejected on INSERT and UPDATE.
Migration validation fails on orphan data rather than deleting, rewriting or
fabricating parent records.

Both internal clusters and external inventory mirrors use this parent table and
the same Tantor UUID. The shared purge path clears live host, node, connection and
service assignments. If service history references the cluster, its parent row is
retained with status DELETED, hidden by the existing active-inventory filters.
Unreferenced parents can still be physically purged. There is no cascading
deletion or nulling of service history.

The native Kafka ID unique index applies to active parents. A deleted historical
parent keeps its original native ID, and re-registering that physical cluster
under a new Tantor UUID remains possible without allowing duplicate active IDs.

V93 adds `kafka_cluster_id` and V94 adds nullable TEXT `cluster_origin`, bringing the current total to 18 fields. `cluster_origin` is `EXTERNAL` when the UUID belongs to external-cluster inventory; otherwise the saved internal inventory `origin_type` supplies `INTERNAL` or `EXTERNAL`. Unclassifiable/missing inventory stays NULL. External inventory takes precedence over its internal mirror.

V94 fills the new origin field for existing rows whose inventory proves their classification. No prior field or audit row is changed. An insert/update trigger also resolves it for future writes and cluster reassignment; status/retry updates preserve it. Both Schema Registry and Kafka Connect use this behavior, without changing existing Java entities, application SQL, API responses or deployment commands.

The diagram's `host_id/fqdn` label is represented by `host_id`. The following meanings were explicitly approved:

| Field | Meaning/source |
|---|---|
| add_service_id | Existing `kf_cluster_services.id` or `kf_external_services.id`; one mapping per deployed instance |
| cluster_id | FK to kf_clusters.id; includes the canonical mirror for external clusters |
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

Status updates use a null-safe conditional UPDATE, so repeated callbacks reporting
the same status neither change the mapping's actor nor emit another audit event.
The database evaluates this condition under the row lock, including concurrent
callbacks. Real retries that transition back into an earlier status still create
new events. Previously recorded audit history is retained unchanged.

Existing jobs, steps, tasks, service assignments, REST connections, JSON payloads, API responses and UI reads retain their existing storage and behavior. No existing entity/DTO column mapping or existing SQL select list was changed. No migration or data backfill touches the 11 config-change ER tables.

## Verification

Run the complete server suite with Java 21 and Maven: `mvn -B -ntp verify` in `tantor-server`.

Database tests are explicitly opt-in. Existing local test flags retain their local
defaults. TANTOR_STATUS_AUDIT_TEST_URL, USER and PASSWORD can redirect the service
mapping tests only to a deliberately named *_check_* database. Foreign-key and
status-transition tests require this isolated override. Coverage includes real
internal/external parents, orphan rejection, NULL, purge/history preservation,
external re-registration and active-ID uniqueness. Fixtures never target the live
management database.

Refresh **Schemas → public → Tables** in pgAdmin on the appropriate PostgreSQL connection. For inspection:

```sql
SELECT add_service_id, cluster_id, created_time, status, host_id,
       broker_ip, "user", created_by, updated_by, process_role
FROM public.kf_add_service
ORDER BY created_time DESC;
 
SELECT conname, convalidated, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'public.kf_add_service'::regclass AND contype = 'f';
```

The V97 migration changes constraints and indexes only; existing mapping and audit
rows are not backfilled or rewritten.
