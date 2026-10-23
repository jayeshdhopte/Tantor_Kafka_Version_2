# Bugs 58, 59 and 60

## 58: Host availability

Host availability uses the internal host agent's heartbeat. An offline internal
agent makes an enrolled host unavailable even when its persisted status is
OCCUPIED or an external cluster uses that node. External cluster association is
retained as metadata. Removed hosts stay removed; pending onboarding stays pending.
The external agent's connectivity cannot overwrite the internal agent's status.

## 59: External service directories

External overviews now include per-instance Schema Registry and Kafka Connect
deployment paths. The existing Info/directory panels consume the same overview
API, and the overview also displays installation/configuration/data/log paths for
external Kafka nodes when available. No agent update is needed: the missing link
was that overview path mapping only read internal assignments.

Service directory rows come from stored external deployment configuration after
successful installation. ONLINE legacy services are included; other states must
have a successful INSTALL task. This preserves paths when installation succeeds
but the subsequent connection SAVE fails. Pending/failed installations without a
successful INSTALL and deleted services are excluded. Paths remain visible when
the discovery agent goes offline, labelled **Saved deployment paths**, with agent
connectivity. These are recorded installation paths, not a new live filesystem
discovery feature. Manual filesystem changes outside Tantor are not rediscovered
by this change. Missing values remain Not reported.

Per-instance IDs are used as UI keys, so multiple services on one node no longer
overwrite/reuse each other's directory cards. Custom paths are preserved. The
external overview fallback includes service paths even when live Kafka metadata
cannot be retrieved.

## 60: Multiple instances and default connections

External Add Service no longer requests that every new instance become default.
The first active connection becomes default; additional instances retain the
current default. Explicit user-requested default switches still work.

Connection saves/deletes obtain a PostgreSQL transaction-scoped advisory lock for
their cluster/service scope. This also serializes concurrent first inserts where
there is no existing connection row to lock. Clearing the previous default is
flushed before setting/inserting its replacement, avoiding Hibernate's insert-before-
update ordering conflict. Default deletion flushes deactivation before electing
a replacement. The existing unique partial index remains in place.

Retry reuses the named connection and completed external deployment steps. This
change does not automatically resume any live failed job or reinstall services.

## Testing after deployment

1. On an idle test host, keep the external agent running and stop the internal
   `tantor-agent.service`. After the internal heartbeat timeout (normally 90s),
   refresh Hosts: Agent must be Offline and Availability must be Unavailable.
   Cluster membership must remain associated. Restore the internal agent if that
   host is intended to use it.
2. Open an external cluster's Schema Registry and Kafka Connect Info/directory
   panels. Verify custom install/config/plugin/working/log paths for each installed
   instance. Compare them with the submitted deployment settings. Check the
   Service installation directories section in Cluster Overview as well.
3. For multiple instances on one node, verify separate directory cards. A pending
   uninstalled service must not appear as installed. An installed service whose
   later SAVE failed must still show its recorded paths and failed deployment state.
4. Add a second Schema Registry/Connect instance with distinct directories and a
   free REST port. Verify SAVE succeeds, both connections exist, and the previous
   default remains selected. Select the new instance explicitly to validate it.
5. Explicitly switch the default connection, then refresh: exactly one active
   default should remain. For an existing failed SAVE job, Retry should preserve
   completed installation steps; no automatic retry is performed by this fix.

PostgreSQL regression tests use disposable `tantor_default_test_*` schemas and
exercise the real Hibernate repository and partial unique index, including
concurrent first saves, explicit switches, retry and default deletion.
