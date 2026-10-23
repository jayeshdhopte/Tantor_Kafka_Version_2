# Points 67 and 70

## Rolling restart identity (67)

Internal and external rolling-restart jobs now save `hostname` and `ipAddress`
alongside each step's existing host/target ID, role, Kafka node ID and systemd
service. Labels use the same layout:

```
Restart broker node 1 - node229.translab.io (192.168.3.229)
```

External hostnames come from the linked discovery agent. The broker's recorded
numeric address is preferred to a different management-interface address. If the
broker record contains a hostname, a non-loopback/non-link-local reported agent
address is used. No DNS lookup is required. Missing information is displayed as
`Hostname not reported` / `IP not reported`; IDs are never invented as addresses.

New log messages use the same identity. Job History resolves older steps from
current discovery/internal-host metadata without modifying their database rows.
The UI also uses step snapshots to resolve old UUIDs in log lines. Once discovery
records are gone, genuinely missing legacy metadata stays explicitly unavailable;
new snapshots remain available after nodes are unregistered.

Target IDs, restart ordering, service bindings, retry behavior, health gates and
progress/status indicators are unchanged.

## Artifact staging (70)

Kafka deployment now downloads into a unique private `.tantor-work-*` directory
under the administrator-approved artifact root. The directory contains only
that task's TAR, exporter/JMX staging and an ownership record. It is removed when
the deployment returns, including download/checksum failures. A resumed extraction
re-downloads and verifies its package when its previous staging is no longer
available; it cannot trust a missing archive based only on a completed UI step.

This chooses temporary-transfer cleanup rather than a persistent shared Kafka
TAR cache. Later deployments may download again; they do not accumulate a new
retained TAR for every task. Parcel distribution removes its transferred copy
while retaining the canonical `.downloads` parcel cache that Artifact management
owns. Monitoring and ksqlDB also discard their task-specific transferred TARs.
Schema Registry/Connect already remove their private extraction/download staging.

Kafka deployment state retains the artifact roots used by the cluster. During
deletion, the backend snapshots the cluster/host's actual `INSTALL_KAFKA` task IDs
and original artifact-root settings before metadata can be removed. The agent
then:

- Removes only the exact legacy `kafka_<recorded-task-UUID>.tgz` files.
- Reclaims abandoned private workspaces whose local owner record matches the
  deleted cluster. Linux filesystem locks prevent reclaiming an active transfer;
  a process crash releases the lock automatically.
- Revalidates configured roots, rejects redirected roots and archive symlinks,
  and preserves other clusters, unrelated files, shared parcel caches and the
  artifact-root directory itself.

Previously orphaned files that cannot be attributed to a recorded deployment
are not blindly deleted by wildcard. No live archives have been purged as part
of implementing this change.

## Checks

- Backend identity tests cover internal/external addresses, multihomed agents,
  history snapshots after discovery removal, read-only legacy enrichment and
  explicit unavailable labels.
- Controller tests verify human-readable labels and saved hostname/IP fields
  while external dispatch IDs remain unchanged.
- Artifact tests cover failed HTTP/checksum downloads, safe resume, concurrent
  staging, active-transfer locks, abandoned workspaces, exact legacy ownership,
  retries, traversal and shared-file preservation.
- Both Linux agent suites and amd64/arm64 builds; UI production build and lint;
  full backend suite plus focused final restart/history tests.

## Verification after deployment

1. Open an external cluster's rolling-restart topology and Job History. Each
   broker/controller step should show its actual node ID, hostname and IP, with
   the existing running/success/failure indicators. Existing history is resolved
   when its discovery record is still available.
2. Deploy a disposable internal cluster using an artifact staging directory you
   can inspect. During deployment a private `.tantor-work-*` directory is normal.
   After success or a download/checksum failure, that task's directory should be
   gone. Application data and installed binaries should remain.
3. Delete a disposable test cluster with a known retained legacy Kafka TAR. Its
   recorded TAR and abandoned owned staging should disappear. Unrelated TARs,
   other clusters' workspaces and shared `.downloads` parcel caches must remain.
4. If deletion finds an active transfer for the same cluster, it must fail visibly
   for retry rather than remove that transfer's staging.

Implementation/testing alone does not deploy this change or restart a cluster.
