# Points 63 and 65 — cluster cleanup and Connect plugins

## Cluster deletion (63)

Internal deletion still uses the saved per-host Kafka deployment state and
Schema Registry/Connect assignments. It now also stops/disables and removes each
application's log-retention service/timer and their systemd override directories.
This happens before deleting logs/archives. Kafka exporter units and the recorded
binary, application-log, storage and data directories remain part of cleanup.

The application service's override directory is removed too. Unit files are kept
until directory/override cleanup succeeds, so a failed deletion can be retried
with ownership evidence intact. Successful Kafka cleanup leaves a small receipt
in the shared agent's state directory; redelivery returns success without touching
paths again. A later new deployment resets that receipt.

New isolated Schema Registry/Connect installations write an ownership marker
before copying runtime files. This allows cleanup after installation failed
before creating a systemd unit. The marker must exactly match the saved service
identity and directories. An old partial installation with remaining files but
neither a matching unit nor marker fails visibly for inspection; it is not
reported as successfully deleted.

For a service sharing Kafka's runtime, cleanup preserves Kafka's installation,
configuration and plugin trees and removes the service unit, application retention
units, and the service's separate log/working directories.

External deletion now queues a `CLUSTER_DELETION` job for persisted
`kf_external_services` deployments. It cleans each owned instance through the
discovery agent's constrained `CLEAN` command, then unregisters the external
cluster. Discovery-only paths and manually added REST connections do not prove
deployment ownership and never become cleanup targets. Existing Kafka, its data
and topics, shared agents and their own retention timers remain intact.

Registration and discovery-agent bindings remain until cleanup completes. An
offline agent, incompatible agent, unsafe path, failed stop or timeout causes a
failed/partially successful job. Retry preserves successful steps and does not
duplicate a still-running cleanup task. Force-delete uses the same cleanup gate
for external deployments instead of forgetting unfinished work. Completion and
failure use the existing cluster-deletion audit history and original requester.

Host prerequisites (Java, system-wide limits/sysctl/THP/time synchronization) and
shared Artifact parcels/caches are host resources, not per-cluster installations;
cluster deletion does not remove or undo them.

External agents must report the `cleanup.1` release before cleanup tasks can be
dispatched. Both agent releases include the shared implementation.

## Connect plugin parity (65)

Fresh internal and external Connect installations use the same installer. It
places the distribution's bundled `connect-file-*.jar` on the configured worker
plugin path in a dedicated `tantor-filestream` directory. The runtime's `libs`
copy is preferred over a different vendor bundle in `share`; nothing is downloaded
from the Internet. Existing custom plugins are preserved. An identical installed
FileStream copy is reused; conflicting versions require an explicit upgrade.

Deployment success additionally requires the real `/connector-plugins` API to
report both `FileStreamSourceConnector` and `FileStreamSinkConnector`. Connector
counts are not hardcoded; installations with additional plugins can show more
than five entries. Existing port-only retries preserve their installed runtime;
this change does not silently restart or upgrade an already deployed worker.

On node .229 the original worker reported three MirrorMaker plugins although
`libs/connect-file-4.3.0.jar` contained both FileStream classes. An isolated CLI
scan of the staged JAR reported both classes as loadable, manifest-compatible
version 4.3.0. Its running worker was not changed during investigation.

## Validation

- Both Linux Go suites; Linux amd64/arm64 builds for both agents.
- Disposable real-systemd cleanup test through the public external-agent bridge:
  application, child process, active prune process, timer, units, overrides and
  directories removed; unrelated file retained; repeat cleanup succeeds.
- Ownership/path mismatch, foreign units, missing half of a timer pair, stop
  failures, Kafka-role cleanup receipts, failed partial installs and HTTP plugin
  discovery tests.
- Full backend suite and focused final deletion/capability/controller tests:
  cleanup-before-unregister, timeout retention, original-actor auditing, retry,
  reassigned-agent/deployment protection and old-agent rejection.
- UI production build and lint of the changed cluster page.

The systemd test is opt-in, creates only uniquely named disposable units and
cleans its fixtures:

```sh
cd internal-agent/source
TANTOR_TEST_SYSTEMD_CLEANUP=1 go test -v -count=1 -timeout 90s \
  -run TestSystemdCleanupStopsProcessesAndRemovesOwnedResources ./pkg/dataservices
```

## UI verification after deploying the server, UI and agents

1. Deploy a disposable internal test cluster with Schema Registry and Connect.
   Record its actual units/paths, including application retention timers. Delete
   it and open the returned Jobs entry. Confirm all owned resources are removed
   and the host agent remains online.
2. Add disposable Schema Registry/Connect instances to an external cluster through
   Tantor. Delete the external registration. The job must remove those instances
   first. Compare Kafka service PIDs and data before/after: they must remain.
   Shared discovery agents and their retention timers must remain running.
3. With a test agent offline, request deletion. Confirm registration remains and
   the job reports a failure. Reconnect it and retry the same job; completed
   instance cleanup must not be repeated.
4. Deploy fresh Connect instances through each agent type using the same parcel.
   Open Kafka Connect → Plugins. Confirm the three bundled MirrorMaker classes
   plus FileStream Source and Sink; additional custom plugins remain visible.
   The underlying `/connector-plugins` response must contain the same classes.

No existing cluster was deleted or redeployed for these checks. The isolated
build/test work does not deploy the changed application to node .194.
