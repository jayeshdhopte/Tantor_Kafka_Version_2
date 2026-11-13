# Add Node: standard services and editable deployment profile

New readiness requests through the HTTP API use an explicit deployment profile (server defaults when omitted). New broker plans use `add-broker/v3`; controller and separate-process plans use `add-quorum-expansion/v2`. Existing frozen v1/v2 broker and v1 expansion operations keep their original filenames, paths and service identities on resume. Existing installations are not renamed or migrated.

| Item | New addition |
| --- | --- |
| Broker service | `broker.service` |
| Controller service | `controller.service` |
| Default active installation | Existing deployment installation base + `/kafka` (an existing `/kafka` suffix is retained) |
| Default configuration directory | `<installation>/config` |
| Broker configuration | `<configuration directory>/broker.properties` |
| Controller configuration | `<configuration directory>/controller.properties` |
| Ownership and recovery state | Existing agent-private operation receipts; never used as the runtime configuration directory |

The wizard's first step offers installation directory, configuration directory, JAVA_HOME, existing runtime account, broker/controller data directories, separate metadata and application log directories, and maximum heap for each process. An empty broker metadata path uses the first data directory. Multiple data paths use commas. Controller settings are used for controller and separate-process additions. The final review shows the selected paths and heaps. Kafka version, cluster ID, sequential node IDs, listener ports, JMX artifact/ports and minimum resource policy remain server-controlled.

The profile defaults are read from `GET /api/v1/ui/clusters/{id}/add-node/previews/deployment-settings`. Submit the edited object as `deploymentSettings` with the existing `hostId` and `brokerRack` in the readiness POST. Expansion validation obtains that same stored profile from the common readiness task; it does not accept an independently editable native execution plan. Broker final validation repeats readiness with the proposed profile instead of reverting to server defaults. Every profile change alters the preview context and requires fresh checks.

Writable paths must be canonical children of `/opt`, `/var`, `/data`, `/srv` or `/app`. JAVA_HOME additionally supports a direct JDK directory below `/usr/lib/jvm`. Both backend and agent reject traversal, shell characters, whitespace, overlapping broker/controller storage, storage/config/log/runtime overlaps and configuration in executable directories. A config subdirectory of the selected installation is permitted. Candidate configuration and installation may not overlap the approved parcel cache. Native adapters reject symlink ancestors and existing foreign files/units. Resource, package checksum, cluster identity, rack and quorum checks remain required.

The agent copies only the checksum-verified Kafka `bin` and `libs` trees into the selected installation, leaving storage/configuration separate. It accepts identical files on retry, rejects extra or different runtime files, and publishes each copied file atomically. Controller startup and CLI execution reverify copied runtime bytes. Kafka properties, units and receipts retain their existing ownership checks. Config change and deletion resolve standard-name additions through matching root-owned receipts, unit headers, exact configuration paths, role/node/cluster identities and digests. Inventory records retain an Add Node operation marker so missing ownership cannot silently fall back to ordinary cleanup. Shared Kafka binaries, receipts and retained auxiliary config/backup artifacts are preserved during receipt-backed deletion.

Internal agent **addnode.9 or newer** is required on the candidate. Controller and separate-process expansion also require it on every existing participant because they consume the new frozen expansion contract. Deploy backend/UI and compatible agents together after draining active work. This change does not enable execution flags, change host allow/exclusion scope, stop Kafka, resize storage or migrate old operations. The lab's running agents remain .8 until a coordinated rollout.

Validation: focused backend AddNode/BrokerAddition/KafkaExpansion suite, full Linux native suite and Go vet on 191 in an isolated `/var/tmp/tantor-layout9-final-tests-20261113` directory, TypeScript, changed-component ESLint, production UI build and the real wizard browser fixture. Tests cover standard-layout broker recovery after a lost format callback, standard/legacy config ownership and cleanup, frozen profile drift, legacy wire omission, resource/release gates and profile propagation. Live Kafka addition/maintenance with the new layout has not been run; existing cluster services were not changed by validation.
