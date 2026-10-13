# Schema Registry and Kafka Connect deployment

## Managed clusters

In the cluster deployment form, select a Kafka node and its broker/controller
role. Schema Registry and Kafka Connect can also be selected on the node. Each
service has its own artifact, REST port, heap, and directories. The UI runs the
host precheck before the deployment job is created. The job verifies or creates
the required compacted internal topic(s), installs the service, verifies its
REST API, and saves its connection to the cluster.

For a cluster that is already deployed, use **Clusters → ⋮ → Add services to
this cluster**. Select either service or both, configure a host for each, and
start one job. Existing brokers and controllers are not redeployed.

### Multiple managed service instances

Managed cluster deployment supports Schema Registry and Kafka Connect on
multiple selected Kafka hosts, with one instance of each service per host.
Select the service role on every target host in the deployment form. When adding
services to an existing cluster, select multiple host checkboxes in the Add
services dialog; its settings apply to all selected hosts. Hosts that already
have that role cannot receive another instance. External discovery-agent
deployment continues to use its existing single-host workflow.

Schema Registry instances share the Kafka bootstrap servers, `_schemas` topic,
Schema Registry group ID, version and compatibility level. Connect workers share
their worker group ID and config, offset and status topics, while each worker
advertises its own host and REST port. Existing instances' internal topic names
and replication factor are retained when scaling out; adding a worker does not
recreate topics or silently change replication. Workers must have the same
runtime version and matching connector plugins installed on every host.

Each host has separate persisted deployment steps, status and errors. The job
checks and installs each instance and verifies its REST API. A failed step stops
later steps; successful peers retain their own status. Resume the existing job
to retry a failed deployment. New service connections have host-specific names;
adding an instance preserves the current default connection. The application
does not provision a load balancer or automatically fail over that default URL.
For client endpoint availability, configure a load balancer separately. Kafka
topic replication still depends on the number and availability of Kafka brokers.

The managed APIs accept plural addon fields in addition to legacy single-instance
fields. For new deployment, place them under `addons` in
`POST /api/v1/ui/clusters/deploy`. For scale-out, send them directly to
`POST /api/v1/ui/clusters/{id}/services`:

```json
{
  "schema_registries": [
    {"host_id": "host-a", "artifact_id": "schema-artifact", "version": "8.1.1", "port": 8081},
    {"host_id": "host-b", "artifact_id": "schema-artifact", "version": "8.1.1", "port": 8081}
  ],
  "kafka_connect_workers": [
    {"host_id": "host-a", "artifact_id": "unified-artifact", "version": "4.3.0", "group_id": "tantor-connect", "port": 8083},
    {"host_id": "host-b", "artifact_id": "unified-artifact", "version": "4.3.0", "group_id": "tantor-connect", "port": 8083}
  ]
}
```

Supply the selected artifact SHA-256 through the same artifact metadata/checksum
mechanism as single-instance deployment. The host agents already implement the
required per-host tasks. Shared unified Connect requires agent `dataservices.3`
or newer; shared unified Schema Registry requires `dataservices.4` or newer.

The managed agent source is `internal-agent/source`. Build its Linux amd64
binary from that module, then install the new binary on every target host. An
older installed binary cannot execute the new precheck commands.

## Kafka Connect with the existing unified installation

For managed clusters, leave Connect's directory fields blank to reuse the
selected host's Kafka installation (normally `/opt/kafka`, including its
versioned symlink target). The UI selects the available unified artifact that
matches the cluster's Kafka version. No second Connect distribution is downloaded
or extracted in this mode. This requires internal agent `dataservices.3` or newer.

Defaults:
- Launcher: `<Kafka installation>/bin/connect-distributed.sh`
- Properties: `<Kafka installation>/config/connect-distributed.properties`
- Plugins: `<Kafka installation>/connect-plugins`
- Logs: `/var/log/tantor/kafka-connect`
- Working directory: `/var/lib/tantor/kafka-connect`

Prechecks verify the existing launcher, runtime libraries, requested version and
accessible plugin directory. An explicit install directory outside Kafka retains
the separate-installation behavior; explicit config/plugin/log/work paths are
validated overrides. In shared mode a config override must contain an existing
regular `connect-distributed.properties` file. Kafka storage and runtime trees remain protected.

Installation backs up the existing Connect properties and atomically updates
managed properties, preserving unrelated settings, comments and connector JARs.
It creates one `kafka-connect.service`, refuses to start alongside another
Connect worker, and restores the prior properties if installation or REST
verification fails. A managed installation retry can change only REST ports.
Cleanup removes the managed Connect unit but preserves all shared installation
files and directories. Existing standalone Connect deployments need an explicit
migration; changing directories in a retry will not silently move a running worker.

## Schema Registry with the existing unified installation

For managed clusters, leave Schema Registry's install/config directory fields
blank to reuse the host's unified Kafka installation. The UI selects the unified
artifact matching the cluster's Kafka version. With Kafka 4.3.0 installed at
`/opt/kafka`, the existing layout is:

- Launcher: `/opt/kafka/bin/schema-registry-start`
- Libraries: `/opt/kafka/share/java/schema-registry`
- Properties: `/opt/kafka/etc/schema-registry/schema-registry.properties`
- Dedicated logging configuration: `/opt/kafka/etc/schema-registry/tantor-schema-registry-log4j2.xml`
- Logs: `/var/log/tantor/schema-registry`
- Working directory: `/var/lib/tantor/schema-registry`

This requires agent `dataservices.4` or newer on every selected host. The backend
rejects shared deployment on older agents. No second distribution is downloaded
or extracted, and the agent does not recursively change Kafka ownership. Prechecks
verify existing launchers, server libraries and resolved path safety. Shared
configuration must use the existing `etc/schema-registry` directory; log/work
overrides must remain outside the shared Kafka installation and protected storage.

Before updating managed properties, the agent backs up the existing properties
beside the file and preserves unrelated properties. Writes are atomic. Failed
installation or REST verification restores the original properties and removes
the new Schema Registry unit without stopping Kafka or Connect. Successful
installation prunes these configuration backups older than seven days. Cleanup
removes only the managed service unit and preserves shared files and directories.

Explicit standalone installation paths remain supported. Existing standalone
Schema Registry services are not migrated automatically: a retry requesting a
different runtime path fails rather than silently moving a running service.
External discovery deployments retain their standalone defaults.

## External clusters

Connect or discover the external cluster first. Then use **Clusters → ⋮ → Add
services to this cluster**. The dialog lists discovery agents that are online
and advertise service-deployment capability. The resulting job runs host
precheck, topic validation or creation, installation, and REST verification in
order. The external cluster's Kafka configuration is not changed.

Build `tantor-discovery-agent` for the target Linux architecture and install
it with `tantor-discovery-agent/install-agent.sh`. Service deployment requires
`--enable-tasks --enable-service-deployment --run-user root`; the installer
checks these settings. The agent journals task results locally so interrupted
installs are not replayed silently.
The configured HTTP control-plane origin must also serve
`/api/v1/artifacts/{id}/download`; the production proxy and management server
provide that route for the agent.

The discovery service-task polling and completion endpoints currently rely on
the agent ID and the existing discovery-agent network boundary. Restrict access
to these endpoints to trusted agent hosts before enabling root-level service
deployment in production. HTTP transport also needs a trusted private network
or a TLS-terminating proxy.

## Prechecks and current scope

The supplied plaintext scripts are preserved verbatim in `deploy/prechecks`.
Both agents execute the shared, embedded implementation in
`internal-agent/source/internal/deploy/dataservice/precheck.sh`. It takes
validated task values as arguments, avoids shared `/tmp` files, and checks
open-file limits, Java 17, NTP, DNS and Kafka reachability, REST port
availability, Connect plugin JARs and disk capacity, and existing topic
information. The agent also validates artifact checksum and service paths
against Kafka and other service directories. A failing precheck stops the job.

Service deployment currently supports Kafka PLAINTEXT listeners. Select an
available Schema Registry or Kafka Connect artifact with SHA-256 metadata. A
unified Kafka artifact may also be selected if it contains the required
service. In shared-installation mode the existing plugin directory must be accessible.
Install the connector JARs needed by your connectors there; deployment preserves them.

Run `build.ps1` with Go 1.22+ and Java 21 available to build the backends and
both Linux agents. The release manifest packages the built agent binaries and
the external installer.

## Kafka configuration checkpoints and retention

Internal agent `configchange.4` is required for managed Kafka config changes.
Before each static file change, the agent creates a checkpoint under
`<config directory>/.tantor-backups/<properties filename>/`. Dynamic broker
changes also checkpoint the local properties file and save a sibling
`.dynamic.json` containing the live property value/source and requested value.
The backend waits for a successful agent backup before sending the Kafka Admin
API change. A failed backup stops the operation, and backup paths appear in the
job logs. A dynamic change does not rewrite the static properties file.

Checkpoints have unique UTC creation timestamps. The agent removes managed
checkpoints older than seven days at startup and hourly while it runs, even
when no further configuration changes occur. It keeps a private durable index
under `/var/lib/tantor-agent/config-backup-index`, recording resolved versioned
paths so a later Kafka symlink upgrade does not orphan retention entries.
The original config file's mtime does not affect retention. Cleanup skips
symlinks and unrelated filenames and does not delete active properties files.
Backups remain restricted to root. Old agents cannot perform changes under
this policy; unmanaged/external nodes without a managed backup agent are
rejected rather than changed without a checkpoint.
