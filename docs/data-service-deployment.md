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
