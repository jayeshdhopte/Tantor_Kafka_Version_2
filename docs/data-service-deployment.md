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
service. Kafka Connect connector JARs must be staged in the selected plugin
directory before its precheck runs.

Run `build.ps1` with Go 1.22+ and Java 21 available to build the backends and
both Linux agents. The release manifest packages the built agent binaries and
the external installer.
