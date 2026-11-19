# Offline Java upgrade on external Kafka hosts

This release adds `.java-upgrade.1` to the repository discovery agent. It runs
Java upgrade logic directly in Go; Ansible is not installed or invoked. The
supplied `java ansible script.zip` remains the reference for version ordering,
sequential hosts, health checks and recovery. Upload a JDK `.tar.gz`, not the
Ansible ZIP or YAML, to Tantor Artifacts.

## Required deployment

Deploy the matching Tantor backend/UI and this external agent before testing.
No additional database migration is introduced: distribution receipts and
execution results use the existing `kf_external_kafka_tasks` journal. Do not
apply migrations or install this package on a production server as part of a
local build. No server has been upgraded by building this release.

For an enrolled agent, retain its identity, token, scan paths and existing
configuration. Follow your normal reviewed agent binary replacement process;
do not re-enrol an existing node just to replace its binary. The installer is
included for fresh approved lab nodes.

The agent must run as root, with tasks and configuration writes enabled,
bearer authentication with backend ADMIN task access, and service restarts
enabled with the exact discovered Kafka unit in `--allowed-services`.
Existing configuration flags are `--enable-tasks`, `--auth-type bearer`,
`--token-file`, `--enable-config-write`, `--enable-service-restart`,
`--allowed-services` and `--run-user root`. Never share bearer tokens in logs.

## Supported scope

- Linux AMD64 or ARM64, matching JDK architecture; dedicated Kafka VM.
- One discovered KRaft process per agent host, current service/config identity.
- PLAINTEXT local Kafka health probes; SASL/TLS-only probes are rejected.
- Existing `/usr/bin/java` symlink and a verifiable installed Java version.
- Exact dotted target version and approved catalog SHA-256.
- Point upgrades and sequential LTS progression; 17 to 25 directly and
  downgrades are rejected, matching the internal agent policy.
- Explicit per-service JAVA_HOME may override the VM default: verification
  detects that and attempts restoration instead of reporting success.

## UI workflow

1. Artifacts > Upload Parcel Binary > Java JDK. Upload the approved Linux JDK.
2. Expand that Java artifact. Eligible external nodes appear alongside internal
   hosts. Select the intended external hosts and Distribute. A backend-origin
   authenticated download stages the checksum/version/architecture-verified
   archive under `<parcel_dir>/.downloads/java-<artifact-id>.tar.gz`. The agent
   never follows download redirects. Archive download limit is 1 GiB.
3. Wait for DISTRIBUTED. Distribution leaves installed Java unchanged.
4. Open the external cluster > Actions & Restarts > Upgrade Java. Choose the
   distributed JDK and hosts; review the confirmation and start the job.
5. Jobs executes one host at a time. Before mutation the agent rechecks the
   discovered config, cluster ID, unit allowlist and ExecStart binding, rejects
   other Java processes, and verifies partition and quorum health if running.
6. The shared upgrade engine verifies the staged package again, preserves the
   old JDK and Java link, stops a running Kafka service, switches the VM Java
   symlink atomically, starts Kafka and verifies its actual `/proc/<pid>/exe`
   plus health. An already-stopped Kafka service remains stopped.
7. Failure stops the rollout and attempts to restore the old Java/default
   service state. Interrupted tasks are journalled as failed; they are not
   automatically re-executed after agent restart. Review before submitting
   a fresh job; generic Java job retry remains blocked.

Cancel/remove only removes the staged archive, not an installed JDK. Changing
a node's discovered agent, service, config or cluster identity invalidates the
old receipt and requires redistribution.

## Read-only lab verification after a real job

Run on the actual upgraded lab host (replace kafka-broker.service with its
real unit):

```sh
java -version
readlink -f /usr/bin/java
systemctl is-active kafka-broker.service
pid=$(systemctl show kafka-broker.service -p MainPID --value)
readlink -f /proc/$pid/exe
```

The default Java and running Kafka executable must both reference the selected
JDK. Check the job output and cluster health. If Kafka was previously stopped,
verify it remains inactive instead. A green demo result is not real-node proof.

## Build from the bundled source

The source archive contains `tantor-discovery-agent` and `internal-agent/source`
in sibling directories so the existing Go module replacement resolves. From
`source/tantor-discovery-agent`, build with Go 1.24.2 or a compatible toolchain:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o tantor-discovery-agent-linux-amd64 .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o tantor-discovery-agent-linux-arm64 .
```

## HTTPS deployment

Use the approved control-plane HTTPS origin as `--server-url`; the agent fetches
the JDK from that same origin at `/api/v1/artifacts/<artifact-id>/download`.
Supply your organization's PEM CA bundle with `--ca-file` when its issuing CA
is not already trusted. Certificate chains and the server hostname must verify;
do not enable `--insecure-skip-verify` for deployment.

The repository production Nginx configuration terminates TLS and routes artifact
downloads to the artifact service, while discovery/task routes reach the backend.
Keep the artifact service's identity verification URL reachable and trusted.
Internal upstream HTTP behind that proxy does not mean the agent should use an
HTTP public URL. Confirm the deployed proxy routes and certificate on the target
environment before rollout; a local TLS test does not certify that deployment.

Bearer credentials must remain valid for the operation. This agent reads its
token file at startup. Replacing the file alone does not refresh the in-memory
token: renew credentials and restart the agent between operations, never during
an active upgrade. Use the organization's approved credential-renewal procedure.

See `JAVA_UPGRADE_VERIFICATION.md` for the actual local Linux test results and
their limits. Production rollout still requires the target environment's checks.
