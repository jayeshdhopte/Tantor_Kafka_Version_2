# External Kafka Discovery Agent v3.2.1 — Production Hardening Status

## Scope
External discovery/system agent only. Internal managed-cluster deployment is excluded.

## Implemented in this bundle
- Precheck and filesystem discovery share configured scan roots.
- `running-only` is the production default; offline inventory is explicit.
- Disk metrics use the discovered Kafka data/log filesystem.
- `localBrokerCount` is emitted for correct local semantics.
- Installer service name is configurable.
- Runtime user creation is explicit; pre-provisioned identities are validated.
- Exact scan roots, environment, and node identity are mandatory.
- Unknown listener security is reported as `UNKNOWN`.
- Java properties continuations and escaping are supported.
- Precheck wording is specific to external onboarding.
- Generic client documentation replaces lab/APB examples.
- Delivered source binaries are not chmod-modified before install.
- Missing previously-running nodes are reported with `isRunning=false`.
- Static AMD64/ARM64 binaries include version metadata.
- JMX Exporter and Kafka Exporter collection is optional and bounded.
- Exporter health and data availability are distinct from real zero values.
- Remote task polling is disabled by default for the external monitoring role.

## Backend-dependent acceptance items
The following cannot be completed by the agent binary alone and must be validated with the backend/UI:
1. Aggregate unique node IDs to produce cluster-wide broker count. Prefer `localBrokerCount` from this agent.
2. Configure and test heartbeat TTL, offline display, deregistration and decommission behavior.
3. Confirm the backend accepts `isRunning=false` reports and the additive `localBrokerCount` field.
4. Complete full topology acceptance tests for KRaft combined/split/multi-node and ZooKeeper.
5. Accept and persist the additive v3.2.1 metrics payload fields.

## Current transport
HTTP remains intentionally supported for the current phase. Security hardening is deferred by project decision. Do not use Basic/Bearer credentials over HTTP unless explicitly approved.

## Validation performed
- `bash -n install-agent.sh`
- installer `--help` execution
- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- static Linux AMD64 and ARM64 builds
- SHA-256 verification of all packaged files

## Verdict
The code-level P1 items within the external agent and installer have been implemented. Production sign-off still requires backend compatibility and lifecycle acceptance testing in the client-like environment.
