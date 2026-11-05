# Add Node Phase 2: readiness preview

Initial milestone: 5 November 2026. Current status: deployed on 194 with updated internal agents on 213, 229 and 191; Linux and authenticated live 4.3.0 broker pilot checks completed. The original validation notes below describe the earlier local milestone. See [the current deployment and readiness verdict](add-node-deployment-194.md) for live verification and scope limits.

## Delivered behavior

The broker-only wizard schedules real observation tasks instead of simulating checks. The backend queries Kafka using describe/list APIs and the approved artifact catalog. The candidate agent inspects its actual machine, runtime, services, storage, mounts, package and network paths. Existing peer agents perform temporary route probes. No installer, formatter, remediation, service start/stop or controller admission action is invoked by the preview.

The server stores its context and backend observations in an `INSPECT_ADD_NODE` task's JSON parameters. The agent returns the versioned structured report through the existing leased task reporting path. Existing claim-token/host authentication remains in force. The task ID is the durable preview ID. No new tables or Flyway migration are required for this phase.

The new preview does not allocate node IDs, assign hosts, create Kafka service membership or authorize deployment. It proposes an unused ID for inspection only. `deploymentEnabled` is always false. The new Deploy button stays disabled. The existing legacy broker deployment endpoint is not replaced by readiness evidence in this phase.

## HTTP contract

Both endpoints require the existing `ADD_NODE` permission.

| Route | Behavior |
|---|---|
| `POST /api/v1/ui/clusters/{clusterId}/add-node/previews` | Body `{ "hostId": "enrolled-host-id" }`; responds 202 with persisted preview and initial checks |
| `GET /api/v1/ui/clusters/{clusterId}/add-node/previews/{previewId}` | Returns current task evidence, input validation, expiry and blockers; rejects a preview belonging to another cluster |

Fields: `previewId`, `clusterId`, `hostId`, `contextHash`, `checkedAt`, `expiresAt`, `status`, `checks`, `brokerPlan`, `deploymentEnabled`. Checks contain code, phase, name, status, expected value, observed value, remedy, source and observation timestamp.

Preview states are RUNNING, PASS, BLOCKED, STALE and EXPIRED. Individual evidence is PASS, FAIL, UNKNOWN or BLOCKED. All C01-C21 and B01-B11 codes must be present and pass before the summary can pass. The UI independently checks required coverage; a summary string alone never enables progression.

The two-minute default lifetime starts before the earliest backend observation. It is not extended when the task finishes. The context hash binds configuration, cluster identity/version, service topology, stored node IDs, host identity/agent version, approved artifact and policy. GET also checks current host/operation availability and revalidates completed previews against the pinned artifact catalog. Changing host or refreshing inventory invalidates the browser's previous evidence and cancels old polling. Failed polling also blocks use of a formerly passed preview.

Live Kafka observations remain snapshots within the evidence window; out-of-band Kafka changes are not continuous monitoring. Phase 3 must run fresh final observations under operation ownership before mutation.

## Measurements and conservative boundaries

- Backend: actual Kafka identity, broker inventory parity, finalized metadata/KRaft feature levels, voter/observer IDs, quorum responsiveness/lag, all topic partitions including internal topics, effective topic min ISR, Kafka log-directory errors and disk headroom. API timeouts, unsupported measurements or permissions failures do not pass.
- Inventory/conflicts: managed KRaft status, shared job conflict guard, known service assignments, outstanding host mutations and duplicate host IP identities. Preview observes conflicts but does not reserve resources across screen review.
- Agent: live host/IP identity, command schema, existing units/processes (including stopped Kafka units), absent/empty intended storage, package archive presence/hash, service-user Java, Linux/systemd/clock/nofile ceiling, available RAM/load, persistent non-root mounts, bytes/inodes and service-user access, basic heap headroom and planned ports.
- Package version inspection compares all installed regular-file payloads against the checksum-approved parcel archive and checks the versioned Kafka core JAR. It rejects extra files, changed bytes and symlinks. It does not execute installed Kafka scripts just to discover version. The implemented parcel layout follows the existing installer: `<parcelDir>/kafka/<version>` and `<parcelDir>/.downloads/parcel_kafka_<version>.tgz`.
- Basic listener/discovery inputs and existing-cluster formatting inputs are observed; preview does not generate/execute a final installation configuration. Full rendered-config validation, format adapter selection and final identity rechecks belong to Phase 3.
- Capacity observations are policy baselines, not throughput benchmarks or a workload simulation. Kafka API reachability is not a claim that every existing network fault metric has been inspected. A sustained post-change health window and unfenced broker verification remain later execution gates.

## Peer and client route probes

New agent commands: `INSPECT_ADD_NODE` on the candidate and `PROBE_ADD_NODE` on configured source hosts. All participating agents need the updated source in the collective release. An old agent's unknown-command response becomes an explicit incompatible/missing-evidence blocker.

The inspector temporarily binds the planned candidate broker port; it does not start Kafka. Source agents connect, verify the `TANTOR-READINESS/1` greeting, then exchange a per-preview nonce. The candidate accepts only a known nonce from the expected source IP. No nonce is sent to an existing non-probe service. The server excludes nonces from the public broker-plan response.

Broker and controller origins are derived from existing cluster service hosts; missing/offline/unsupported agents cannot be counted as successful probes. Representative client origins are explicitly configured by enrolled host ID. If none are configured, B06 is UNKNOWN. NAT/source-address differences conservatively fail the source-IP check and need an explicit future probe policy; do not silently bypass identity comparison.

Probes are bounded (60-second source deadline, 20-second candidate wait, overall inspection timeout) and close temporary listeners. TCP route evidence is not Kafka protocol, registration or fencing evidence. Final execution must account for outstanding preview tasks and rerun port checks before deploying.

## Required release policy and inputs

Defaults below are admission policy, not universal Kafka requirements. Configure and validate them before collective deployment:

```yaml
tantor:
  add-node-readiness:
    ttl-seconds: 120
    timeout-seconds: 30
    min-available-memory-mb: 4096
    min-disk-gb: 20
    min-inodes: 10000
    max-quorum-lag: 10
    max-topics: 1000
    allowed-java-majors: [17, 21]
    supported-kafka-versions: ["4.1.0", "4.1.1", "4.1.2"]
    client-probe-host-ids: [] # Set actual enrolled representative client hosts; empty keeps B06 UNKNOWN.
```

The initial version list is a conservative preview policy, not certification for production deployment. Certify each intended Kafka/Java/OS package during release testing before expanding it. Security mode must be PLAINTEXT for this adapter. Dynamic controller choices remain disabled.

The selected cluster must supply an actual Kafka ID, explicit broker port, runtime user, heap, data/log paths and controller discovery configuration. The existing deployment policy supplies fallback runtime user/Java/limits when configured. Missing inputs remain blockers; the inspector does not invent safe-looking directories or modify users/mounts. Root or temporary fallback filesystems do not pass the persistent-mount check.

An approved available Kafka artifact with SHA-256 must uniquely match the version, or `artifact_id` must pin it in the inherited configuration. Its parcel must be explicitly distributed to the candidate through the existing artifact workflow. Catalog availability, downloaded archive identity, and installed payload identity are separate observations. This phase does not automatically distribute or activate anything.

## Local validation and remaining collective-release gates

- Backend: 11 focused tests passed for persisted observation tasks, no host/cluster assignment writes, complete/unknown evidence, malformed/old agents, context changes, expiry, cluster binding, artifact changes, authorization, read-only Kafka calls and missing measurements.
- Frontend: production build and changed-file lint passed. Ten browser acceptance scenarios passed against isolated API fixtures: asynchronous polling, common/broker gates, unknown/missing evidence, expiry, server invalidation, mismatched host, old backend, host switching and no deployment/remediation requests.
- Agent: the initial four inspector tests passed natively before the final package-integrity/probe refinements. Windows Application Control subsequently blocked execution of rebuilt readiness test executables. Latest Linux agent and readiness test binaries cross-compile successfully, but latest test execution is not claimed. The additional installed-payload tamper/extra-file case is included for Linux execution.
- Broader existing Go test runs also encountered an unchanged data-service cleanup fixture failure and a Windows-only `syscall.Statfs` compilation limitation in the collector dependency. These results are not represented as passing suites.
- No actual VM, candidate host or Kafka cluster was contacted by the browser fixtures/unit tests. No source agent binaries installed on VMs or tracked release binaries were replaced.

Before collective release, run the updated inspector tests on Linux, resolve the broader baseline test issues, and execute read-only preview integration scenarios with the selected package, service user, mounts, client/peer agents and Kafka APIs. Verify listener cleanup, source-IP behavior, old-agent blockers and TTL under realistic latency. Deployment must remain disabled until Phases 3 and 4 implement final ownership, execution, verification and recovery.

Suggested isolated test commands (not deployment commands):

```text
mvn -Dtest=AddNodeReadinessServiceTest,AddNodeKafkaReadinessTest,AddNodeReadinessControllerTest test
go test ./internal/deploy/readiness ./pkg/api
npm run build
```

Browser fixtures and screenshots: parent workspace `ui-verification/add-node-phase2-review.cjs` and `ui-verification/add-node-phase2/`. They stub API/auth role responses in an isolated browser; application authentication settings remain unchanged.

Kafka references used to qualify the policy and join terminology: [Kafka 4.1 compatibility](https://kafka.apache.org/41/getting-started/compatibility/) and [Kafka 4.1 KRaft operations](https://kafka.apache.org/41/operations/kraft/). This phase does not upgrade features or perform membership changes.
