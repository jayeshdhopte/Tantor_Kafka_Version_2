# External Add Node — Phase 2 readiness

Phase 2 implementation/verification record. Phase 3 development is now implemented behind default-off gates; see [Phase 3 contract](external-add-node-phase3.md). Live Phase 2 acceptance is still pending.

Status, 16 November 2026: source implementation and automated checks; **live external-cluster acceptance pending**. The management database has no connected external cluster. The user will deploy Kafka. No cluster has been created for this feature, and no running backend/UI or internal/external agent has been replaced by Phase 2.

The agreed seven phases are in [external-add-node-phases.md](external-add-node-phases.md). Phase 2 does not authorize broker execution. Controller and separate-process choices and Deploy broker remain disabled. A readiness PASS is observation evidence only, never a deployment permission.

## Contract

All endpoints require the existing explicit Add Node administrator permission, including agent endpoints. Readiness uses a separate transport; it never weakens the discovered-node/cluster ownership checks in the existing external managed-task queue.

| Endpoint | Purpose |
| --- | --- |
| `POST /api/v1/ui/external-clusters/discovery/add-node-inspections` | Fresh opt-in host heartbeat/inventory; works before Kafka exists |
| `GET /api/v1/ui/external-clusters/{clusterId}/add-node/candidates` | Host ownership, scope, duplicate identities, local inventory and capability evidence |
| `POST /api/v1/ui/external-clusters/{clusterId}/add-node/readiness` | Create a bounded read-only preview and, when eligible, inspection/probe tasks |
| `GET /api/v1/ui/external-clusters/{clusterId}/add-node/readiness/{previewId}` | Read current evidence; reassess expiry, context, agent freshness and conflicting operations |
| `POST /api/v1/ui/external-clusters/discovery/add-node-tasks/{agentId}/claim` | Claim one pending INSPECT or PROBE with a unique token |
| `POST /api/v1/ui/external-clusters/discovery/add-node-tasks/{agentId}/{taskId}/result` | Complete the bound claim; identical retries are idempotent |

Preview request: `agentId`, numeric registered `hostIp`, approved `parcelDir`, optional pinned `artifactId`, validated `brokerPort`, explicit `brokerRack`, and typed `deploymentSettings`. The settings cover installation/configuration, existing runtime user and Java, data/metadata/log paths and heap. Cluster ID/version and admission thresholds come from server observations/policy.

A preview contains `id`, `clusterId`, `agentId`, opaque `contextHash`, `checkedAt`, `expiresAt`, `status`, structured `checks` and `deploymentEnabled:false`. Statuses are CHECKING, BLOCKED, PASS, STALE and EXPIRED. Every required result must be present and PASS; absent evidence stays UNKNOWN. The default evidence lifetime is 120 seconds.

The context binds the requested profile, cluster identity/version/bootstrap/connection settings, discovered participant IDs/configuration/service references, enrolled agent addresses/versions/ownership, and admission/scope policy. Candidate edits invalidate the UI immediately. Server polling detects changed context and current conflicts/capabilities. Cluster health observations remain bounded by their evidence TTL; Phase 3 must repeat final checks before any mutation.

## What is observed

- Empty-host eligibility: live enrolled identity, exact registered address, explicit opt-in, no discovered Kafka ownership, no duplicate external/internal deployment identity, known Kafka processes/units, complete bounded inventory, approved inspection scope and protected-host exclusions.
- Kafka: live cluster ID, exact broker/controller ID set, finalized metadata/quorum mode, expected brokers, partition/internal-topic health, quorum health, log-directory headroom, rack policy and inherited controller discovery. Static voter configuration is read from effective broker configs and checked against live membership; it is never guessed.
- Package: one approved AVAILABLE catalog artifact matching the cluster version and SHA-256; cached distribution checksum and installed payload identity. Preview does not distribute or extract a package.
- Candidate: existing runtime-user Java, OS/service manager, available ports, empty isolated storage, mount capacity/inodes, host memory/heap, file limits and runtime access.
- Routes: candidate-to-Kafka reachability and nonce/source-IP-bound probes from enrolled broker, controller and explicitly configured client-probe hosts. The temporary listener closes after inspection. Missing client-probe policy cannot pass.
- Existing cluster operations/reservations, including outstanding work retained by previous operations.

The external agent calls the same native readiness implementation through `pkg/addnodereadiness`. That public facade exports only INSPECT and PROBE. It exposes no formatting, installation, configuration-writing, service-start or Kafka membership command. Existing internal execution handlers are unchanged.

## Task failure behavior

V107 stores latest bounded monotonic local observations. V108 stores preview context and read-only tasks separately from execution jobs/reservations. Candidate-row locking prevents two active previews racing to bind the same port. Claims are durable and token-bound. Claiming again does not repeat a RUNNING task. A lost result response retries the identical completion without rerunning the inspection. Another result/token cannot replace completed evidence. An agent/backend interruption that leaves an observation unfinished expires the preview; rerunning creates a fresh context after the old deadline. No storage formatting or membership change exists to recover in Phase 2.

## Live demonstration prerequisites

1. Connect the external Kafka cluster that the user deploys. Initial readiness is limited to explicitly PLAINTEXT, supported Kafka 4.1.0/4.1.1/4.1.2/4.3.0 KRaft clusters; unsupported inheritance is blocked.
2. Coordinate the matching backend/UI (V107/V108) and external agent release. New source has the compact `-an.1` capability suffix (within the deployed 128-character version column).
3. On the candidate and all route-probe participants, opt in with `TANTOR_AGENT_ENABLE_ADD_NODE_INSPECTION=true`, `TANTOR_AGENT_ENABLE_TASKS=true`, bearer authentication with existing administrator task permission, and Linux root. Keep configuration writes, restarts and service deployment disabled. Preserve enrolled identities and protected credential files.
4. Allow the exact external agent IDs/hostnames in `tantor.add-node-inspection.allowed-host-ids`, and set `tantor.add-node-readiness.client-probe-host-ids` explicitly. Exclusions still take precedence. Every route must map to registered participant addresses. Stale/unresolved probes block.
5. Designate a clean, unassigned external candidate. Existing internal deployment identity is an ownership blocker; do not remove internal agents or Kafka automatically to manufacture eligibility.
6. Make the matching approved artifact available and cache it on the candidate. Native cache inspection uses `.downloads/parcel_kafka_<version>.tgz` in the cache ancestry and the verified extraction at `<parcelDir>/kafka/<version>`. Preview never creates those files. Review actual user/Java/ports/rack/paths before running checks.

## Acceptance checklist

| Scenario | Current evidence |
| --- | --- |
| Missing/failed checks block progression | PASS browser fixtures, backend required-code gate |
| Changed profile clears PASS | PASS browser fixtures |
| Clock expiry and server context change clear PASS | PASS browser fixtures; backend tests |
| Old agents, unsupported clusters, unsafe paths, mismatched/duplicate/stale evidence | PASS backend contract tests |
| Admin permission on all preview/agent endpoints | PASS controller tests |
| Durable claims, concurrent-preview blocking, duplicate delivery, wrong token, context invalidation | PASS actual local PostgreSQL transaction/schema test; all test DDL/data rolled back |
| Native code reuse rejects mutation commands | PASS Linux facade tests |
| External Go suite and vet | PASS on 191 in isolated `/var/tmp/tantor-external-phase2-20261116` |
| Real host measurements | PASS diagnostic on 191: 20 results collected; checksum and missing network evidence remained FAIL/UNKNOWN. This diagnostic is not external-cluster admission |
| Real external-cluster wizard run | **PENDING** — no connected external cluster on 194 |
| User demonstration/acceptance | **PENDING** — fixture demonstration is labelled test data |
| Phase 3 broker execution | Implemented behind separate gates; live broker-add acceptance pending |

The browser demonstration script is `tantor-ui/scripts/test-external-add-node-phase2-browser.cjs`; its artifacts are explicitly fixture-only. Live external acceptance must demonstrate both a healthy eligible candidate and real blockers/expiry before advancing to Phase 3.

## Verification record (16 November 2026)

- Focused external readiness/inspection/controller suite: 21 tests passed, including the real PostgreSQL test (enabled against a dedicated loopback test instance). No failures/errors/skips in that focused run.
- Broader AddNode/BrokerAddition/KafkaExpansion/ExternalKafka regression run: 192 tests, 173 passed, 19 explicitly opt-in database tests skipped, zero failures/errors. The new database test was then run separately and passed; it is not represented as passed in the broader skipped-test total.
- Browser: 9 Phase 2 scenarios passed with 5 read-only readiness POSTs and zero other mutation requests. The 11 Phase 1 navigation/access/inventory regression checks and existing internal separate-process browser suite also passed.
- TypeScript, changed-component/page ESLint and production UI build passed. Linux amd64 and arm64 external-agent builds passed.
- External Go suite and vet, and the shared native readiness/facade suites, passed on 191. The optional real-host diagnostic collected all 20 measurements; failures/unknowns were retained. A temporary diagnostic listener closed at the end. No running service/configuration/Kafka storage was changed.
- Fixture demonstration copied to Downloads: `External_Add_Node_Phase2_Demo/Demo.html`, with screenshots and machine-readable acceptance results. It is explicitly labelled TEST DATA.
- Runtime deployment and live external-cluster acceptance remain pending. Existing Downloads release kits and running internal/external agents were not replaced by this work.
