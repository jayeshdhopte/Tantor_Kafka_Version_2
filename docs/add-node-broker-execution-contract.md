# Broker addition: execution contract and recovery design

Historical Phase 3 planning snapshot. The statuses and “remaining/not implemented” descriptions below describe that earlier increment. They are superseded by [the current deployment and Phase 4 readiness report](add-node-deployment-194.md), which records the deployed broker-only 4.3.0 pilot, actual recovery/resume and remaining production scope.

## Implemented boundary

`GET /api/v1/ui/clusters/{clusterId}/add-node/previews/{previewId}/broker-plan` requires the existing ADD_NODE permission. It re-reads the durable readiness preview, which checks current host, cluster, artifact and policy context. Non-passing evidence returns 409. It returns:

| Field | Meaning |
|---|---|
| previewId, contextHash, expiresAt | The exact reviewed readiness context and its freshness deadline |
| proposedPlan | Typed add-broker/v1 proposal; no arbitrary commands or properties |
| executionSteps | The ordered execution stages that remain to be implemented |
| executionEnabled | Always false in this increment |
| blocker | Explicit explanation that execution cannot be submitted |

The proposed operation UUID is deterministic for this preview and context, for stable review. It is **not** an accepted-operation receipt, an idempotency guarantee or a reservation. The proposed node ID is one greater than the greatest known service/cluster ID. Passing C02 proves the stored identity set matched observed brokers, voters and observers when inspected. The ID still must be reallocated under a transaction at acceptance; a proposed ID cannot authorize execution.

`BrokerAdditionPlan` requires all 32 common/broker checks to be present, unique and passing; an unexpired preview; explicit Kafka ID, node ID, host, version, runtime user, Java, package SHA-256, heap, paths and discovery mode. The current adapter supports inherited PLAINTEXT only. Unsafe endpoints, invalid IPs, path traversal, overlapping storage/runtime paths and a broker ID in static controller voters are rejected. Missing inherited inputs block review rather than choosing guessed values.

The UI requests this plan on Review. A mismatched host/context, expired response, missing backend or malformed contract displays an error. Host or preview changes abort the old request. Deployment remains disabled.

The internal agent understands `VALIDATE_ADD_BROKER_PLAN` with a `broker_plan` JSON string in the existing string parameter envelope. It independently checks its enrolled host identity, IDs, paths, discovery and supported schema; hashes the cached approved archive and compares installed executable payload bytes. Successful evidence contains schemaVersion, operationId, hostId, contextHash, payloadVerified=true and mutationPerformed=false. It writes no configuration, installs no unit, formats no storage and starts no service. The backend does not dispatch this command from the read-only review endpoint.

The agent's `Properties()` builder is an adapter foundation and is not executed. It produces broker-only discovery/identity properties. It is **not yet a complete inherited configuration renderer**: effective broker settings, template compatibility, logging, monitoring artifacts and service environment still need integration before execution is enabled.

## Remaining Phase 3 execution design

Use a dedicated BROKER_ADDITION job handler; do not reuse legacy ADD_HOST publication or generic deployment cleanup. Add an operation record linked to the existing job and task infrastructure, rather than inventing a second task queue.

Acceptance must be one transaction with a fixed lock order (cluster row, candidate host row, operation row). Persist an immutable reviewed plan, requester, request idempotency key/body digest, reserved node ID, reservation state and initial job steps. Database uniqueness must protect an unresolved host reservation and `(cluster,nodeId)` identity reservation across concurrent requests. The cluster guard must include unresolved broker operations even when the generic job summary is FAILED/PARTIAL_SUCCESS. No inventory assignment is made at acceptance.

Repeated same-key/same-body requests return the original operation. Same key with different body returns 409. Existing assignments, tasks in flight or conflicting cluster work reject acceptance. A preview is permission to request final validation, not permission to skip it. Final validation repeats the live identity/topology, package, candidate ownership/storage, resources, reachability and health observations under the operation's reservation; the guard must exclude its own operation but no other writer.

Freeze the effective inherited broker settings before mutation. Explicitly replace process.roles, node.id, listeners/advertised address, data/metadata paths and broker discovery. Preserve supported cluster settings and monitoring requirements. Store the rendered config hash, package hash, service identity, resolved Java/runtime user, paths and Kafka ID in the accepted plan. Unsupported template/security/monitoring combinations must block before any write. Candidate formatting must use the existing cluster ID and, for dynamic quorum, `--no-initial-controllers`; never standalone or initial controller bootstrap.

Proposed mutation commands below are **not yet implemented**:

| Command | Required precondition | Evidence |
|---|---|---|
| PREPARE_ADD_BROKER | Current operation owns reservation; complete final validation; no foreign files/unit | Operation-owned config/unit manifests, hashes and receipt |
| FORMAT_ADD_BROKER | Approved executable bytes; owned config hash; empty persistent storage | Matching cluster.id/node.id in every expected meta.properties plus directory identities |
| START_ADD_BROKER | Same ownership, hashes and matching storage identity | Candidate unit identity, active process/start identity and runtime endpoint |
| INSPECT_ADD_BROKER | Read-only; may run after uncertainty | Actual receipt, config/unit hashes, each storage identity, service/process state and endpoints |

Every mutation task must carry operation ID, action ID, immutable plan hash and expected prior checkpoint. A durable agent receipt must be recorded before the first external side effect. Persist both task creation and step binding in one backend transaction; a restart must not lose the dispatched task reference. Agent replay of the same action inspects the receipt and actual state instead of rerunning a formatter or overwriting unknown files. Conflicting receipt/plan hashes block.

No command may delete storage, change existing brokers/controllers, alter controller voters, or overwrite an unowned unit/config. These restrictions apply to failure handling too. Privileged execution must use the internal agent's configured executor and explicit runtime user. The root and sudo service modes both require validation before release.

## Remaining Phase 4 state and resume design

The operation's durable state is separate from a generic job summary:

| State | Allowed next action | Failure/resume rule |
|---|---|---|
| ACCEPTED | Final validation | Keep host/ID reservation |
| VALIDATING | Record fresh final evidence | No mutation on unknown/failed evidence |
| PREPARING | Owned config/unit preparation | Reconcile a receipt or task whose result is uncertain |
| PREPARED | Format owned empty storage | Verify manifest/config/package again |
| FORMATTING | Await task and inspect storage | Never blindly replay format |
| FORMATTED | Start candidate | Require matching identity in every storage directory |
| STARTING | Await task and inspect service | Do not start a second process while task/lease is live |
| VERIFYING | Observe Kafka and candidate | Broker may already be live; no destructive rollback |
| RECOVERY_REQUIRED | Read-only reconciliation, then explicit resume | Retain reservation and old task IDs |
| SUCCEEDED | View evidence | Publish atomically once, then release reservation |
| CANCELLED_SAFE | View evidence | Only after proven no mutation/no active task/no registration |

Before every action, persist its intent/task reference. After its result, persist the identity-bound evidence and checkpoint. A timeout means uncertainty; it does not prove failure. Backend restart returns to reconciliation from persisted intent, not to a guessed beginning. A pending/running task must be awaited or investigated, never duplicated. Terminal task failure permits a new action only after read-only reconciliation proves a safe prior state. Preserve the failed attempt and evidence.

Partial formatting with mixed missing/matching metadata, unexpected directory IDs, foreign config/unit bytes, ambiguous process ownership or unavailable observation stays RECOVERY_REQUIRED. The first release should require operator resolution for these cases; it must not silently delete, reformat or release the host. Only all expected matching formatted storage can advance to FORMATTED.

After start, require live Kafka cluster identity, the exact reserved broker ID and advertised endpoint, and a supported registration observation proving the broker is unfenced. Socket reachability, a systemd active state or a successful agent task alone is insufficient. Implement and verify the Kafka-version adapter for this observation; do not infer unfenced state from an unverified Admin API default.

Compare controller voter **identities and directory IDs** with the accepted baseline, not only count. Confirm existing broker availability and stable partition/internal-topic/quorum health across the configured observation window. Broker addition does not rebalance partitions automatically. Publication writes the service assignment, host assignment, cluster node IDs and supported discovery/bootstrap metadata in one idempotent transaction after these checks. Publication failure retains the reservation and can retry publication after fresh verification without restarting or reformatting the live broker.

Resume must preserve completed checkpoints and outstanding task IDs. Generic retry currently clears some task references; it must not be connected to BROKER_ADDITION until a specialized recovery path is implemented. Generic rollback/cleanup endpoints also must reject Add Node-owned tasks, so legacy APIs cannot bypass ownership rules. Recovery states should appear explicitly in Jobs with a reason and the next safe action.

## Compatibility and release gates

This increment adds a plan endpoint, DTO/planner, review component and two new internal-agent task cases (readiness already added INSPECT/PROBE; this increment adds VALIDATE_ADD_BROKER_PLAN). It does not change existing task branches, job retry, rolling restart, configuration changes, service deployment, database schema or inventory writes.

Local validation for this increment:

- 86 selected backend tests passed, including 17 Add Node/readiness/plan tests and 69 regression tests covering restart, configuration changes, deployment and Schema/Connect paths.
- TypeScript compilation and changed-file ESLint passed.
- 13 browser fixture scenarios passed, including the matching broker review, mismatched-host rejection, absent planning endpoint, stale/expired evidence and no deployment/service mutation requests. API/permission fixtures do not establish live integration behavior.
- Updated internal agent and readiness tests cross-compiled for Linux amd64. The full readiness package (including plan tests) executed successfully on Windows. Linux runtime integration remains untested.
- Broader native agent tests failed in unchanged `dataservice` and `kafka` packages: missing log-directory fixtures and Windows path/locked-file behavior. Running an isolated HEAD archive reproduced the four Kafka test failures; Windows Application Control blocked the baseline data-service test executable. The Schema/Connect wrapper packages have no tests. These are incomplete regression gates, not a passing live multi-instance deployment certification. Baseline evidence is in the parent workspace's `ui-verification/add-node-agent-baseline-e86f3cd3f8f84ac6bd30a2ba5b9df9d2/results.txt`.

Do not describe Phase 3 or 4 as complete on this evidence. Required work includes the operation/reservation persistence, mutating agent commands, full inherited configuration/monitoring adapter, specialized resume, live unfenced registration adapter, verified atomic publication, failure injection and VM validation. Deploy to 194 only after those gates are satisfied and the compatible backend/UI/internal agent are packaged together.
