# External Add Node — Phase 3 broker execution

16 November 2026: implementation and automated validation; **not deployed, not live-accepted, not a complete broker-only release**. The latest read-only inventory check on 194 still found no connected external cluster. The user will deploy Kafka. Both backend release flags remain false by default; no running agent, Kafka service or application database was replaced/migrated by this development.

## Scope and release boundary

The external wizard supports broker-only preparation/startup for the explicitly supported PLAINTEXT KRaft adapters. Controller and separate-process selections remain disabled. The final external agent source keeps existing feature compatibility prefixes and adds `-an.2`; earlier `-an.1` agents remain inspection-only.

Two independent backend controls default to false:

- `tantor.external-add-node.execution-enabled`
- `tantor.external-add-node.phase2-accepted`

An opted-in candidate also needs Linux root, bearer Add Node administrator task access, `TANTOR_AGENT_ENABLE_TASKS=true`, `TANTOR_AGENT_ENABLE_ADD_NODE_INSPECTION=true` and the **separate** `TANTOR_AGENT_ENABLE_ADD_NODE_EXECUTION=true`. A readiness-only agent cannot enable the Deploy button or claim broker actions. Configuration-write/restart/data-service flags are independent; do not enable unrelated privileges merely to use Add Node.

Phase 3 is a controlled startup checkpoint for a designated lab after live readiness acceptance. Phase 4 still must implement and accept registration/unfenced verification, stable-health sampling, inventory publication and reconciliation/resume. Do not treat a started process or a successful build as a completed addition or production release.

## Reviewed and accepted contract

| Method / endpoint under `/api/v1/ui/external-clusters` | Purpose |
| --- | --- |
| `GET /{clusterId}/add-node/broker-review/{previewId}` | Fresh passing readiness → server-owned broker proposal, digest, expiry, service/path and release/capability blockers |
| `POST /{clusterId}/add-node/broker-operations` | Accept `{previewId, contextHash, planDigest, requestKey}`; never client-supplied IDs, properties or commands |
| `GET /{clusterId}/add-node/broker-operations/request/{requestKey}` | Confirm an uncertain submission outcome without submitting a second operation |
| `GET /{clusterId}/add-node/broker-operations/{operationId}` | Read checkpoint, tracked job, retained ownership and `inventoryPublished` |
| `POST /{clusterId}/add-node/broker-operations/{operationId}/cancel-safe` | Release only before any candidate mutation was dispatched |
| `POST /discovery/add-broker-tasks/{agentId}/claim` | Claim one token-bound, unexpired broker-only action owned by an active job |
| `POST /discovery/add-broker-tasks/{agentId}/{taskId}/result` | Validate exact receipt identity/digest/checkpoint and record an immutable result; identical delivery retries are idempotent |

Every endpoint checks Add Node administrator permission. The mutation queue is separate from readiness and the existing external Kafka/data-service task queues. It does not bypass discovery ownership by temporarily assigning the candidate to the cluster.

The broker ID is **highest existing broker ID + 1**, with cluster-wide controller/observer/reservation collision checks. Existing IDs are not renumbered; a collision blocks review instead of silently skipping IDs. Live readiness C02 must already prove the discovered and live identity sets agree.

The frozen payload includes the existing Kafka cluster ID and exact Kafka version/checksum, candidate profile, broker-only properties, inherited controller discovery, approved monitoring/runtime settings and exact voter identity baseline. The payload digest covers the plan, generated properties and runtime options. Existing effective broker custom settings must have a consistent supported inheritance policy; unsupported secrets/security/listeners block planning. Artifact changes, context changes, incompatible agents or policy drift block dispatch/claim.

Standard deployment conventions are preserved: `process.roles=broker`, `broker.service`, and `<approved configuration directory>/broker.properties`. Installation, existing runtime user/Java, heap, data/metadata and application logs come from the reviewed profile. The native adapter copies the already approved cached distribution to the reviewed installation. It formats only empty candidate storage with the existing Kafka ID. Operation-specific ownership receipts belong under the private agent journal, not an operation-specific Kafka configuration directory.

Monitoring ports reported by existing brokers are included in readiness port checks. Mixed/unresolved monitoring inheritance blocks review. An inherited exporter uses a catalog-pinned ID/checksum through the existing same-origin authenticated artifact downloader. Normal cluster participants are never stopped or restarted by broker addition, and controller voters are never expanded by this adapter.

## Durable execution and reservations

V109 adds an external operation ledger and action queue. Cluster/agent locks and unique constraints retain the candidate, cluster and broker ID. Same-key/same-input retries return the same operation/job; changing reviewed inputs under a used key is rejected. Concurrent candidates on the same cluster, or the same candidate across clusters, cannot become independent accepted operations.

Candidate reservation also blocks conflicting external service/Kafka mutation tasks and public readiness previews. Existing pending/running candidate tasks/probes must settle before acceptance. Own final readiness is allowed under the operation's job identity; another preview cannot bind the candidate's listener while startup is being prepared. Read-only legacy Kafka inspections and completed callbacks remain compatible.

The workflow uses `EXTERNAL_BROKER_ADDITION` and five visible job steps:

1. Repeat complete final readiness under ownership; compare the new plan/digest/voter baseline with the frozen proposal; validate approved executable payload and candidate capabilities.
2. Prepare only the candidate package/configuration/storage/unit.
3. Format only the candidate's empty broker storage with the existing Kafka cluster ID.
4. Start only the candidate broker and validate its exact owned receipt, matching storage and active service.
5. Verify and publish membership — **pending Phase 4**.

Final readiness expiry is checked again before dispatching and claiming the first mutating preparation action. Later actions retain the frozen plan and repeat context/artifact/runtime/voter checks rather than pretending an already prepared host is empty.

The shared native broker runner is reused through `pkg/addnodebroker`; the internal runner's existing entry point is unchanged. Only VALIDATE, PREPARE, FORMAT, START and owned INSPECT are supported. Arbitrary shell/deployment commands and legacy plans without an explicit standard profile are rejected. Native operation journals, storage checks and format-intent checkpoints are retained.

At startup completion, state becomes `STARTED_PENDING_VERIFICATION`. The job remains **PARTIAL_SUCCESS**, with the verification/publication step pending; it is never reported as a completed broker addition. The reservation remains active. Discovery reports from the candidate refresh its heartbeat but cannot automatically publish its membership. Database guards protect candidate ownership, participant identity and conflicting jobs/tasks. Live monitoring may observe the actual new Kafka process; this does not mean verified inventory publication occurred.

## Failure behavior in this phase

A failed or unknown mutating action retains its task, receipts and reservations. Running tasks are not reclaimed or blindly rerun after a lost response. Repeated result delivery can record the same proof, not replace it. Wrong tokens, foreign host/operation IDs, different payload digests, mismatched cluster/node/service/configuration or stale checkpoints are rejected.

Interrupted executions become `RECOVERY_REQUIRED`; replay of formatting/startup is blocked pending the Phase 4 reconciliation adapter. Generic job retry is disabled for this new job type in this release. Safe cancellation is limited to states with no dispatched prepare/format/start action. Existing job types keep their existing retry/rollback behavior.

The wizard displays the server proposal and release/capability blockers. On an uncertain submission response, it performs a read-only lookup using the original request key and links to the existing tracked job. Profile/inventory changes invalidate the review and readiness evidence. Acceptance never grants healthy membership immediately.

## Acceptance checklist

| Scenario | Evidence/status |
| --- | --- |
| Default-off backend gates; separate agent execution opt-in | PASS unit/transport tests |
| Readiness-only agent keeps submission disabled | PASS backend review test |
| Standard broker service/configuration, existing Kafka identity/version, sequential ID proposal | Implemented through shared validated contract; PASS browser/contract fixtures |
| Idempotent submission and conflicting input rejection | PASS actual PostgreSQL private-schema test |
| Concurrent host/cluster ownership and normal task/discovery protection | PASS database constraints/guard tests; live concurrent-request acceptance remains pending |
| Final readiness failure/expiry prevents preparation | PASS handler and database tests |
| Exact claim token/receipt binding; duplicate result delivery | PASS database/contract tests |
| Startup cannot mark the job/operation as verified success | PASS executor/handler tests |
| Lost submission response resolves without another deployment POST | PASS real React fixture demonstration |
| External Linux Go suite/vet and shared broker-runner/readiness suites | PASS on 191 in isolated `/var/tmp/tantor-external-phase3-20261116`; live Kafka test explicitly not enabled |
| Production UI build, TypeScript and changed-page lint | PASS local checks |
| Internal separate-process wizard regression | PASS browser suite |
| Real external-cluster broker start from wizard | **PENDING** — no connected external cluster; matching runtime release not deployed |
| Phase 4 unfenced/stable-health/publication/resume acceptance | **NOT IMPLEMENTED IN THIS PHASE** |

The fixture demonstration is generated by `tantor-ui/scripts/test-external-add-node-phase3-browser.cjs`. It clearly labels test data and does not prove real Kafka admission. Matching backend/UI and external agent rollout, followed by designated live Phase 2/3 tests, is still required before calling this phase accepted. The existing Downloads release kits and running internal/external agents remain unchanged.

## Verification record

- Focused external broker/readiness/controller suite: 29 tests passed, including both enabled PostgreSQL tests; zero failures/errors/skips.
- Broader rolling-restart, config-change, jobs, external Kafka/cluster, data-service, schema/Connect, broker, quorum, Add Node and multi-instance selection: 456 tests, 405 passed, 51 explicit integration skips, zero failures/errors. The two new PostgreSQL suites were then run separately with their loopback opt-in enabled; the broader skip count is retained as recorded.
- Browser: six Phase 3 scenarios passed, including exactly one submission and one read-only lookup after an uncertain response. Phase 1/2 browser regressions and the internal separate-process browser suite passed.
- TypeScript, changed-page ESLint and production UI build passed. Linux amd64/arm64 external-agent builds passed.
- External Go tests/vet and shared native broker/readiness/facade tests passed on 191. They used isolated test directories and fixtures; the live broker adapter test was explicitly not enabled.
- Demonstration artifacts copied to Downloads under `External_Add_Node_Phase3_Demo`. Screenshots are labelled TEST DATA and do not show live Kafka acceptance.
- No application database migration, backend/UI deployment, running-agent replacement, Kafka installation/format/start or inventory publication was performed on the VMs by this phase's development work.
