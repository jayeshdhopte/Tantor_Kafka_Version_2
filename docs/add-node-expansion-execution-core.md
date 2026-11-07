# Controller and separate-process execution core

7 November 2026. Implementation and isolated acceptance are complete for the supported first-release scope. Deployment to 194 and replacement of agents on 191/229 remain held. Node 213 is excluded. The matching release agent is `.7`; existing installed agents are unchanged. Expansion execution defaults to `tantor.add-node-expansion.execution-enabled=false` until rollout prerequisites are satisfied.

## Implemented source

`KafkaExpansionWorkflow` compiles an ordered checkpoint sequence for static and dynamic controller addition and two-process addition. A combined broker/controller JVM is stopped and started once. Static expansion stages all original/expanded voter configurations before downtime, stops reviewed existing units, applies identical expanded voters, formats only the candidate storage, verifies controller membership before starting dedicated brokers, and publishes last. Dynamic expansion never stops or rewrites existing units: observer catch-up precedes admission and independent voter verification precedes a separate candidate broker start.

The checkpoint reducer retains uncertain mutation intent and the parent reservation. It rejects out-of-order evidence, foreign plan digests, stale observations, incomplete facts and corrupt persisted ordering. `KafkaExpansionLedger` now persists this reducer in a V106 parent operation/action ledger. Shared database reservations arbitrate with broker-only operations, jobs, host assignments and native tasks; uncertain outcomes retain all affected hosts and both identities.

`KafkaExpansionQuorum` uses typed Kafka Admin responses through controller bootstrap endpoints. It verifies Kafka cluster identity, finalized features, existing voter directory identities, expected membership, metadata high watermark, bounded lag, candidate directory/endpoint and observation freshness. This permits static maintenance checks independently of stopped brokers. Dynamic membership requires the real API directory identity and endpoint. An API zero directory identity in a static protocol needs independent native storage and consistent voter-configuration proof; it is never presented as an observed nonzero API directory ID. Literal endpoint exclusions are checked before any Admin client is created.

The native `internal/deploy/expansion` package provides:

- A frozen `add-quorum-expansion/v1` contract binding operation, host, cluster ID, exact Kafka version, catalog checksum, both candidate identities, existing units/configuration hashes, process identities, storage directory IDs, voter baseline and explicit resource policy.
- Broker ID allocation matching maximum broker ID plus one; controller ID matching maximum known ID with a floor of 100 plus one. Collisions block the plan; existing identities are never renumbered.
- Candidate ownership receipts and failure recovery for preparation, formatting, start and dynamic admission. An uncertain format is inspected rather than repeated. Directory IDs are pinned after formatting. A separate broker needs fresh parent-bound controller membership proof and matching native controller storage/process identity.
- Existing-participant receipts for staging, stop, voter application and restart. Configuration replacement changes only the reviewed voter property, preserves unrelated lines, and rejects ambiguous Java Properties syntax. Existing participant storage has no formatter or cleanup action.
- Linux filesystem adapters with bounded regular-file reads, symlink rejection, inode checks, exclusive immutable backups, fsynced receipts/replacements and preservation of configuration ownership/mode. Actual service state and running JVM identity are rechecked around observations.
- Checksummed archive manifests and actual running classpath/startup-script verification. A Kafka version in a directory name does not establish artifact identity. Foreign libraries, class-loading overrides and unapproved Java agents are rejected.
- Combined JVM heap plus OS reserve accounting, allowed Java majors, available ports, fresh-process checks and per-filesystem capacity accounting. Separate processes cannot count the same free disk bytes or inodes twice.
- Bounded Kafka CLI execution in a separate process group. Timeout cancellation kills that group so a formatter/admission subprocess cannot keep running after the task ends.

The broker-only `add-broker/v1` and `v2` payloads, digests, receipts and mutation commands remain separate from this package. Shared Add Node readiness/controller planning now enforce configured host/route/probe exclusions before Kafka observation or task creation.

## Backend and user flow

`KafkaExpansionRuntimeInspection` accepts existing service IDs and resolves the approved artifact and recorded service paths. The native inspector proves actual classpath/startup files, approved Java agents and canonical configuration/unit/metadata bindings. Legitimate installation aliases are pinned and rechecked. Effective inherited settings stay server-side.

`KafkaExpansionPlanCollector` requires fresh C01-C21 checks, the complete participant evidence set, a typed live voter baseline and approved candidate profiles. Separate-process addition also requires broker checks. Static maintenance acknowledgement is part of the frozen context. `KafkaExpansionPreviewService` queues read-only candidate validation before an execution request can reserve anything.

The submitting API accepts a server-owned preview ID/context and an idempotency key. A lost submission response can recover the previously accepted operation even after its preview expires. `KafkaExpansionJobHandler` dispatches ordered actions through short ledger transactions, drains earlier tasks, inspects uncertain effects before replay, obtains independent voter/health evidence and publishes the candidate roles, host ownership and cluster routes atomically. A crash after publication repairs the job without repeating native actions.

The four-step UI now places controller and separate-process observations, final layout validation and submission in the final review. The job page resumes the original expansion and displays retained reservations. Safe cancellation is offered only before mutation; the server also checks drained tasks and all mutation history. A cancelled operation cannot be reopened through generic job retry.

Compatibility updates honor recorded custom systemd units in Rolling Restart and Config Change. The native config reader/patcher/backup path accepts the exact added-node configuration only with a matching root-owned completed addition receipt, host/node/role identity and owned unit. Existing installation path rules are unchanged.

## Final acceptance evidence

- Full backend regression: **784 tests, 783 passed, one skipped, zero failures/errors**. The skipped test requires an explicitly configured production controller endpoint; it is not counted as passing. PostgreSQL opt-in tests, full migration through V106, atomic publication rollback/success, reservation arbitration and legacy connection-default tests passed.
- Full native Go suite passed on Linux 191 from source archive SHA-256 `c2a5435da81017dec3ef0e84eaf8e7cc74a3dafaad01760491e564e59f839856`. The release source differs only in the agent version label. Linux ARM64 cross-build and Go vet passed.
- All six isolated **real Kafka 4.3.0** cases passed: static/dynamic broker-only, controller-only and separate-process additions. Evidence includes original cluster identity/version, role-specific configuration, sequential broker ID, voter identities, storage-format replay, broker registration/rack where applicable, topic-list smoke checks, and receipt-backed cleanup plus replay. See `add-node-phase7-acceptance.json`.
- Native failure/recovery tests cover interrupted mutation and cleanup; an actual controller startup failure resumed without changing formatted directory identities. A controller proof serialization issue and systemd startup/cache races found during acceptance were fixed.
- A broker added through the real v2 adapter accepted a native configuration patch and verified custom-unit restart. Existing rolling restart, configuration recovery, multi-instance Schema/Connect and deployment paths passed automated regressions. No live Schema/Connect reinstall was performed.
- UI production build and browser fixture checks passed evidence gating, static acknowledgement, release gating, submission retry idempotency and navigation. Final edits clarify preview requirements without changing the contract.

## Scope and rollout

Live tests use isolated QA identities, units, ports and storage on 191/229, with explicit small test heap/disk settings. Production capacity defaults are unchanged. The live matrix certifies the tested Kafka 4.3.0 managed KRaft PLAINTEXT/internal-root-agent scope. Existing 4.1.x automated coverage is retained; no live 4.1.x matrix is claimed. Controller and separate-process candidates require fresh unassigned hosts. A second process on an occupied host is outside this flow.

This evidence combines real native execution and independent typed backend verification with separate database/API/browser tests. It is not a production UI end-to-end deployment. No regression was detected in the covered existing features; this does not guarantee every live environment.

See `add-node-phase7-release-checklist.md` for rollout checks and rollback boundaries. Keep database ledgers, native receipts and formatted identities during recovery. Never blindly undo an admitted controller or downgrade to a build that cannot deserialize newly created expansion jobs.
