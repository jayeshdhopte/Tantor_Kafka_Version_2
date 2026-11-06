# Controller and separate-process execution core

6 November 2026. **Development in progress; not release acceptance.** This source is not installed on 194, 191 or 229. Node 213 is excluded from all task connections and future rollout.

Controller and separate-process execution are now wired in development source, behind `tantor.add-node-expansion.execution-enabled=false`. No collective release has been deployed. Agent `.6` binaries and their pinned source manifest still represent their earlier committed source; the new runtime-inspection contract requires a matching `.7` or later build.

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

`KafkaExpansionPlanCollector` requires fresh C01–C21 checks, the complete participant evidence set, a typed live voter baseline and approved candidate profiles. Separate-process addition also requires broker checks. Static maintenance acknowledgement is part of the frozen context. `KafkaExpansionPreviewService` queues read-only candidate validation before an execution request can reserve anything.

The submitting API accepts a server-owned preview ID/context and an idempotency key. A lost submission response can recover the previously accepted operation even after its preview expires. `KafkaExpansionJobHandler` dispatches ordered actions through short ledger transactions, drains earlier tasks, inspects uncertain effects before replay, obtains independent voter/health evidence and publishes the candidate roles, host ownership and cluster routes atomically. A crash after publication repairs the job without repeating native actions.

The four-step UI now places controller and separate-process observations, final layout validation and submission in the final review. The job page resumes the original expansion and displays retained reservations. Safe cancellation is offered only before mutation; the server also checks drained tasks and all mutation history. A cancelled operation cannot be reopened through generic job retry.

Compatibility updates honor recorded custom systemd units in Rolling Restart and Config Change. The native config reader/patcher/backup path accepts the exact added-node configuration only with a matching root-owned completed addition receipt, host/node/role identity and owned unit. Existing installation path rules are unchanged.

## Evidence collected on 6 November

- Full backend suite: **739 tests, 641 passed, 98 skipped, zero failures/errors**. The four subsequently added preview service tests also passed. Skips remain outside acceptance evidence.
- The expansion/controller/ledger/database focused run passed 56 tests; the handler subsequently passed all seven cases, including static/dynamic controller-only and separate-process ordering.
- Real temporary loopback PostgreSQL tests passed ownership migration/backfill, cross-family conflicts, retained recovery reservations, callback guards and cancellation rules.
- The complete agent suite passed natively on 191 from checksummed source archive `fcf3abd05a1dea7a0a0c1264a6277d4d3a9565f40e453a2c53be7fc9ba499513`. This includes existing agent package regressions and Linux filesystem/process-group tests. A later isolated CLI probe is tracked separately.
- Read-only verification of the running 4.3.0 controller passed: full approved runtime payload, 107 actual classpath libraries and four canonical file bindings.
- The production typed controller Admin adapter successfully queried the existing 4.3.0 controller: leader 101, voter 101, observer broker 1, metadata feature level 30. This observation alone does not certify expansion.
- The real Kafka 4.3.0 storage CLI accepted the adapter formatter arguments for STATIC/broker, STATIC/controller, DYNAMIC/broker and DYNAMIC/controller. Eight new directory identities were verified in fresh test-only temporary storage, with no service started. Test executable SHA-256: `dd44b775a807680ac4281e173f030d9e058f4e53dd853b1b24ce13df89411fd0`.
- UI production builds passed. Browser fixtures passed complete-evidence gating, no automatic participant commands, static acknowledgement, release gating, stable submission keys after a lost response and navigation to the accepted job.

## Still required before collective deployment

1. Finish targeted collector/native evidence/publication failure coverage and run isolated real static/dynamic controller and separate-process add/recovery acceptance. Ordered mocked handler tests are not a real quorum mutation.
2. Verify added-node cluster deletion uses exact receipt-backed custom units and storage. A historical deleted-cluster broker had remained enabled; its ownership and zero live assignment references were verified before stopping/disabling it. The current broker recovered and its controller stayed running. No files were deleted.
3. Exercise live rolling restart/configuration change and existing multi-instance service regression in a controlled scope. Agent and backend regression suites pass, but this does not guarantee every existing live flow.
4. Build a matching new agent release from committed source, prepare rollback/backup and deployment metadata, then deploy collectively on 194 and update internal agents on 191 and 229. Never connect to or update 213. Keep expansion execution held until acceptance passes.

SSH connectivity has been restored. No application deployment or internal-agent replacement occurred during this development checkpoint.
