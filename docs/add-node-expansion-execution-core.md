# Controller and separate-process execution core

6 November 2026. **Development in progress; not release acceptance.** This source is not installed on 194, 191 or 229. Node 213 is excluded from all task connections and future rollout.

The current controller/separate-process API and UI remain planning-only. The new code below is deliberately not registered in the native task dispatcher or connected to a submitting API. Agent `.6` binaries and their pinned source manifest continue to represent their earlier committed source, not this execution work.

## Implemented source

`KafkaExpansionWorkflow` compiles an ordered checkpoint sequence for static and dynamic controller addition and two-process addition. A combined broker/controller JVM is stopped and started once. Static expansion stages all original/expanded voter configurations before downtime, stops reviewed existing units, applies identical expanded voters, formats only the candidate storage, verifies controller membership before starting dedicated brokers, and publishes last. Dynamic expansion never stops or rewrites existing units: observer catch-up precedes admission and independent voter verification precedes a separate candidate broker start.

The checkpoint reducer retains uncertain mutation intent and the parent reservation. It rejects out-of-order evidence, foreign plan digests, stale observations, incomplete facts and corrupt persisted ordering. This reducer is **not yet a database ledger**.

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

## Evidence collected locally

- Full backend suite: 695 tests, 596 passed, 99 skipped, zero failures/errors. All 146 report files were regenerated in this run. Skips are not acceptance evidence.
- 34 portable native expansion contract/failure tests passed using Go's WebAssembly runner. This does not execute the Linux adapters.
- The final focused quorum suite passed all 9 tests, including the expected observer-only waiting state before voter admission becomes visible.
- The supplied local Kafka 4.3.0 archive passed the new compressed-checksum and runtime-manifest verifier: 195 runtime files; SHA-256 `057b6f783ab5a745d7918f457d2de57963471b89342395cc6f252cfdbde4f407`.
- Linux amd64 and arm64 agent source builds and amd64 `go vet ./...` passed. The expansion Linux test executable compiled. Linux-only filesystem/process-group tests have not run.

## Required before release

1. Execute Linux adapter/failure tests on an allowed isolated Linux test scope; validate real systemd lifecycle, package-cache paths, Java/classpath proof, ownership/permissions, interruption handling and actual Kafka CLI arguments. Recheck static configuration metadata, including any required SELinux/ACL handling, before allowing replacements on an existing deployment.
2. Complete the backend frozen-plan/evidence collector and durable parent operation ledger. Atomically reserve cluster, all affected hosts and both candidate IDs; arbitrate with existing broker/jobs/tasks; retain action history and reservations through uncertain outcomes. Generic retry/cleanup must not bypass this ledger.
3. Connect the native task dispatcher and backend job handler, including controller-only and separate-process readiness, fresh typed quorum proofs, final health/smoke gates and safe resume. No web-request transaction may wait for an agent.
4. Atomically publish both candidate role assignments and cluster routing only after joint verification. Test interruption before/during publication and preserve controller progress if its sibling broker fails.
5. Wire reviewed submission, static maintenance acknowledgement, progress, reconciliation and resume into the agreed UI. Keep capability/version and release gates closed until the actual installed agents support the contract.
6. Run real broker-v2, static/dynamic controller and separate-process acceptance/recovery, plus rolling restart, configuration change, multi-instance Schema Registry and Connect regression. Local unit results do not certify those live flows.
7. Build matching agent release artifacts from the final committed source, prepare rollback/backup and deployment metadata, then deploy collectively on 194 and update internal agents on 191 and 229. Never connect to or update 213.

SSH remained unreachable on all three allowed VMs during this work. No application deployment, agent replacement or Kafka mutation was performed remotely.
