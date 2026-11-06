# Broker corrections and Phase 5 planning rollout

Verified on 6 November 2026, approximately 12:43 IST. Branch: `version2_db_mapping`.

## Delivered

The backend and production UI are deployed at `/opt/Tantor_kafka_VERSION-2_049b9d7` on 192.168.3.194. Internal agents on 213, 229 and 191 run `1.0.0-prod.17-dataservices.4-configchange.10-rollingrestart.1-addnode.5`, verified by binary checksum, active systemd service and fresh authenticated backend heartbeats.

New broker operations use `add-broker/v2`, `process.roles=broker` and `broker.properties`. Allocation follows the supplied script: highest existing **broker** ID plus one, with collision checks across every known cluster node. Brokers 1 and 2 plus controller 101 propose 3; collision or overflow blocks. Existing IDs are not renumbered. Frozen v1 operations keep `server.properties` and their original receipt/digest for resume. Existing cluster ID and exact Kafka version inheritance remain mandatory; the admitted versions include 4.3.0.

Phase 5 now has a permission-protected, read-only controller plan API and UI. The live authenticated test against the team's current managed cluster detected **STATIC**, returned all eight maintenance phases and kept `executionEnabled=false`. Dynamic observer/admission planning is separately implemented and covered by fixtures. Unknown quorum mode selects neither execution path. Inventory service/configuration references are explicitly unverified until runtime inspection is implemented.

Controller execution, static maintenance, dynamic admission and their durable recovery handlers are **not implemented** by this increment. No controller was added, stopped, formatted or promoted. See [the execution contract](add-node-phase5-controller-contract.md) for the next milestone.

## Verification and limits

- Selected backend regression suite: 294 tests, 258 passed, zero failures/errors, 36 opt-in database tests skipped. Coverage includes broker addition, controller planning, rolling restart, configuration change/recovery, multi-instance behavior, Schema Registry/Connect and masking. This patch adds no migration.
- Production TypeScript/Vite build and changed-file ESLint passed.
- Browser fixtures passed for v1 compatibility, v2 broker submission/idempotency/resume, and six controller-planning scenarios. Fixtures did not execute a real Kafka mutation.
- Isolated Linux broker mutation/recovery and readiness tests passed directly on 194, including v2 `broker.properties`, exact systemd configuration path, format checkpoint and lost-result resume without another format. Both Linux agent architectures were built; amd64 is the deployed architecture.
- Public HTTPS UI index and its new main asset were byte-verified against installed files. Authenticated controller review succeeded; unauthenticated controller review returned 401.
- Before rollout: zero active platform jobs, agent tasks or unresolved broker operations. Both current cluster lifecycle statuses remained `SUCCESS` across the application replacement.
- Agent config checksums and identities were preserved. The four running Kafka/data-service Java processes on 213 and 191 retained their baseline PIDs and start times; none was running on 229. Backend `.env`, UI runtime configuration and both Add Node systemd drop-ins were preserved.

These checks found no regression in the covered existing features. They do not replace disruptive end-to-end acceptance of every legacy operation. No new live broker addition was performed for this patch; the earlier broker-102 pilot predates the sequential-ID/filename correction and is historical evidence only. A fresh-host v2 broker acceptance run remains necessary before claiming that exact updated flow has passed live end to end.

## Artifacts and recovery

| Item | Value |
|---|---|
| Backend SHA-256 | `a08c4f74b05de64b6c0aadccff0de5eb50488dc3b57f4fef515cf74239f80159` |
| UI main asset | `/assets/index-DUAVdQh9.js` |
| Agent amd64 SHA-256 | `3ad4e8064cfb4cbec322bf6419fec4d6a09b0fee0bf7f6443a40e1912b9dc490` |
| Agent arm64 SHA-256 | `ed2d4cdda99ffd86885a1b9d18548c2eb1c043f72f71e8b0a9fefcfbaaea60fa` |
| Application/database backup | `/opt/tantor-backups/add-node-20261106T071222Z` |
| Evidence/staging directory | `/var/tmp/tantor-add-node-phase5-20261106` |

The evidence directory contains isolated Linux test logs, deployment log/manifest, authenticated live controller review and post-verification JSON. Each agent retained its old executable beside the installed binary with a timestamped `before-addnode` suffix. The enrollment kit in the active release was also updated and its previous contents backed up.

An application rollback must preserve current cluster membership and the additive V105 operation history. Do not restore a pre-addition database, rename live configuration files or delete/reformat Kafka storage as a software rollback.
