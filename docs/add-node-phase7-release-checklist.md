# Add Node: Phase 7 pre-deployment checklist

7 November 2026. Phases 1-7 implementation and isolated acceptance completed for the supported first release. Release artifacts must match the pinned committed source. Deployment to 194 and agent replacement on 191/229 are held pending this checklist. Node 213 is excluded.

| Phase | Evidence | Result |
| --- | --- | --- |
| 1-3: flow and contracts | Agreed flow, complete evidence, cluster identity/version, sequential IDs, permissions, stale-preview rejection | Passed automated/backend/browser checks |
| 4: broker-only | Static and dynamic real v2 addition; broker.properties; same ID/version; next broker ID; rack; format replay; config patch/custom restart | Passed |
| 5: controller | Dynamic observer admission and voter proof; static script stop/apply/controller-first restart; recovery | Passed real isolated matrix and recovery tests |
| 6: separate processes | Separate JVMs/units/config/storage; controller verified before broker; atomic publication and rollback | Passed real isolated matrix and PostgreSQL tests |
| 7: acceptance/release | Six real scenarios; cleanup/replay; full native/backend regression; UI build; checksum-bound agent release | Acceptance passed; rollout held |

## Completed acceptance

- [x] Dynamic controller-only addition and independent voter verification.
- [x] Dynamic separate-process addition, broker registration/rack proof and topic-list smoke check.
- [x] Static controller-only expansion using supplied stop/apply/start order.
- [x] Static separate-process expansion with controller verification before broker startup.
- [x] Static and dynamic broker-only v2 addition with format replay.
- [x] Receipt-backed cleanup and completed-cleanup replay in all six cases; interrupted cleanup/resume and foreign-storage rejection in Linux tests.
- [x] Actual failed controller startup resumed with original directory identities.
- [x] Full native Linux regression; ARM64 cross-build and Go vet.
- [x] Backend: 784 tests, 783 passed, one explicit production-endpoint skip, zero failures/errors. PostgreSQL migration/publication/recovery checks passed.
- [x] UI production build and browser fixtures for evidence, downtime acknowledgement, release gate and idempotent retry.
- [x] QA services removed/stopped; no active QA units in final inventory. Current live cluster assignments are on 229; retired 191 services were not restarted.

Detailed scenario identities: `add-node-phase7-acceptance.json`. Release source/build checksums: `../internal-agent/BUILD-ADD-NODE.json` and `../internal-agent/SHA256SUMS`.

## Supported scope and limits

The six cases are real isolated Kafka 4.3.0/native-adapter runs on 191/229 with independent typed backend verification. Database/API/browser checks were separate. This is not a live production UI end-to-end run. Live coverage is managed KRaft PLAINTEXT with the internal root agent; retained 4.1.x coverage is automated. Explicit small QA heap/disk settings do not change production capacity defaults. Candidate hosts must be fresh/unassigned for the selected layout.

Rolling restart, configuration change/recovery, multi-instance Schema/Connect and deployment regressions passed. Added-broker configuration patch and custom-unit restart also passed live. No live Schema/Connect redeployment was performed. No regression was detected in covered paths; an absolute guarantee for untested environments is not claimed.

## Checks immediately before 194 deployment

- [ ] Record reviewed source/artifact commits; verify all release checksums.
- [ ] Back up the application, UI, PostgreSQL database and agent binaries/configuration; verify restore access.
- [ ] Recheck disk headroom on 194: final snapshot had about 1.68 GB available (97% used). Ensure room for release, backup and working files before copying.
- [ ] Confirm current managed inventory and no unresolved old Add Node operation before migration/backfill.
- [ ] Install the matching `.7` internal agents only on 191 and 229, preserving enrollment identity, server URL and local receipts. Do not connect to 213.
- [ ] Deploy backend/UI together; verify health, migrations, agent connectivity and capabilities before enabling expansion execution.
- [ ] Revalidate actual production candidate eligibility, capacity, security mode, exact Kafka artifact and service references. Existing cluster hosts are not automatically eligible candidates.
- [ ] Run a controlled platform smoke check and capture job/evidence; enable the release gate only when these checks pass.

## Rollback and recovery

Before any new expansion job exists, application/UI/agent backups can restore the previous compatible release. Once expansion jobs exist, do not downgrade to a build unable to deserialize QUORUM_EXPANSION jobs or their ledger. Preserve the database, reservations, receipts and formatted storage. Roll forward with a compatible build and reconcile/resume the original operation. Never blindly reformat storage or remove an admitted voter. Static maintenance failures resume from recorded participant state; do not bypass the controller-first verification sequence.

No application deployment or production-agent replacement was performed during this acceptance work.
