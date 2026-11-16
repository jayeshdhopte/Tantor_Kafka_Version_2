# External Add Node — Phase 4 verification and recovery

16 November 2026: source implementation and automated validation. **Not deployed and not live-accepted.** A read-only query on 194 found zero non-deleted external clusters. The user will provision Kafka; development does not create a cluster or replace running internal/external agents. Controller and separate-process external additions remain disabled.

This supersedes the Phase 3 startup-only behavior. The historical Phase 3 record remains available in [external-add-node-phase3.md](external-add-node-phase3.md).

Current source has advanced to [Phase 5 external dynamic controllers](external-add-node-phase5.md). The following records Phase 4 broker behavior and validation; its live acceptance remains pending.

## What completes an addition

After the candidate starts, the operation enters `VERIFYING`. Three successful samples, at least five seconds apart, must prove:

- The exact Kafka cluster ID, broker ID, advertised endpoint and rack; the broker must be unfenced.
- The broker set equals the existing brokers plus the candidate. Controller voter IDs/directory IDs remain unchanged.
- All required Kafka health observations pass, including metadata leader/membership, partition/ISR health, broker log directories and controller reachability. The transient health inventory includes the candidate before database publication.
- A fresh native receipt matches the operation, frozen digest, cluster/node identity, standard unit and configuration path. Formatted storage metadata matches the existing Kafka cluster ID and new broker ID.
- Installed `bin`/`libs` bytes match the checksum-approved cached Kafka package, without extra files or symlinks. Configuration, unit, logging and monitoring files match the owned preparation.
- The active unit's MainPID uses the reviewed Java executable, heap, Kafka library root and exact broker configuration. Formatter observation must be complete and show no formatter for this configuration.

A failed sample resets the consecutive sample sequence. Evidence is operation-bound and expires; publication requires a verification witness at most 60 seconds old, a final sample at most 15 seconds before that witness, and a candidate inspection accompanying each sample. Registration/health waiting is bounded; failure retains ownership for reconciliation.

`PUBLISHING` commits the external node, agent/cluster ownership, broker count/status, operation `SUCCEEDED`, publication step and job `SUCCESS` in one transaction. Repeating publication returns the existing result. A callback/transaction failure rolls the entire publication back. Normal discovery is quarantined while the operation owns the candidate, so it cannot publish unverified membership early. Publication does not grant unrelated configuration-write or restart permissions.

## Resume contract

`POST /api/v1/ui/external-clusters/{clusterId}/add-node/broker-operations/{operationId}/resume` requires Add Node permission. It resumes the same parent job and reservations. Generic job retry cannot bypass reconciliation. The Job page's **Retry / Resume** button uses this owned endpoint.

Resume checks the current release gate, compatible candidate agent, frozen participant/candidate identity, connection settings, approved artifacts, inherited configuration and voter baseline. It increments a durable resume generation and enters `RECONCILING`. Generation checks fence old workers, step callbacks and task claims. A compatible agent upgrade or heartbeat refresh alone does not change the stable execution binding; identity, participant, policy or connection drift does.

Each claim supplies an absolute `expiresAt`. The external adapter refuses missing/expired leases and bounds native execution and artifact downloading to the lease and its local timeout. Unclaimed tasks can be retired safely. A claimed task is not retired until its deadline plus 190 seconds; resume refuses to continue while it may still be running. Retired claim tokens cannot deliver results into the new attempt. Native operation locking and fresh formatter/process observations provide additional protection.

Only a fresh owned inspection chooses the next checkpoint:

| Observed native state | Required evidence | Next action |
| --- | --- | --- |
| `ABSENT` | Empty storage, no active broker/formatter; no prior claimed format/start with missing journal | Repeat complete final readiness, then prepare |
| `CLAIMED` | Empty storage, inactive service, existing partial owned files match | Continue idempotent preparation; do not pretend the partially prepared host is a new empty candidate |
| `PREPARED` | Exact runtime/config/unit, empty storage, inactive service | Format once |
| `FORMAT_INTENT` or `FORMATTED` | Matching formatted metadata and inactive service | Continue to start; **do not format again** |
| `START_INTENT` or `STARTED`, inactive | Matching formatted metadata and exact runtime/config/unit | Start the same owned unit |
| `START_INTENT` or `STARTED`, active | Matching storage, installed runtime and exact live process | Repeat stable verification and publication |
| Partial/foreign metadata, unknown formatter, missing durable evidence after claimed formatting, or regressed checkpoint | Insufficient ownership/outcome proof | Keep reservations and hold for investigation; no blind formatting, deletion or rollback |

After a backend restart, queued work uses the same physical reconciliation before continuing interrupted mutations. If an old action's lease is still active, the job stays recoverable and an operator resumes after it settles. Agent restart/lost response never proves that a format failed. Publication interruption rechecks the started candidate and health; it does not repeat preparation or formatting.

Safe cancellation remains limited to operations where no prepare/format/start action was dispatched. This release deliberately provides no destructive "force retry" or automatic storage cleanup.

## Compatibility and rollout boundary

V110 adds recovery states, resume generations, task retirement/claim timestamps and verification/publication fields. Apply through the normal migration process together with matching backend/UI and the compatible external agent; no migration was applied to 194 by this work.

The external source version ends in `-an.3`. Earlier adapters can remain discovery/readiness agents, but cannot claim this verification/recovery release's broker tasks. Old Phase 3 operation payloads without the stable recovery binding are held for explicit migration/review; they are never silently adopted.

These backend gates remain **false by default**:

- `tantor.external-add-node.execution-enabled`
- `tantor.external-add-node.phase2-accepted`

The candidate separately requires Linux root, authenticated task access, and explicit task, Add Node inspection and Add Node execution opt-ins. Existing service-deployment, config-write and service-restart flags remain independent. Standard `broker.service` and the reviewed `/opt`/approved configuration, data/log directories and heap settings are retained. Existing cluster services are not stopped or restarted by broker addition.

## Demonstration and acceptance checklist

`tantor-ui/scripts/test-external-add-node-phase4-browser.cjs` exercises the real React wizard and Job page with clearly labelled fixtures. It demonstrates release blockers, one idempotent submission, read-only lost-response lookup, retained recovery tracking, the owned Resume endpoint and server-confirmed verified publication. It does **not** add a real Kafka broker.

| Acceptance check | Automated evidence / live status |
| --- | --- |
| Exact unfenced registration, endpoint/rack/identity and unchanged voters | Backend tests; **live pending** |
| Full health checks and three separated fresh ownership/health samples | Backend evidence/health tests; **live pending** |
| Atomic publication, rollback on callback failure, idempotent repeat, permissions unchanged | Real PostgreSQL private-schema rollback tests |
| Lost format response with matching metadata skips formatting | Recovery classifier/database tests; **live pending** |
| Partial/foreign/missing storage or active/unknown formatter blocks retry | Backend/native tests; **live pending** |
| Active task lease blocks resume; retired token and stale generation rejected | Database/agent lease tests |
| Started recovery verifies/publishes without prepare/format/start replay | Handler tests; **live pending** |
| Read-only installed-runtime and live-process inspection rejects drift | Linux native tests in isolated `/var/tmp/tantor-external-phase4-validation` on 191 |
| Wizard/job recovery and verified-completion display | Phase 4 fixture browser checks |
| Internal Add Node, rolling restart/config, schema/Connect and multi-instance regressions | Selected backend suites and internal browser regression passed |
| Actual external cluster, broker add, connection loss and backend/agent restart demonstration | **PENDING — no external cluster registered on 194** |

Before accepting Phase 4, provision and connect the designated external test cluster, enroll compatible opted-in agents, accept live readiness, and roll out the matching gated build in the approved test environment. Test a normal addition, a lost result after formatting, agent/backend restart during work, a still-running action, partial/foreign storage rejection and interrupted publication. Confirm exactly one new broker/node row, unchanged voter membership, matching cluster ID/version, standard paths/unit, stable health and no repeated formatter invocation. Record operation/job IDs and evidence. Only then accept the broker-only release and move to Phase 5.

The Downloads demonstration folder is `External_Add_Node_Phase4_Demo`; it contains labelled fixture screenshots and the acceptance report, not a new installable agent release.

## Recorded automated validation

- Selected backend regression suites: 448 tests in the broad run, 406 passed, 42 explicit opt-in integration skips, zero failures/errors. The final focused run adds the started-resume happy-path test: the combined selected reports contain 449 tests, 407 passed, the same 42 skips and zero failures/errors. Both external private-schema PostgreSQL suites were enabled on the isolated loopback test database.
- External Linux Go tests/vet, shared native broker/readiness/layout/facade tests and amd64/arm64 cross-builds passed on 191. Real Kafka mutation tests were not enabled.
- Changed-page ESLint, TypeScript and production UI build passed.
- Browser checks: Phase 4 passed 9 scenarios with exactly one submission, one read-only submission lookup and one owned resume. Phase 1/2/3 fixture regressions and the internal separate-process wizard regression passed.
- Fresh 194 inventory query: zero external clusters. Live acceptance remains pending. Running backend/UI, internal/external agents and Kafka services were not replaced or restarted.
