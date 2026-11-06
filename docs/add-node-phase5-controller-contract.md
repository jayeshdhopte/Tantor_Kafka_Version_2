# Phase 5: controller addition following the supplied scripts

## Delivery status

First implementation milestone: authenticated, read-only controller planning API and UI. Static maintenance and dynamic observer/admission are separate workflows selected from live finalized Kafka features. Controller execution is deliberately unavailable in this increment. It does not schedule agent tasks, reserve IDs, stop/restart services, format storage, change voters, migrate features or publish membership.

This is progress into Phase 5, not completion of controller execution. The next required milestone is participant/candidate inspection followed by durable execution checkpoints and failure/resume tests. A static maintenance run also needs a fresh eligible controller host and review of its concrete downtime plan.

## Source of truth

References are the user's `Downloads/add_host` scripts, copied from `/root/add_host` on 194:

- `add_host.yml`: explicit static and dynamic branches.
- `tasks/common_preview.yml`, `tasks/role_preview.yml`: identity discovery, quorum mode, eligibility and role checks.
- `tasks/static_quorum_expand.yml`: the static phases 0–7 maintenance procedure.
- `tasks/quorum_expand.yml`, `tasks/storage_format.yml`, `tasks/service_deploy.yml`, `tasks/verify_controller.yml`, `tasks/post_health_gate.yml`: dynamic observer preparation, catch-up, admission and verification.
- `docs/dynamic_quorum_migration_runbook.md`: a separate migration procedure. Controller addition must not automatically convert a static cluster to dynamic quorum.

The backend/agent implementation must preserve these workflows. Service names and paths are resolved from real runtime evidence, because the script's literal generic names/path searches do not cover every Tantor instance. In particular, an existing Phase 4 broker may use `tantor-kafka-broker-102.service` and its versioned operation-owned configuration path. Omitting it from static maintenance would not implement the supplied workflow correctly.

## Planning API implemented

`GET /api/v1/ui/clusters/{clusterId}/add-node/controller-plan?hostId={candidate}`

ADD_NODE permission is mandatory. The response uses `add-controller-review/v1` and includes:

- Existing Kafka cluster ID, exact Kafka version, live quorum mode and approved matching artifact SHA-256.
- Proposed controller ID using the script's controller rule: maximum known node ID, with a minimum baseline of 100, plus one. It is only a proposal, not a reservation.
- All registered broker/controller participants. Recorded `service_name` and `config_path` values are shown as inventory references with `RUNTIME_INSPECTION_REQUIRED`, not as verified process/file evidence.
- A context hash binding cluster configuration, membership references, live IDs, candidate identity/version, artifact and policy; observation and expiry timestamps.
- Static or dynamic script steps, impact and blockers. An unknown mode selects neither workflow.
- `status=PLANNING_ONLY` and `executionEnabled=false`, regardless of the broker execution release switch.

The Choose Role screen exposes this planning preview and blocks advancement into broker submission while Controller is selected. Switching back to Broker preserves its existing review/execution flow. Phase 6 now adds a separate two-process planning preview; its execution remains disabled and cannot bypass the unfinished Phase 5 executor. The controller review also returns observed IDs and controller endpoints so the two-process plan can bind their identity and invalidate its context when endpoints change.

## Static execution contract to implement next

| Script phase | Required checkpoint and evidence |
|---|---|
| 0 Prepare | Discover exact participant unit/PID/configuration/metadata paths; verify candidate, approved package, matching Kafka version/cluster ID; take verified backups and stage expanded voters without changing active files |
| 1 Pre-checks | Healthy leader and quorum, zero offline/under-replicated partitions, fresh agents, sufficient storage, candidate ports and complete staged files; abort before downtime if any gate fails |
| 2 Stop | Accept reviewed maintenance/downtime acknowledgement, reserve all affected hosts/processes and cluster; stop the reviewed broker/controller units and prove they stopped |
| 3 Reconfigure | Promote only staged, operation-owned configurations and verify the identical expanded voters string in every affected broker/controller configuration |
| 4 Format | Format only the candidate's proven-empty controller storage with the existing cluster ID; persist format intent before invocation; never format existing metadata |
| 5 Controllers | Start existing controllers and candidate; prove leader, intended voter identities and bounded replication lag; reconcile/restore according to the source failure branch and verified checkpoint |
| 6 Brokers | Start every broker stopped by this operation, serially; verify registration and URP=0 |
| 7 Post-checks | Verify expected broker/controller membership, partition health and smoke test; atomically publish controller membership and close maintenance only after success |

The source declares brokers-before-controllers stop order but currently issues a combined broker/controller stop command inside a per-host loop. The final executor review must make its exact unit order explicit. This is a planned maintenance stop/reconfigure/start procedure, not reuse of the existing availability-preserving rolling-restart operation. Raw broad `systemctl` guesses, `|| true` results or hard-coded path searches are not proof that the intended processes stopped or the correct files were updated.

## Dynamic execution contract to implement next

1. Prove finalized dynamic quorum; static clusters cannot enter this branch.
2. Preserve the current Kafka ID/version and reserve a unique candidate controller ID, host and cluster operation.
3. Prepare candidate `controller.properties` using bootstrap servers and no static voters. Existing controller/broker configuration remains untouched.
4. Format only empty candidate storage using the live cluster ID and `--no-initial-controllers`.
5. Start only the candidate controller as observer.
6. Verify its actual directory identity and metadata catch-up before `add-controller` admission.
7. Re-read actual voter membership, quorum health and candidate identity before publication. Existing services are not restarted for normal dynamic admission.

## Backend/agent boundary and recovery requirements

Planned typed commands must separate read-only inspection, staging, stopping, promoting staged files, formatting, starting, observer inspection, voter admission and membership reconciliation. No command may invoke the source playbook or arbitrary shell content supplied by a request. Commands bind host ID, operation ID, frozen plan digest, exact service/configuration identity, intended voters and artifact checksum.

The existing job/task system supplies task leases, authenticated results and visible progress. Controller execution needs its own durable operation/checkpoint ledger and reservations covering **all static participants**, not only the candidate. It must participate in existing configuration/restart/deletion guards and isolate callbacks from the legacy whole-cluster deployment updater.

Required durable states include `ACCEPTED`, `VALIDATING`, `STAGING`, `STAGED`, `MAINTENANCE_STOPPING`, `STOPPED`, `CONFIGURING`, `CONFIGURED`, `FORMAT_INTENT`, `FORMATTED`, `CONTROLLERS_STARTING`, `QUORUM_VERIFIED`, `BROKERS_STARTING`, `OBSERVER_CATCHING_UP`, `ADMISSION_INTENT`, `VOTER_VERIFIED`, `VERIFYING`, `RECOVERY_REQUIRED`, `SUCCEEDED` and pre-mutation `CANCELLED_SAFE`. Each branch only traverses its applicable states.

- Before mutation: cancellation releases reservations without service changes.
- After any stop/configuration mutation: retain reservations, original process states and verified backup references. Generic retry/rollback/cleanup cannot bypass the controller recovery handler.
- After a formatter timeout/lost callback: inspect every storage identity. Never replay formatting blindly or delete partial metadata.
- After uncertain `add-controller`: inspect actual controller ID, directory ID, endpoints and voter set before any retry. Do not blindly promote twice or remove/stop a possible admitted voter.
- Static recovery must reconcile active files, actual running quorum and the completed script phase before choosing the applicable restore/start path. An unverified backup or a mixed/unknown quorum state is not permission for an automatic metadata restore.
- Do not publish membership or declare maintenance complete until fresh independent verification passes.

## Acceptance still required

Controller candidate/common checks; exact runtime participant inspection; durable multi-host reservation guards; static and dynamic native executors; maintenance acknowledgement; stage-specific rollback/resume; isolated Linux failure tests; separate real static-maintenance and dynamic-admission pilots; and regression validation for existing broker, rolling-restart, configuration, Schema Registry and Connect flows. None is represented as completed by the planning preview.

## Concurrent Phase 4 corrections

New broker plans use `add-broker/v2` and `broker.properties`. Frozen `add-broker/v1` plans retain `server.properties` for safe resume; existing running brokers are not renamed or reformatted. Both backend publication and the native systemd/formatter select the filename from the frozen plan version.

Broker allocation is `max(existing broker IDs) + 1`, independently of the controller maximum, with cluster-wide collision protection. Brokers 1 and 2 plus controller 101 propose broker 3. A collision blocks instead of renumbering existing processes or silently skipping IDs. A pre-existing pilot broker 102 remains 102; subsequent allocation follows the script from the actual broker maximum.

The existing cluster ID and exact existing Kafka version remain mandatory and are unchanged by these corrections.
