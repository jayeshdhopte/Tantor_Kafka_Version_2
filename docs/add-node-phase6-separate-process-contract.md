# Phase 6: one host, two separate Kafka processes

## Current delivery boundary

Phase 6 planning is implemented in the backend and Add Node role screen. It does not execute an addition. Phase 5 controller execution/recovery is still unfinished; selecting this layout cannot bypass that dependency or submit a broker-only job.

Local validation on 6 November 2026: production UI build and changed-file lint passed; 306 selected backend tests completed with 270 passed, zero failures/errors and 36 opt-in database skips. Eleven separate-process browser fixture scenarios plus five existing broker-v2 and six controller-planning scenarios passed. Desktop/mobile layouts were inspected; the process table scrolls locally on narrow screens without page overflow. The fixtures do not execute Kafka commands.

Deployment of this planning increment is pending: SSH connectivity to 194, 213, 229 and 191 timed out during rollout preflight. No Phase 6 artifacts have been uploaded or services restarted. Existing internal agents remain at the previously deployed `.5` release; this increment adds no agent command or executable.

One **fresh unassigned host** receives a controller-only JVM and a broker-only JVM. This flow does not add a second process to an occupied host and does not create a combined `process.roles=broker,controller` JVM. The same Kafka cluster ID and exact existing Kafka version apply to both processes.

## Source alignment

The team's HTML prototype explicitly offers two processes on one host, controller-first verification and joint publication. It supersedes the Draw.io sheet's single-new-service cardinality for this layout. Existing `kf_cluster_services` and `kf_nodes` already represent per-role service rows on one host; do not merge the two identities into one row or change the existing broker-only ledger to hold two IDs.

The supplied `Downloads/add_host` scripts already implement their `broker,controller` intent using separate configurations and services:

- `tasks/common_preview.yml`: controller maximum known ID (with floor 100) plus one; broker maximum broker ID plus one.
- `tasks/storage_format.yml`: `controller.properties` and `broker.properties`, different storage trees, the same live cluster ID; `--no-initial-controllers` for a dynamic observer only.
- `tasks/service_deploy.yml`: separate controller and broker units, controller started first.
- `tasks/verify_controller.yml`: observer catch-up, voter admission and membership verification.
- `tasks/static_quorum_expand.yml`: combined-role maintenance branch, formatting both candidate trees in Phase 4, controllers/quorum in Phase 5, brokers in Phase 6.

Two source/design differences are explicit:

1. The prototype's consecutive example IDs are illustrative. Real proposals use the scripts' role-specific allocation rules and the team's sequential broker requirement. Brokers 1 and 2 plus controller 101 propose broker 3 and controller 102. If both proposals collide with each other or an existing identity, planning records a failure; it never silently skips/renumbers an ID.
2. The dynamic source task bundle starts both services before its later controller-verification include. The agreed prototype requires verified controller admission **before broker start**. The proposed executor must split those existing tasks at this dependency gate. Both empty storage trees can be formatted first, as in the scripts, while the broker stays stopped until controller verification. This is documented orchestration work still required before execution, not an already implemented agent behavior.

The HTML initial scope disables controller-containing layouts on static quorums. At the user's later instruction to follow the supplied static scripts, this increment exposes the static combined-role **maintenance plan for review only**. It does not enable a static deployment or automatically migrate a static quorum to dynamic. Unknown quorum mode selects no workflow.

## Implemented review API

`GET /api/v1/ui/clusters/{clusterId}/add-node/separate-plan?hostId={host}&brokerRack={actual-rack}`

The ADD_NODE permission is required before any Kafka observation. `add-separate-review/v1` returns:

- Existing Kafka ID/version, matching approved artifact/checksum, live quorum mode, observation/expiry times and a context hash.
- Exactly two ordered process proposals: controller and broker. Each has a distinct required role/config filename/unit, a proposed ID, an inherited listener port when unambiguous, and required relative directory separation.
- Controller `process.roles=controller` with `controller.properties`; broker `process.roles=broker` with `broker.properties`. The broker is never included in controller voters.
- S01 identity checks and S02 listener-layout checks. PASS here describes a backend proposal, not free-port or storage proof.
- S03 storage/ownership, S04 combined JVM/exporter resources and S05 rack/dependency observations remain UNKNOWN until two-process agent readiness is implemented. A passing broker readiness result cannot satisfy them.
- Static maintenance or dynamic observer-first steps, recovery requirements and all inherited controller blockers.
- `status=PLANNING_ONLY`, `executionEnabled=false`. Broker execution flags do not change this value.

The context binds the controller observation (including observed IDs and endpoints), both process proposals, cluster configuration and requested broker rack. Changing the host, rack or role resets the UI review; refreshing aborts prior requests and expiry is visible. Mismatched, malformed or executable responses are rejected. Directory entries are relative **requirements**, not inspected/approved absolute paths. No task, job, storage file, reservation, membership or migration is written by GET.

## Backend/agent execution contract still to implement

A separate parent operation must own one host, both IDs and the cluster operation. It must reserve **both IDs and the host in one transaction** and retain them through partial failure. Static mode additionally reserves every affected existing participant. Reusing two independent jobs would allow partial publication and conflicting recovery; the existing broker-only operation and its historical receipts stay unchanged.

Freeze a versioned parent plan containing operation ID/digest, host identity, exact matching artifact, Kafka ID/version, quorum baseline and two process subplans. Each subplan binds its ID, role, absolute configuration path, exact unit, runtime user/Java/heap, listener/JMX/exporter ports, data/metadata paths, log path and approved roots. Native commands must identify both the parent and process role; receipts must never be interchangeable across the two processes.

Readiness must inspect both candidate storage/config/service namespaces, ensure no symlink/foreign metadata or nested path collisions, prove free ports and bidirectional routes, and budget both heaps plus OS reserve. Disk/inodes must be assessed per actual mount instead of counting the same free space twice. Read-only package content can be shared; mutable metadata, service configuration and application logs cannot be shared between the JVMs.

For dynamic mode, stage both layouts, format only proven-empty candidate trees with the existing ID, start/verify/catch up/admit the controller, prove its directory/voter identity, then start and independently verify the broker. Do not restart existing members. For static mode, execute the supplied eight maintenance phases, including stopping existing brokers/controllers and applying consistent voters everywhere; start the candidate broker only after quorum verification. Never format existing members.

## Failure and resume design

Parent states need at least `ACCEPTED`, `VALIDATING`, `PREPARING`, `CONTROLLER_FORMAT_INTENT`, `CONTROLLER_FORMATTED`, `BROKER_FORMAT_INTENT`, `BROKER_FORMATTED`, `CONTROLLER_STARTING`, `OBSERVER_CATCHING_UP`, `ADMISSION_INTENT`, `CONTROLLER_VERIFIED`, `BROKER_STARTING`, `BROKER_VERIFIED`, `VERIFYING`, `PUBLISHING`, `RECOVERY_REQUIRED`, `SUCCEEDED` and pre-mutation `CANCELLED_SAFE`, plus the applicable static maintenance phases. These are a design contract, not installed execution states.

| Failure point | Required recovery |
|---|---|
| Before mutation | Cancel/release host and both IDs together |
| Format/start timeout or lost callback | Inspect that process's actual metadata/config/unit against its frozen identity and receipt; never blindly format again |
| Admission outcome uncertain | Read actual voters, directory ID and endpoints; do not replay promotion, remove or stop a possibly admitted voter |
| Controller verified, broker failed | Retain controller checkpoint and both reservations; reverify controller health, resume incomplete broker work; publish neither role yet |
| Static maintenance interrupted | Retain all participant ownership, verified backups and original states; recover the actual script phase after inspecting active files and quorum |
| Both verified, publication interrupted | Reverify health and complete the same idempotent transaction; do not repeat admission/formatting |

Publication must atomically create both per-role service/node identities, add both IDs and mark the host assigned. Success and reservation release occur only after the transaction commits. Existing configuration/restart/delete writers and generic job retry/cleanup must honor the parent reservation; no generic rollback may erase a potentially active controller.

## Remaining acceptance

Phase 5 execution/recovery; parent ledger and multi-ID guards; typed two-process agent readiness/execution; process-specific durable receipts and recovery; atomic publication; Jobs progress/resume UI; Linux failure injection (especially verified controller plus failed broker); and real dynamic/static lab pilots remain pending. This planning milestone is not a Phase 6 execution-complete or production-readiness verdict.
