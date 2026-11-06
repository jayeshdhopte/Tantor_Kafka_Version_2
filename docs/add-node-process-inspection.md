# Local process inspection development and agent package

## Operational scope

Deployment is on hold by user instruction. Do not upload, install, restart agents or replace application artifacts until the user explicitly lifts that hold. **192.168.3.213 is excluded from all Add Node connections, probes, tests, agent updates and rollouts.** It is being used for other work. The local SSH helper also removes it from the permitted targets. Future lab work is limited to the explicitly authorized roles on 194, 229 and 191; that list does not override the deployment hold.

The prepared [lab configuration](examples/add-node-inspection-lab.yml) permits only the two intended participant host IDs and excludes both 213's address and its recorded `broker6` host ID. It is a file for a later reviewed deployment, not a live configuration change. Existing broker readiness can create peer/client route probes; before any future live use, remove 213/broker6 from those probe inputs as well. Never use a broad old four-node rollout helper.

## Implemented capability

The `.6` internal-agent source adds `INSPECT_ADD_NODE_PROCESS` with `add-node-process/v1`. It observes one **recorded existing process**, selected by service assignment ID rather than a caller-provided host or shell command. It supports broker-only, controller-only and existing combined-role processes, including node ID 0. New separate-process proposals still require distinct role-only identities.

The agent uses two fixed `systemctl show` observations and bounded file reads. It checks the exact unit, MainPID/start identity, active configuration hash, `node.id`/`process.roles`, each data/metadata directory's Kafka cluster/node/directory IDs, and the PID's actual Kafka JVM/configuration arguments. It rejects missing/foreign/duplicate metadata, symlinks, ambiguous identity properties and changes during observation. It never returns raw config files, unit contents or JVM arguments, which may contain credentials. Hashes and selected identity evidence are returned instead.

The command does not stop, start, restart, format, chown, stage files or change voters. It makes no network connection to Kafka or peer nodes. Native execution is Linux-only. Filesystem/service errors produce blocked evidence; there is no automatic remediation.

`identityVerified=true` means the recorded process/configuration/storage identities were observed consistently. It does **not** prove Kafka artifact/version bytes, controller voter membership, broker unfencing or overall cluster health. `versionVerified=false` and `executionReady=false` remain mandatory, even when identity is observed. Those missing proofs are not converted into passing readiness.

## Backend and UI

- `POST /api/v1/ui/clusters/{clusterId}/add-node/process-inspections` with `{"serviceId":"<assignment UUID>"}` queues one leased read-only task.
- `GET /api/v1/ui/clusters/{clusterId}/add-node/process-inspections/{taskId}` returns RUNNING, OBSERVED, BLOCKED, STALE or EXPIRED.
- Both endpoints require ADD_NODE permission. A service must belong to the managed, available KRaft cluster. Its host, exact unit/configuration path and identity come from the service inventory; incomplete references block instead of being guessed.
- Dispatch is denied by default. `tantor.add-node-inspection.allowed-host-ids` is an explicit opt-in; exclusions by host ID/IP take precedence. The agent must advertise addnode.6 or newer. Active cluster operations block inspection scheduling.
- The frozen request binds an inspection nonce, cluster/host/service context and two-minute expiry. Results are checked against those values and a strict typed evidence contract. Changed references, agent version or policy invalidate the observation. Failure logs are not forwarded as evidence.
- Controller and separate-process review screens offer an explicit per-participant inspection button only for permitted, compatible hosts with recorded references. Opening a plan does not automatically queue inspection. Polling reuses the same task; a lost poll does not silently create a new request. Refreshing/changing plan context clears prior observations.
- Agent callbacks for this command cannot rewrite the existing cluster lifecycle. Generic task retry/rollback/cleanup rejects the command; request a fresh observation through its own endpoint.

No new database migration or Kafka mutation command is introduced. Existing broker v1/v2 plans and receipts are unchanged. Existing configuration, rolling-restart and data-service executors are unchanged.

## Remaining scope

This implements participant observation, not complete controller or separate-process execution. Candidate controller/two-process readiness, approved runtime artifact proof, durable multi-host/two-ID operation ownership, native maintenance/admission execution, reconciliation/resume, atomic publication and Linux lab acceptance remain required. Deployment and native Linux validation are not implied by a prepared binary.

## Local validation and artifacts

The selected backend regression suite completed 322 tests: 286 passed, no failures/errors, and 36 opt-in database tests skipped. It covers the new inspection endpoint/evidence checks plus broker addition, controller/separate planning, rolling restart, configuration/recovery, multi-instance data services and masking. The production UI build and changed-file lint passed; seven inspection browser scenarios and the 22 existing broker/controller/separate planning scenarios passed.

Nine portable agent contract tests passed through Go's local WebAssembly/Node runner, including metadata/role mismatches, changing PID/config/unit/metadata, secret-bearing input redaction, combined-role/node-zero observations and no privileged mutation calls. Windows Application Control blocked the rebuilt native Windows test executable. The WebAssembly result is a portable-contract result, not a Linux runtime acceptance claim. Go vet and Linux compilation are checked separately; the Linux-only symlink/bounded-read test and live systemd integration still need an authorized Linux run on an allowed node after the hold is lifted.

`internal-agent/build-release.py` builds both Linux architectures from an exact committed Go source snapshot, embeds that source commit and UTC build time, verifies ELF architecture/build metadata and writes `SHA256SUMS`, a source archive and `BUILD-ADD-NODE.json`. It only creates local artifacts and makes no VM connection. Use a new local build directory; it refuses to erase/reuse an existing one.

No VM has been contacted or modified for this development increment. The `.6` binaries are prepared artifacts, not installed agents and not a controller/separate-process execution-complete release.
