# Configuration job recovery

Failed configuration jobs offer **Retry / Resume** and **Roll back this change**.
This applies to agent-managed internal and external Kafka clusters. Recovery is
explicit; a failed health gate does not trigger automatic configuration changes.

## Checkpoints and progress

- Before mutation, persist each node's original editable values in the job step.
  Preserve absent values as null and dynamic override source/scope separately.
- Never overwrite that checkpoint on Retry / Resume. Agent mutation task IDs and
  rollback task IDs are distinct durable fields, recorded atomically with queueing.
- Refuse rollback while the original task or an earlier rollback is still running.
  Outstanding configuration tasks also prevent a conflicting cluster operation.
- Recover the failed node first, then completed nodes in reverse rollout order.
  Untouched nodes are not restarted. Completed rollback steps are skipped on retry.
- A successful agent write/restart followed by a failed health check is verified
  on resume, rather than blindly repeating its restart.

## Restoring values

Static recovery patches only the job's properties and creates another file backup
before writing. Unrelated settings and node-specific protected properties remain
unchanged. A missing original property is removed, not written as an empty value.
Reject conflicting changes made since the job. Internal recovery validates the
configuration node ID; external recovery also checks the original discovered
agent, node, unit and configuration path. External service binding and restart
permissions are checked before recovery writes.

Dynamic recovery uses Kafka validation and Admin API operations. Restore a previous
per-broker override with SET; remove the job's override with DELETE when the
original value was inherited. Do not convert inherited defaults into overrides.
Back up live dynamic state before restoration and verify override readback.

## Health gates

Before restarting a running node, require the normal cluster health checks and
broker min-ISR safety or controller quorum safety. The failed/inactive node
already modified by this job may be recovered while the cluster is degraded.
It must recover before another node is restarted.

After static recovery, verify the file, service restart result, broker reachability,
active controller, partition leaders, replica/ISR recovery, min-ISR compliance,
agent heartbeat and disk health. Existing consumer-lag limits also apply. Controller
changes verify caught-up quorum voters. Require two consecutive healthy samples.
Any failure stops recovery and exposes **Retry rollback**.

## Compatibility and installation

Deploy the updated management server and UI along with agents reporting
`configchange.9` or newer. Agent binaries alone do not add the UI action.
New jobs capture recovery checkpoints. Existing legacy jobs do not gain missing
checkpoints retroactively; do not guess their prior state from arbitrary backups.
Approval requirements on versioned configuration changes remain in force.

Completed changes use the separate Configuration History restore-version flow.
Standalone rolling restart does not change configuration and offers Retry / Resume.
Versioned job recovery retains its existing rollback version history.

Node backup files retain the existing seven-day retention policy. The small,
non-sensitive per-property recovery checkpoint is persisted with job history;
recovery does not require selecting a filesystem backup by timestamp. Do not delete
job history if recovery of those jobs must remain available.

The release packages contain Linux amd64 and arm64 builds and source ZIPs, without
live agent environment files, tokens or private keys. For an existing installation,
preserve its service configuration, TLS trust, credentials and state directory;
replace only its matching binary with a backup and controlled agent restart.
