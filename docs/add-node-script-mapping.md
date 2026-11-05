# Mapping the supplied Add Host scripts to Tantor

Reference folder: `C:/Users/Translab/Downloads/add_host` (copied from `/root/add_host` on 192.168.3.194). Rechecked on 5 November 2026. These scripts are workflow inputs, not a playbook automatically executed by the platform.

The platform uses its existing Java backend, durable tasks and Go host agents. It translates the source workflow's responsibilities into those components; it does not introduce an Ansible/SSH execution service or replace its existing agent infrastructure.

| Supplied source | Responsibility retained | Tantor implementation / delivery phase |
|---|---|---|
| `add_host.yml` | Validate selected cluster/host, choose role-specific workflow, observe health, then execute controlled steps | Phase 1 React wizard; Phase 2 readiness service. Deployment orchestration remains Phase 3+ |
| `tasks/common_preview.yml` | Cluster identity/topology/features, health, fresh host inspection, service/storage conflict checks, Java and package availability | Phase 2 `AddNodeKafkaReadiness`, `AddNodeReadinessService` and agent `readiness.Inspect` |
| `tasks/role_preview.yml` | Broker role/identity, free ports, controller endpoint connectivity and inherited quorum configuration | Phase 2 broker checks B01-B11. IDs are proposed only, not reserved in preview |
| `tasks/cluster_lock.yml` | Exclude competing cluster/host operations | Phase 2 observes the existing shared job guard and host tasks. Atomic reservations/ownership are Phase 3; no lock files are created in preview |
| `tasks/storage_format.yml` | Prepare new owned storage and format with the existing Kafka ID | Phase 2 inspects only. Package/config/storage preparation and guarded formatting are Phase 3 |
| `tasks/service_deploy.yml` | Role-specific systemd units, startup and listener readiness | Phase 3; not invoked by readiness |
| `tasks/verify_broker.yml` | Verify intended broker membership and preserve controller voter set | Phase 4; API discovery alone will not be accepted as explicit unfenced-state proof |
| `tasks/post_health_gate.yml` | Confirm health after joining and release ownership safely | Phase 4; must include complete evidence and account for in-flight tasks |
| `tasks/quorum_expand.yml` and `tasks/verify_controller.yml` | Observer setup, catch-up, voter admission and membership verification | Phase 5; controller choice remains disabled now |
| `tasks/static_quorum_expand.yml` | Static controller maintenance expansion | Outside agreed first-release scope; latest Draw.io blocks direct static controller addition |
| `templates/kafka_server.properties.j2`, `templates/kafka.service.j2` | Configuration/service intent | Reference for Phase 3. Resolve real platform paths, runtime policy and supported package adapter rather than hard-code illustrative paths |
| `group_vars/all.yml`, `vars/add_host_vars.example.yml` | Input defaults and policy | Resolve through managed cluster configuration and validated backend policy. Do not import execute/cleanup defaults or temporary data paths |
| `cleanup_voters.yml` and migration runbook | Environment-specific cleanup/migration | Not called by Add Node; separate operations with separate scope |

## Deliberate corrections

1. The source `common_preview.yml` can stop candidate services when auto-reconciliation is enabled. Tantor readiness records a conflicting service and blocks; it never stops it.
2. The source `storage_format.yml` can remove candidate directories when `kafka_auto_reconcile` is true (the supplied defaults enable it). Tantor preview never removes directories. Unknown or foreign storage is a blocker; later retries must reconcile ownership and identity before considering any mutation.
3. The source outer rescue can restart existing controllers and remove locks after failure. That is not copied into the online Add Node recovery path. Later failure handling must inspect actual membership and account for old tasks before releasing reservations.
4. Source broker ID calculation uses a broker-specific range in places. Tantor proposes from known broker/controller/observer and stored identities, then must reserve transactionally at deployment time.
5. Source Java checks and guessed paths are replaced with explicit package/runtime policy and actual service-user observations. Unsupported/missing observations stay blocked, rather than falling back to a plausible value.
6. Package presence is not inferred from a directory or database receipt alone. Phase 2 checks approved metadata, downloaded SHA-256 and installed archive payload identity separately.
7. A candidate-side connection does not prove reverse reachability. Phase 2 adds nonce/source-bound temporary probes from existing peers and explicitly configured representative client hosts. No Kafka process is started for these probes.
8. Kafka CLI text matching is replaced by typed read-only Kafka API observations where supported. Unsupported API measurements stay unknown; final runtime verification remains a later phase.

## Design references and coverage

The newer three-sheet Draw.io file supplies the C01-C21 / B01-B11 / T01-T10 / D / E check map. Some of these expanded requirements exceed the actual older Ansible checks; assigning a code is not a claim that the older scripts already implemented the entire requirement.

Phase 2 documents exactly what each observation measures in [readiness implementation notes](add-node-phase2-readiness.md). In particular it supplies baseline resource/route/package observations, not workload throughput certification, a continuous health monitor, final rendered-config certification, or post-start Kafka registration proof.

No source playbook has been executed against a VM during this integration. The workflow is implemented through typed backend and native-agent commands. The 6 November follow-up validated the approved 4.3.0 package, authenticated broker addition on 191, safe same-operation resume and a candidate produce/consume test. Existing Kafka services were not restarted for the platform/agent upgrade. See [the current deployment verdict](add-node-deployment-194.md).

The broker workflow implements the typed plan, final validation, durable reservations/checkpoints, candidate preparation/format/start commands, verification, atomic publication and reconciliation/resume. Broker 102 was added on 191, preserving the Kafka identity and controller voters. The earlier [execution design snapshot](add-node-broker-execution-contract.md) is historical; the deployment report records current coverage and remaining release scope.
