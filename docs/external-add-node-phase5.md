# External Add Node — Phase 5 dynamic controller addition

16 November 2026: implementation and automated validation. **Not deployed and not live-accepted.** A fresh read-only query on 194 found zero non-deleted external clusters. The user will provision Kafka; this development does not install Kafka or replace running agents. Earlier live phase acceptance remains pending.

## Scope and script alignment

This phase adds the **external-agent** controller-only path for supported, explicitly PLAINTEXT, finalized **dynamic KRaft quorums**. It follows the supplied `Downloads/add_host/tasks/quorum_expand.yml`, `storage_format.yml` and `verify_controller.yml` observer/admission sequence:

1. Preserve the existing Kafka cluster ID and exact Kafka version/checksum.
2. Prepare only the fresh candidate, using `controller.service` and the reviewed configuration directory's `controller.properties`.
3. Format only proven-empty candidate controller storage with `--no-initial-controllers`. Never use `--standalone` or initial-voter formatting to add a controller to an existing quorum.
4. Start the controller as an observer using the existing controller bootstrap routes.
5. Verify actual metadata directory identity, recent catch-up and bounded replication lag.
6. Admit through `add-controller` only after fresh independent observer/absence proof, or skip admission if exact live membership already proves the candidate is a voter.
7. Verify exact expanded voter membership, unchanged original voter directory identities, candidate endpoint/directory, existing unfenced brokers, stable cluster health and the script's topic-list smoke check. Publish inventory only after verification.

Existing brokers/controllers are not stopped, restarted or reconfigured by this dynamic path. Static quorum, implicit static-to-dynamic migration and external Broker + Controller are unavailable here. The existing internal workflows remain separate.

## Wizard and readiness

The agreed four-screen external wizard now exposes Controller. Common checks return a server-owned `controllerCommonPassed` flag. A failed **unused broker profile** (for example its listener port, rack or storage paths) cannot authorize a broker and does not block a controller-only review when the required common identity/package/Java/network/cluster checks pass. Missing, stale, duplicate or failed required common evidence blocks controller planning.

Controller installation/configuration directories, data/metadata paths, logs, Java/runtime user and JVM heap are entered in Common checks. Controller review additionally accepts its listener port and optional mappings from participant agent IDs to their actual approved cached parcel directories. An empty mapping uses the candidate parcel directory for participants. Paths remain canonical and restricted to approved roots.

**Run controller checks** schedules read-only observations. Every existing process must prove its actual unit/PID/start identity, configuration/unit checksums, cluster/node/storage directory identities and Kafka JVM libraries against the approved matching artifact. Missing participants, ambiguous combined units, differing controller custom settings or monitoring policies, and stale/inconsistent observations prevent freezing the plan. Directory and membership identities are observed, never fabricated from inventory labels.

The selected controller receives its own package, Java, empty storage, free port/JMX port, memory/heap/reserve, disk/inode and service-user checks. The supplied **C19 non-root persistent mount and access policy** applies to the controller's intended storage/log paths independently of the unused broker paths. Selected-layout preview creates no candidate journal, service, configuration, installation or storage directory; temporary listener probes close immediately.

The proposed controller ID follows the supplied controller allocation rule: `max(100, highest existing participant node ID) + 1`, with global collision/reservation protection. Review proposes an ID; acceptance reserves it. Existing IDs are unchanged.

## External contract

All endpoints below require explicit Add Node permission and use `/api/v1/ui/external-clusters`:

| Method / path | Purpose |
| --- | --- |
| `POST /{cluster}/add-node/controller-previews` | `{commonPreviewId, controllerPort, participantParcelDirs}` starts scoped read-only observations |
| `GET /{cluster}/add-node/controller-previews/{id}` | Current checks, participants, proposed identity, standard unit/path, expiry, digest and release blockers |
| `POST /{cluster}/add-node/controller-operations` | Accept `{previewId, contextHash, planDigest, requestKey}`; no client-supplied native plan or allocated ID |
| `GET /{cluster}/add-node/controller-operations/request/{key}` | Confirm an uncertain submission with the original request key |
| `GET /{cluster}/add-node/controller-operations/{id}` | Tracked job, checkpoint, error and verified publication status |
| `POST /{cluster}/add-node/controller-operations/{id}/resume` | Reconcile/resume the same owned operation |
| `POST /{cluster}/add-node/controller-operations/{id}/cancel-safe` | Cancel only before any candidate mutation was dispatched and after previous tasks settle |
| `POST /discovery/controller-tasks/{agent}/claim` | Claim a scoped external observation/action with a token and absolute lease |
| `POST /discovery/controller-tasks/{agent}/{id}/result` | Immutable, identity/digest/time/token-bound result; identical delivery retries are idempotent |

The Job type is `EXTERNAL_CONTROLLER_ADDITION`, with six visible steps: final checks, preparation, formatting, observer startup, catch-up/admission, and final verification/publication. The Job page's **Retry / Resume** uses the controller operation endpoint. Generic retry/rollback cannot bypass ownership.

## Ownership and execution states

V111 adds separate controller preview, operation and task tables, plus shared external broker/controller reservation arbitration. The shared owner view protects candidate discovery, participant inventory identity, conflicting jobs and other mutation queues. Cluster/candidate leases last through recovery; allocated IDs remain protected after verified publication. Normal metrics/heartbeat updates remain compatible. Preview inputs and the frozen native plan cannot be rewritten after freezing.

The main states are `ACCEPTED`, `VALIDATING`, `PREPARING`, `PREPARED`, `FORMATTING`, `FORMATTED`, `STARTING`, `OBSERVER_STARTED`, `CATCHING_UP`, `ADMITTING`, `VOTER_VERIFIED`, `VERIFYING`, `PUBLISHING`, `SUCCEEDED`; interruptions use `RECOVERY_REQUIRED` and `RECONCILING`. Pre-mutation cancellation uses `CANCELLED_SAFE`.

Final checks repeat actual participant/runtime and candidate readiness before candidate changes. The frozen plan binds the existing cluster/version/artifact, configuration inheritance, all participant identities, finalized features, voter directory identities, reviewed profile and candidate identity. The stable external binding tolerates compatible heartbeat refreshes while rejecting membership, enrollment, connection or policy drift. Kafka API observations independently check the finalized features and exact expected quorum membership.

## Failure and resume

Every task has an absolute execution lease; native work and artifact downloads are bounded to it. Running actions are not reclaimed immediately. Resume waits for completion or the task deadline plus 190 seconds, retires unclaimed/expired work, increments a durable generation and starts fresh inspection. Retired tokens, stale worker generations and attempts to replace completed results are rejected. Kafka CLI subprocess groups are terminated on timeout; fresh formatter/admission-tool observations provide additional recovery evidence.

| Actual evidence | Recovery behavior |
| --- | --- |
| No journal/empty storage, with no claimed format/start/admission | Repeat final validation and continue owned preparation |
| Owned partial preparation and empty storage | Continue idempotent preparation; conflicting existing files block |
| `PREPARED`, empty storage, verified files/unit and mounts | Format once |
| `FORMAT_INTENT` with matching completed metadata | Use **RECONCILE_FORMAT**, an adoption-only native action that cannot enter the branch which invokes a formatter; then start |
| Partial/foreign/unknown metadata, missing journal after claimed formatting, active/unknown formatter or changed ownership | Keep reservations and hold for investigation; no blind formatting or cleanup |
| Formatted, inactive, owned controller | Start the same frozen unit |
| Running observer / uncertain admission | Re-read exact directory and live quorum membership. An already admitted voter skips `add-controller`; a proven caught-up observer needs a new post-intent absence/catch-up proof before any admission retry |
| Stopped admitted/possibly admitted controller | Hold for explicit quorum investigation; do not remove a possible voter or discard its storage |
| Publication interrupted | Reinspect/reverify the same running controller and complete the idempotent publication transaction; no prepare/format/admission replay |

Successful publication requires three complete, consecutive health/membership samples at least five seconds apart, fresh native ownership/runtime/mount evidence, exact candidate directory/endpoint membership, and unchanged original voter identities. It commits the controller node, agent ownership, operation, final step and job success in one transaction. Broker count and unrelated management permissions remain unchanged. Discovery cannot publish the candidate early while ownership is retained. An old worker cannot downgrade committed success or overwrite a later resume attempt.

An uncertain submission response locks the wizard's navigation/refresh controls and preserves one request key until a read-only lookup confirms its operation. Readiness, startup and voter admission are displayed as distinct checkpoints; only server-confirmed `SUCCEEDED` reports verified inventory publication.

## Agent compatibility and release controls

The external source version now ends in **`-an.4`**. Every participant needs that compatible external adapter with task access and Add Node inspection enabled for its read-only process/runtime observations. The candidate separately needs:

```text
--enable-tasks
--enable-add-node-inspection
--enable-controller-addition
```

The corresponding new mutation flag is `TANTOR_AGENT_ENABLE_CONTROLLER_ADDITION=true`. Keep the established authenticated enrollment/server/host identity settings. Broker execution's existing opt-in does not enable controller execution. Legacy config-write/restart/data-service permissions are independent and are not automatically granted.

Backend controller execution requires all four controls; they remain false by default:

```properties
tantor.external-add-node.execution-enabled=false
tantor.external-add-node.phase2-accepted=false
tantor.external-add-node.phase4-accepted=false
tantor.external-add-node.controller-execution-enabled=false
```

Broker-only operations retain compatibility with tested `-an.3` and `-an.4` candidates. Controller work requires `-an.4`. Older or incompatible adapters cannot become controller execution candidates.

The external Go module imports the shared native library under `internal-agent/source`. This phase adds an external facade, read-only selected-controller validation and a guarded format-intent adoption action there; the internal agent entry point/release version is unchanged. Existing internal execution regressions were checked. No running internal agents were updated, restarted or replaced.

Matching backend/UI migrations and external-agent rollout must be coordinated after acceptance planning. The existing Downloads agent kits were not relabelled or replaced by this development.

## Demonstration and live acceptance

The real React wizard/Job-page fixture demonstration is generated by `tantor-ui/scripts/test-external-add-node-phase5-browser.cjs` and saved in Downloads under **External_Add_Node_Phase5_Demo**. It is labelled test data and is not evidence of a real controller admission.

| Scenario | Automated evidence / live status |
| --- | --- |
| Controller-specific review, profile controls and standard service/configuration | Browser/native-plan contract fixtures passed |
| Unused broker-profile blockers do not authorize brokers or prevent controller review | Server common-gate and browser checks passed |
| Static/unsupported scope and disabled release gates block execution | Backend/native/browser tests passed |
| Full participant runtime coverage; missing process evidence cannot freeze a plan | Public planner contract tests passed |
| Shared broker/controller reservation, immutable plan, old tasks/tokens/generations and active lease protection | Real isolated PostgreSQL tests passed |
| Lost admission response with an already admitted voter does not repeat mutation | Handler/typed quorum/native recovery tests passed |
| Unknown/partial format never invokes a second formatter; matching intent has an adoption-only path | Native/backend recovery tests passed |
| Atomic publication, rollback on callback failure, idempotency, unchanged broker count/permissions | Real PostgreSQL tests passed |
| Permissions and owned Resume routing | Controller endpoint and browser tests passed |
| Internal and existing external broker/config/restart/schema/Connect regressions | Selected backend and browser/native suites passed |
| Actual dynamic external cluster, observer catch-up, admission and connection/backend/agent restart demonstration | **PENDING — no external cluster registered on 194** |

Before accepting Phase 5, complete the pending live broker/readiness acceptance, provision and connect the designated **dynamic** external test cluster, and roll out matching gated builds in the approved test environment. Demonstrate: a normal controller addition; slow/failed catch-up; a lost admission response before and after membership changes; backend/agent interruption during prepare/format/start/admission; wrong/foreign directory rejection; active-lease protection; and publication recovery. Record operation/job IDs, voter/directory identities, broker continuity and formatter/admission invocation counts. Do not enable Phase 6's separate-process option or call the controller release accepted on build/fixture results alone.

## Recorded automated validation

- Final selected backend regression run: **458 tests, 416 passed, 42 explicit opt-in integration skips, zero failures/errors**. The isolated loopback PostgreSQL tests for external readiness, broker recovery/publication and controller reservations/leases/publication were enabled; no application database was migrated or used for writes.
- External Linux Go suite/vet, shared native expansion/broker/readiness/layout/facade tests and Linux amd64/arm64 builds passed on 191 under `/var/tmp/tantor-external-phase5-validation`. Real Kafka mutation tests were not enabled.
- Changed UI TypeScript/ESLint and production build passed. Phase 5's labelled browser demonstration passed 11 scenarios, with five read-only controller checks, one deployment submission, one read-only lost-response lookup, one owned resume and zero broker-review requests. It also exercises request-key generation without `crypto.randomUUID` for the VM's HTTP context.
- Prior external wizard suites and the internal separate-process wizard regression passed. No running internal/external agent, Kafka service or platform was replaced/restarted.
- Fresh read-only 194 inventory: **zero external clusters**. Real broker/controller acceptance and deployment remain pending.
