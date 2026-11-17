# External Add Node - Phase 6 separate broker/controller processes

17 November 2026. Source implementation and automated validation; **not deployed, not live-accepted**. The user authorized advancing development while prior live acceptance remains pending. No platform or running agent was replaced. Kafka is provisioned by the user.

## Scope and contract

One fresh enrolled external-agent host receives two Kafka JVMs for an existing supported, explicitly PLAINTEXT **dynamic KRaft** cluster. Static expansion, static-to-dynamic migration, an occupied host, and a combined `process.roles=broker,controller` process are outside this external workflow. The supplied add_host observer/catch-up/admission sequence remains authoritative.

The existing Phase 5 external parent contract is extended rather than creating two unrelated deployments:

- `POST /api/v1/ui/external-clusters/{cluster}/add-node/controller-previews`: `{commonPreviewId, controllerPort, participantParcelDirs, separateProcesses:true, controllerJmxPort?, brokerJmxPort?}`. The broker listener and rack come from the reviewed common request. Optional monitoring ports inherit when omitted; inherited monitoring cannot be disabled. Every listener/exporter port must be distinct on the candidate.
- The preview returns `separateProcesses`, both proposed IDs and full `processes` profiles (standard service/configuration, ports, paths and heap), current participants, digest, expiry and release blockers. Two-process review requires a **full common readiness PASS**, including the broker profile/rack, then independent native validation of both profiles.
- Submission uses the existing `controller-operations` endpoint with only the preview/context/digest/stable request key. Layout and both IDs are server-owned. Request-key lookup, owned Resume and pre-mutation safe cancellation use that same parent endpoint. The tracked job remains `EXTERNAL_CONTROLLER_ADDITION`; its immutable payload says `layout:SEPARATE` and the UI identifies it as a broker/controller parent.
- Runtime observations and role-specific actions use the existing authenticated, bounded `controller-tasks` queue. Frozen task parameters identify `role:controller` or `role:broker`. Candidate-only commands cannot target existing participants. Runtime commands are read-only.

The broker ID is the maximum existing broker ID plus one; the controller ID is `max(100, maximum existing participant ID)+1`. Any cross-role, existing or retained-ID collision blocks execution instead of silently skipping identities. The exact existing Kafka version, approved checksum and cluster ID are shared by both new processes.

## Frozen profiles and ownership

The profiles use `controller.service` / `broker.service` and the reviewed configuration directory's `controller.properties` / `broker.properties`. User-reviewed installation/configuration directories, Java/runtime user, role-specific data/metadata/log paths and JVM heap remain part of the immutable plan. Journals are private recovery state, not generated service/configuration paths.

The strict shared plan rejects overlapping storage/log/runtime/configuration paths, copied directory identities, duplicate listeners/JMX ports and incompatible inheritance. Native validation checks both heaps plus OS reserve and charges disk/inode reservations per process on each actual mount; the same remaining filesystem capacity cannot be counted twice. C19 persistent non-root writable mount and runtime-user access checks apply to both selected storage/log layouts. Preview writes no installation, service, config, storage or operation journal.

V112 adds a frozen optional second broker ID and its published-node reference to the existing parent ledger. A transactional trigger reserves the second ID in the same shared resource ledger as the host, cluster and controller ID. Both IDs remain permanent after success. Cancellation before any dispatched mutation releases the parent resources; after dispatch, recovery retains both IDs, host and cluster. Other jobs, service mutation queues and discovery cannot publish or alter the owned candidate or participants early.

## Controller-first execution

1. Reserve the host and both IDs together; repeat selected-layout validation, participant runtime identity and dynamic baseline checks.
2. Prepare **both** isolated layouts while the candidate has no running Kafka process. This lets recovery resume broker formatting/start after controller admission without treating its sibling as a foreign Kafka process. Preparation starts neither JVM.
3. Format only proven-empty controller storage using the existing cluster ID and dynamic observer formatting. Start it as an observer.
4. Independently verify catch-up and admit it as a voter; exact existing voter/directory identities must remain intact. An already admitted voter is reverified without repeating admission.
5. Format the prepared broker's empty storage once. Start it only with a fresh parent/digest/controller-directory-bound voter proof; the native adapter independently verifies the matching running sibling controller against that proof.
6. Reverify original participant runtimes. Collect three separated samples proving the owned running controller, exact expanded voters, owned running broker, unfenced registration, exact advertised endpoint/rack, cluster ID, log-directory/partition/ISR health and topic-list smoke check. Final progress stays incomplete until publication.
7. Publish both process records, candidate agent ownership, broker count, parent success, final job step and job success **in one transaction**. Neither process appears as a healthy published addition if any required verification or publication callback fails. Generic retries cannot bypass the parent ledger.

Existing brokers/controllers are not stopped, restarted or reconfigured by this dynamic path.

## Failure and Resume

The parent retains the immutable two-process plan and both identities. Resume waits for prior task leases plus the established settlement margin, retires stale work and advances the generation. Older workers/tokens cannot publish a later attempt's state.

| Actual evidence | Resume behavior |
| --- | --- |
| Interruption during initial preparation; neither process active | Inspect both role journals and continue only incomplete owned preparation |
| Admitted controller, broker PREPARED | Reverify the controller; format/start only the broker; never remove/re-admit the controller |
| Broker FORMAT_INTENT with complete matching metadata | Use adoption-only RECONCILE_FORMAT, which cannot invoke the formatter; then use a fresh voter proof for start |
| Partial/foreign/unknown broker format or missing journal after a claimed format/start | Hold both reservations for investigation; no formatter replay or cleanup |
| Broker START_INTENT with matching active runtime | Adopt the running broker and verify both; no repeated preparation/formatting |
| Controller stopped, tools still active or identity/mount/runtime proof missing | Hold for investigation; no automatic voter removal or storage deletion |
| Both running, publication interrupted | Reinspect and reverify both roles, then retry the same atomic publication transaction |

An admitted controller with no proven prepared broker is held for investigation. The flow never relaxes the fresh-host rule to prepare a new unrelated broker beside an already active controller.

## Release controls and compatibility

The external source version ends in **-an.5**. All selected participants and the candidate must run that compatible adapter for separate-process checks. Existing broker/controller-only flows retain their an.3/an.4 compatibility where previously supported; an.5 is also recognized.

Participants need authenticated enrollment, task access and Add Node inspection. The candidate additionally needs:

```text
--enable-tasks
--enable-add-node-inspection
--enable-separate-addition
```

`TANTOR_AGENT_ENABLE_SEPARATE_ADDITION=true` is the equivalent explicit mutation opt-in. The existing broker-only or controller-only mutation opt-in does not authorize separate-process mutations. Separate execution does not grant legacy arbitrary config-write/restart/data-service permissions.

All required backend controls remain default-off:

```properties
tantor.external-add-node.execution-enabled=false
tantor.external-add-node.phase2-accepted=false
tantor.external-add-node.phase4-accepted=false
tantor.external-add-node.controller-execution-enabled=false
tantor.external-add-node.phase5-accepted=false
tantor.external-add-node.separate-execution-enabled=false
```

The external module reuses native library code under `internal-agent/source`. This extension adds an external separate-process facade and validates both selected profiles. The internal agent executable/version and its execution entry points were not changed; no running internal agent was updated. Selected internal regressions are part of validation.

## Demonstration and acceptance checklist

The real React fixture demonstration is generated by `tantor-ui/scripts/test-external-add-node-phase6-browser.cjs`; screenshots and its acceptance report are in Downloads under `External_Add_Node_Phase6_Demo`. **Fixtures are not a real broker/controller addition.**

Automated checks cover strict two-profile planning, standard paths/units, distinct IDs/ports, selected-layout flags, both-ID reservation arbitration, frozen identity, token/generation/lease fencing, three-sample requirements, atomic publication rollback/idempotency, broker-count updates, and recovery from prepared/format-intent/running/partial/missing broker states after controller admission. Linux tests cover the external adapter and shared candidate/storage/recovery logic. Existing external broker/controller and internal separate-process browser regressions are included.

Before accepting this phase:

- [ ] Finish pending live broker and dynamic-controller acceptance on the designated external test cluster.
- [ ] Coordinate matching backend/UI migrations and an.5 external agents in an approved test rollout; keep execution disabled until prerequisites are recorded.
- [ ] Add one fresh host with both roles through the wizard; record operation/job IDs, both Kafka IDs, directory IDs, units, paths, approved runtime version/checksum and monitoring ports.
- [ ] Prove controller catch-up and exact voter membership precede broker start; existing cluster processes remain running.
- [ ] Prove broker registration/unfenced state and three stable health samples precede publication of either process.
- [ ] Interrupt before/after formatting, admission, broker start and publication, including lost responses and backend/agent restart. Record formatter/admission counts; Resume must not duplicate them or remove the admitted controller.
- [ ] Exercise partial/foreign storage, occupied ports, inadequate combined resources, stale checks, unsupported/old agents, concurrency, permissions and disabled gates.
- [ ] Confirm both-node inventory, broker count, monitoring, job/audit evidence and restart/config/Connect/Schema regressions in the live environment.

Do not mark live acceptance complete or advance to a production release solely from these automated results. Phase 7 is the remaining release-validation phase.

## Recorded automated validation

- Selected backend regression: **544 tests, 484 passed, 60 explicit opt-in integration skips, zero failures/errors**. External readiness and broker/controller/separate-process PostgreSQL checks ran against a dedicated loopback database at port 65440 using private rollback-only schemas. No application database was migrated.
- Recovery handler checks cover an already admitted controller with a prepared, running or matching format-intent broker; partial storage and missing journal after a claimed format fail closed. Native candidate checks additionally prove that lost broker-format responses preserve the running controller and do not repeat its admission or the broker formatter.
- External Linux Go suite/vet, shared native expansion/broker/readiness/layout/facade suites and Linux amd64/arm64 external-agent builds passed in `/var/tmp/tantor-external-phase6-validation` on 191. Real Kafka mutation tests were not enabled.
- TypeScript, changed-component ESLint and the production UI build passed. External wizard browser regressions for Phases 1-5 and the internal separate-process wizard passed. The Phase 6 real React fixture demonstration passed **12 scenarios**, including two reviewed profiles and monitoring overrides, five read-only check requests, one deployment submission, one lost-response lookup, one owned resume and zero independent broker-review requests.
- Neither platform, running external/internal agents nor Kafka services were deployed, replaced or restarted. Existing Downloads agent kits are unchanged. Actual separate-process admission, broker registration and live interruption/recovery acceptance remain pending.
