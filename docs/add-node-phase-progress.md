# Add Node phased delivery

Updated: 6 November 2026. Current deployment/acceptance status is recorded in [the 194 deployment report](add-node-deployment-194.md). Phase 1 notes below describe the original local milestone.

The latest execution work and remaining release gates are recorded in [the controller/separate-process execution core report](add-node-expansion-execution-core.md). Phases 5 and 6 are still in progress; their UI/API remain planning-only. Deployment is deferred until all remaining implementation and acceptance gates pass. Only 194 (platform), 191 and 229 (internal agents) are authorized rollout targets; 213 is excluded from all connections and task work.

## Phase 1: integrated setup preview

Status: implemented locally; not deployed to a VM.

The Add Node flow follows the team's HTML prototype: Select host -> Common checks -> Choose role -> Review. It is available from the cluster inventory action menu and the managed KRaft cluster Nodes tab. Legacy `/cluster-deployment?mode=add&clusterId=...` links redirect to `/clusters/:id/add-node`.

The page reads the existing cluster and host inventory APIs. Cluster name, Kafka version, Kafka identity, environment and reported broker port are inherited. Host options use the server's availability/deployment eligibility and assignment data. Offline, assigned, discovery-only and unknown-eligibility hosts cannot be selected. Unsupported cluster modes and observed active cluster jobs block progression. Administrator access is required to use this preview.

Broker is the only available role. Controller and separate-process options remain disabled. Check rows show **Not run**; visiting a step never marks readiness as passed. Node IDs are not allocated. No host assignment, package distribution, agent task, formatting or deployment is scheduled. Deploy remains disabled without a mutation handler.

Missing inventory, invalid responses and failed requests show a blocker with refresh recovery. Host selection is preserved through Back navigation; refreshing inventory resets the selection and wizard. Changing route cluster IDs creates a fresh wizard.

### Validation

- Production TypeScript/Vite build passed.
- ESLint passed for the changed TSX files.
- Browser acceptance checks passed against isolated API/role fixtures: four-step navigation, inherited values, host exclusions, unavailable roles, disabled deployment, empty/error states, refresh recovery, active ADD_HOST blocking, external-cluster blocking, legacy URL routing, both entry points and non-admin direct access.
- Browser fixture recorded no mutation requests and no runtime errors.
- Desktop and mobile screenshots inspected; no horizontal overflow in the mobile wizard.
- No live VM, agent or Kafka operation was run. Fixture acceptance does not establish live API permissions, live readiness, broker registration or recovery behavior.

The acceptance runner and evidence are in the parent workspace's `ui-verification/add-node-phase1-review.cjs` and `ui-verification/add-node-phase1/`. The runner intercepts API responses and the permission hook in an isolated test browser only; application authentication code and settings are unchanged.

## Remaining phases

| Phase | Outcome | Status |
|---|---|---|
| 2 | Real common and broker readiness checks; expected/observed results, freshness and context invalidation | Deployed on 194; Linux tests and authenticated live 4.3.0 candidate readiness passed |
| 3 | Broker job execution with final checks, host/ID reservations and safe candidate preparation | Deployed and exercised through the authenticated API; Kafka 4.3.0 candidate prepared, formatted and started as a non-root user |
| 4 | Verified publication, durable evidence, reconciliation and broker resume | Broker-only 4.3.0 pilot passed: same-operation resume without reformat, stable unfenced registration, unchanged voters, atomic publication and candidate produce/consume |
| 5 | Script-aligned static controller maintenance and dynamic observer/catch-up/admission | In progress: planning and local read-only participant inspection implemented; execution, durable recovery and controller pilots pending |
| 6 | Separate controller and broker processes with controller-first verification | In progress: two-process planning API/UI implemented; depends on Phase 5 execution and needs parent reservations, native execution/recovery and atomic publication |
| 7 | Full release acceptance, compatibility certification and deployment documentation | Not implemented |

Phase 2 replaces the old check action with dedicated read-only agent commands and Kafka/catalog observations. The backend/UI and internal agents are deployed, with the 4.3.0 follow-up validated on 6 November. Agents on 213, 229 and 191 preserve their configuration and identities. Broker execution is enabled and broker 102 was successfully published on 191. See [the deployment and readiness verdict](add-node-deployment-194.md) for the scope, evidence and production limits.

The Review page now supplies the broker plan and a release-gated submission path. Dedicated broker ownership, mutation, verification, publication and resume code is installed. Shared guards/callbacks were narrowly updated to retain unresolved reservations, preserve task references and isolate candidate results from the existing cluster lifecycle. The older [contract/design snapshot](add-node-broker-execution-contract.md) records the earlier planning boundary; the deployment report states the current implementation and remaining acceptance work.

## Reference alignment

### 6 November follow-up

The concurrent Phase 4 corrections introduce broker plan `add-broker/v2`: new brokers use `broker.properties`, and their ID is the highest existing **broker** ID plus one, with cluster-wide collision protection. Existing frozen v1 plans retain `server.properties` and their original receipts. Existing IDs, files and storage are not migrated. Cluster ID and exact Kafka version inheritance remain mandatory. The historical broker 102 pilot above predates this correction; it is not evidence of a new broker 3 deployment.

Phase 5 now offers a read-only controller plan selected from the observed quorum mode. Static expansion lists all source maintenance phases, including stopping brokers; dynamic expansion lists observer catch-up and admission without existing-service restart. See [the controller contract and execution-state design](add-node-phase5-controller-contract.md). The preview never schedules controller mutations and does not represent completed controller execution.

The HTML reference is `Tantor Add Node – Updated 3.html`. The additional Draw.io file is `Kafka_KRaft_Add_Host_Final_3_Sheets_All_in_one_final (1) (1).drawio`, containing ER, end-to-end and prerequisite sheets. Phase 1 check groups use its common check codes as labels for future observations, not claims of implemented check coverage.

Both references support the initial broker-only milestone, preserve the existing Kafka identity, keep readiness separate from inventory and treat reassignment as a separate operation. Static controller expansion and automatic partition redistribution are outside this first-release flow.

The Draw.io file proposes one new broker-only or controller-only process per fresh host and one service intent per operation; the agreed HTML reference additionally offers two separate processes on one fresh host. Phase 6 planning follows that explicit two-process choice: two role-only service identities owned by one parent operation. It does not add a second process to an occupied host. See [the Phase 6 contract and source-order differences](add-node-phase6-separate-process-contract.md). Its execution remains unavailable while the controller dependency and parent-operation safeguards are unfinished.

The ER table names describe logical responsibilities; reconcile them with existing Tantor job/task/service tables instead of creating duplicate operation infrastructure by name alone.
