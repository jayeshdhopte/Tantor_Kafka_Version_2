# Add Node deployment and readiness verdict

Updated: 6 November 2026 (Asia/Calcutta). Branch: `version2_db_mapping`.

## Broker-only Phase 4 verdict

The Kafka 4.3.0 broker pilot completed successfully through the authenticated Tantor APIs on 194. Broker 102 was added on 192.168.3.191 to the existing managed `test` cluster. A real recovery/resume completed the same operation without another prepare, format or start action. All six job steps succeeded and the reservation was released.

This accepts the broker-only pilot on this lab configuration. It does not certify every production configuration, secured Kafka, dynamic controller addition or separate-process addition. Those remain separate release scopes.

## Release artifacts and runtime

- Platform: `/opt/Tantor_kafka_VERSION-2_049b9d7` on 192.168.3.194.
- Public UI: `https://192.168.3.194/`; served asset `/assets/index-Cn9DilmF.js` was verified against installed bytes.
- Backend release SHA-256: `77464a0d9afec42f2b44734edf1f2468b6a8ea8f595851984ad254bb652debf8`.
- Migration V105 is applied. It adds broker-operation ownership, history and reservation guards.
- Agent release: `1.0.0-prod.17-dataservices.4-configchange.10-rollingrestart.1-addnode.4` on 213, 229 and 191. Each replacement was checksum checked, configuration preserving and verified through a fresh backend heartbeat.
- Agent amd64 SHA-256: `aefe79712d2fe8b0ddadc00d0e6898dd35b04d2b69cd913687be71e8b8a37df1`.
- Agent arm64 SHA-256: `0906da91239882e7e8f6050a5ffc34e9f1c983da3307481c19c4faa17903b71e`.
- Execution is enabled through `/etc/systemd/system/tantor-server.service.d/40-add-node-hold.conf`. Authentication, passing readiness, reservations, ownership and final verification remain mandatory. The application default remains execution disabled.
- The existing UI runtime configuration, backend environment, agent identities and agent configuration files were preserved. Existing services were not restarted by the platform/agent rollout.

Each platform replacement takes a fresh JAR/UI/environment backup and PostgreSQL 16 custom-format dump under `/opt/tantor-backups/add-node-*`. The latest exact backup location is recorded in `/var/tmp/tantor-add-node-43-20261106/deployment.json`. An earlier pre-pilot backup is `/opt/tantor-backups/add-node-20261105T195836Z`.

## Actual Kafka 4.3.0 acceptance

| Observation | Verified result |
|---|---|
| Managed cluster | `7e8ba09c-3750-4391-b985-5c433f8fa821` (`test`) |
| Kafka cluster identity | `h_dBSGeCSnCAO1_kE6H9gA`, preserved |
| Existing broker | ID 1, 192.168.3.229:9092 |
| New broker | ID 102, 192.168.3.191:9092, unfenced, rack `rack1` |
| Controller voter | ID 101, unchanged static voter identity and leader |
| Operation | `8c106882-c213-48e2-b56c-a2fca4842b63`, `SUCCEEDED` |
| Job | `ac1e4c51-863e-47c8-939a-6401a67092a7`, `SUCCESS`, retry count 1 |
| Prepare / format / start | Exactly one action of each; completed steps retained during resume |
| Storage | `/opt/tantor-broker-storage/data`; Kafka user; original metadata SHA-256 and mtime preserved across resume |
| Application logs | `/opt/tantor-broker-storage/logs` |
| Registration and publication | Exact new endpoint/rack, unchanged voters, three spaced healthy observations, fresh native inspections, one published broker assignment |
| Real data path | Temporary topic assigned exclusively to broker 102; acknowledged produce and exact consume passed; owned temporary topic deletion confirmed |

The initial verification ran before the candidate was unfenced. It preserved the reservation and returned `RECOVERY_REQUIRED`; it did not publish membership prematurely. After live registration was confirmed, the permission-checked resume endpoint reconciled the operation-owned receipt and completed verification/publication. Storage metadata was not recreated and the Kafka process PID remained unchanged across resume.

The final release adds a bounded registration wait (120 seconds by default) for this ordinary startup delay. Foreign brokers, missing existing brokers, identity/rack/voter mismatches and other verification failures still fail closed. Unit tests cover transient registration, deadline expiry and the distinction from unexpected membership. This small follow-up was tested after the real pilot; no second fresh-host broker addition was performed for it.

## Version, package and host preparation

Kafka admission supports exact versions 4.1.0, 4.1.1, 4.1.2 and 4.3.0. Unsupported versions remain blocked. The earlier 4.3.0 restriction was Tantor's backend admission policy, not a restriction in the supplied Ansible scripts.

The actual approved 4.3.0 artifact was SHA-256 checked:

`71a4bba687a5623a79172c8ec688fcd45497a197117bbe8fb0ef2254d83d6380`

Static and dynamic broker storage-format flags were tested in isolated directories with this package. Both preserved the supplied Kafka identity, created distinct directory identities and rejected an implicit repeat format. Production's Kafka client 4.1.2 successfully queried the real 4.3.0 cluster, including features, unfenced registration, quorum, broker configs and log directories. The live broker-add pilot used the current static quorum; dynamic broker addition has package/adapter coverage rather than a second live cluster pilot.

Node 191's `/data` mount contained Elasticsearch data and had only about 6.7 GiB free. It was left intact. The candidate uses the separate persistent `/opt` filesystem, which had about 36 GiB available. The 20 GiB admission default was retained. Only newly created candidate directories were assigned to the Kafka user. A backup was taken before granting that user traverse-only ACL access through `/srv/apps` to the approved parcel.

`50-add-node-candidate-profile.conf` configures candidate-only runtime user, Java home, data paths and log path. It does not rewrite existing service runtimes. Broker heap inherits the per-broker service profile when the cluster-level heap is absent; conflicting broker heaps require an explicit profile. Candidate rack is explicit, bound to preview freshness and the frozen plan, and checked against live registration. It is not copied silently from another broker.

The approved catalog-bound parcel cache takes precedence over a legacy same-version install cache. Readiness checks runtime-user read/execute access after validating the complete package. Formatting uses operation-owned log paths and Log4j configuration, including when Kafka runs as a non-root user.

## Regression verdict and limits

**No new regression was detected in the checks performed.** Existing Java PIDs and service states on 213, 229 and 191 were preserved through agent upgrades; the new broker added its own process on 191. Some legacy unit names were already failed before this rollout and were not treated as newly introduced failures.

Validation evidence includes:

- Backend report set: 222 selected tests, 194 passed, zero failures/errors, 28 opt-in legacy configuration/restart database tests skipped in this validation round. The two new broker migration/repository tests ran against a dedicated temporary loopback PostgreSQL instance and passed. Prior deployment acceptance also retains its baseline comparison evidence.
- Production UI build, changed-file lint and 18 browser fixture scenarios passed. Fixtures include polling/freshness, rack context invalidation, review gating, uncertain submission/idempotency and operation-specific resume. They do not stand in for live authentication or Kafka verification.
- Final readiness and broker mutation/recovery Linux test binaries passed directly on 194, including partial/unknown formatting, lost result reconciliation, four-three version preservation, non-root directory ownership and package traversal checks. A Windows readiness-test invocation was blocked by Application Control; Linux target execution was used and passed.
- Legacy Linux deployment, Kafka and Schema/Connect regression binaries passed. Optional live/artifact cases retained their explicit skips. No working Kafka process was restarted to run these suites.
- Authenticated real broker addition, safe failure/resume, atomic publication, API compatibility and a produce/consume test on the candidate passed.

These checks support preserving rolling restart, configuration change and multi-instance Schema Registry/Connect behavior. They are not a claim that every disruptive legacy operation was repeated against every live instance. The first adapter remains broker-only and PLAINTEXT; new Node Exporter installation, controller addition, separate-process addition and broader production acceptance remain later work.

## Evidence and recovery

Evidence is retained under `/var/tmp/tantor-add-node-43-20261106/` on 194: package compatibility, Linux logs, job progress, accepted operation, acceptance report, produce/consume result, deployment manifest and verification reports. Earlier baseline comparison evidence remains in `/var/tmp/tantor-add-node-20261105/`.

A completed broker is now real cluster membership. Do not restore a pre-addition database or delete its Kafka storage as a platform rollback. To revert the application, use the chosen JAR/UI backup while preserving the additive V105 history and published membership. Agent rollback restores its recorded previous executable and restarts only `tantor-agent`, preserving its configuration. Any unresolved future operation must retain its storage/receipt and use the dedicated reconciliation/resume path; generic cleanup/reformat is not a recovery procedure.
