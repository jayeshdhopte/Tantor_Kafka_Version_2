# Schema deletion and recovery

The Schema Registry UI retains subject and version soft/permanent deletion and
adds recovery of an individual soft-deleted version. No Add new version action
is introduced by this change.

For a deleted subject, **View / recover** opens its versions. Each soft-deleted
version has **Recover** and **Permanently delete** actions. Recover opens a
confirmation preview of the original subject, version, ID, type and definition.
Recovering one version does not restore every deleted version of the subject.

The backend endpoint is:

`POST /api/v1/clusters/{clusterId}/data-services/schema-registry/subjects/{subject}/versions/{version}/recover`

It uses the selected connection/certificate settings and requires Schema Registry
change permission. The backend reads the original version with `deleted=true`,
checks that it is not already active, and registers its unchanged schema,
schemaType, references and any supplied metadata/ruleSet. It does not submit the
old version/ID, change compatibility, change registry modes, recover dependencies
automatically, or modify Kafka records. The registry remains responsible for
compatibility, reference and mode validation.

After registration, the backend looks up the exact schema to obtain its actual
active version and ID. No version increment is invented. If this read-back fails,
the UI reports that registration succeeded but verification is pending; it does
not falsely report the completed mutation as failed. An unchanged soft-deleted
schema normally reuses its retained ID in the same registry. The resulting version
is determined by the registry and its version history.

Recover is unavailable for hard-deleted versions. This flow does not implement
backup import or forced restoration of IDs. Permanent deletion retains its
additional permission check and typed subject/version confirmation.

`SCHEMA_REGISTRY_VERSION_RECOVER` audit events contain the actor through the normal
audit context, cluster, selected connection, source version/ID, resulting version/ID
when verified, and success/failure. Schema bodies, metadata contents, references,
credentials and raw upstream error payloads are not copied into audits.

## Validation

Use a disposable test subject on the intended Schema Registry connection:

1. Soft-delete one existing version. Other active versions must remain active.
2. Enable **Show soft-deleted versions**. Confirm its status and Recover button.
3. Open Recover and cancel. No registration should occur.
4. Open Recover and confirm. Check the resulting version/ID message and refreshed
   version list against the registry. Do not assume a fixed version increment.
5. For a fully soft-deleted subject, use **Show only soft-deleted**, then
   **View / recover**, and recover one selected version. Other deleted versions
   must stay deleted.
6. Check success/failure recovery events in Audit History and its operation filter.
7. On a disposable soft-deleted version, confirm permanent deletion requires typing
   the exact subject/version and that it no longer has a Recover action afterward.

Backend tests cover changed and unchanged resulting versions, references/metadata
preservation, audit sanitization, complete-subject soft deletion, active and missing
versions, permission checks, registry rejection and post-registration lookup failure.
No agents or live schemas are modified by these tests.
