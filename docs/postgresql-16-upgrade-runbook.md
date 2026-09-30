# PostgreSQL 13.23 to 16.14 Upgrade and Rollback Runbook

## Purpose and scope

This runbook covers H-02 upgrade safety for the Tantor database. It uses a
logical dump and restore into a new PostgreSQL 16.14 database. It does not
authorize an in-place upgrade, reuse of a PostgreSQL 13 data directory, or any
operation against an unapproved shared database.

The approved target image is:

```text
docker.io/library/postgres:16.14@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b
```

The digest is the OCI index digest resolved from the official image registry.
The Linux/AMD64 child manifest observed during approval was
`sha256:670391653713782e51974845b217c56fed4dd8729142299c43c919a8d3e15e00`.
The release process must pull by the approved index digest, scan the image,
generate its SBOM, and record the resolved platform image in the release
manifest.

## Immutable migration baseline

The current application has 74 versioned SQL migration files. The highest
version is V76 and V48_1 is present. Numeric labels V18, V38, and V68 are not
part of the current branch. A numeric gap is not permission to create a file at
that version.

During this upgrade:

- do not edit or add a migration;
- do not run Flyway repair;
- do not baseline an existing database;
- do not ignore missing migrations or checksums;
- keep `validate-on-migrate=true`, `ignore-missing-migrations=false`,
  `baseline-on-migrate=false`, and `clean-disabled=true`.

Record the current source commit and migration-directory Git tree before every
test or cutover. The approved planning baseline was:

```text
HEAD: a4052d437e56d42465d93db8af464fffadaa636b
migration tree: e9a3b545eb3e181874680ee832e2dd59775b788a
```

If either value changes, review the change and rerun the full compatibility
suite before continuing.

## Required evidence before production approval

1. The fresh PostgreSQL 16.14 compatibility job passes.
2. The disposable PostgreSQL 13.23 to 16.14 dump/restore job passes.
3. A restore test using approved production-like data passes in an isolated
   environment.
4. The exact production image digest has passed vulnerability review and its
   SBOM is retained.
5. Source database version, encoding, locale/collation, size, extensions,
   roles, ownership, and `flyway_schema_history` have been captured with
   read-only queries.
6. Disk capacity is sufficient for the source volume, target volume, dump,
   logs, and rollback reserve at the same time.
7. The maintenance window, write freeze, operators, validation owner, and
   rollback decision point are approved.

## Disposable rehearsal

Run these only in a Docker-capable CI runner or approved disposable test host:

```bash
bash scripts/database/test-postgresql-16-compatibility.sh
bash scripts/database/test-postgresql-13-to-16-restore.sh
```

Both scripts generate test-only credentials at runtime, create uniquely named
containers and networks, and remove only those disposable resources. They do
not accept a production connection string.

The fresh test must demonstrate:

- all 74 migrations apply to an empty PostgreSQL 16.14 database;
- `pgcrypto` is installed;
- a second server startup validates the completed Flyway history;
- Tantor server and artifact repository both become ready;
- HTTP smoke checks pass.

The restore test must demonstrate:

- a PostgreSQL 16.14 `pg_dump` client can dump a disposable 13.23 source;
- the custom-format archive can be listed and checksummed;
- `pg_restore` succeeds into a new empty 16.14 database;
- schema object counts, test data, functions, triggers, extension, and Flyway
  history match;
- both applications start against the restored target.

## Pre-upgrade capture and backup

Use approved secret injection. Never place passwords in commands, shell
history, logs, archives, or this runbook.

1. Confirm the source is the expected PostgreSQL 13 release.
2. Capture database encoding, locale/collation, installed extensions, database
   and schema ownership, roles/grants, database size, table counts, and current
   Flyway history.
3. Confirm the source Flyway history validates before taking the final dump.
4. Create a custom-format logical dump using PostgreSQL 16.14 client tools.
5. Capture required globals/role definitions through the approved privileged
   procedure. Do not blindly replay source superuser attributes on the target.
6. Save dump stderr separately and fail on warnings or nonzero status.
7. Run `pg_restore --list` against the archive.
8. Calculate and retain a SHA-256 checksum.
9. Copy the archive and evidence to approved protected storage.
10. Restore it into a disposable target and complete validation before the
    production maintenance window.

A named container volume is persistence, not a backup. A dump is not accepted
as useful until its restore succeeds.

## Production cutover

1. Announce the maintenance window and block new user/agent write activity.
2. Stop both database-writing applications and confirm active application
   sessions have drained.
3. Take the final verified dump and preserve its checksum and logs.
4. Leave the PostgreSQL 13 container and volume stopped and untouched.
5. Create a new PostgreSQL 16.14 container with a new empty volume and approved
   secret files. Never attach the PostgreSQL 13 data volume to PostgreSQL 16.
6. Restore into the new empty `tantor` database with ownership/grants mapped to
   the approved application role.
7. Verify `pgcrypto`, schemas, tables, sequences, indexes, constraints,
   functions, triggers, row counts, and selected data checksums.
8. Start Tantor server first. Strict Flyway validation must pass without repair,
   baseline, ignore, or checksum changes.
9. Confirm all 74 history rows are successful and the highest is V76.
10. Start the artifact repository using the same explicit database credentials.
11. Run readiness and functional smoke checks, including access to existing
    artifact metadata and representative encrypted application values.
12. Review database and application logs, connection pools, errors, locks,
    latency, disk use, and resource utilization.
13. Re-enable writes only after the validation owner approves cutover.

## Rollback

Before writes are enabled on PostgreSQL 16.14, rollback is:

1. Stop the target applications.
2. Preserve target logs and validation evidence.
3. Stop the PostgreSQL 16 target.
4. Reconnect applications to the untouched PostgreSQL 13 source.
5. Start applications and complete rollback smoke checks.

After writes are enabled on PostgreSQL 16.14, the old PostgreSQL 13 database is
no longer current. Do not switch back silently. The incident owner must choose
an approved restore/reconciliation path that preserves or explicitly accounts
for post-cutover writes.

PostgreSQL data directories are not backward compatible. Never attach a
PostgreSQL 16 volume to PostgreSQL 13 or attempt to restore a PostgreSQL 16
physical data directory into PostgreSQL 13.

## Credential handling

`POSTGRES_USER` and `POSTGRES_PASSWORD` initialize an empty official PostgreSQL
container; changing a secret file does not automatically change an existing
database role password. Existing-role rotation requires an authorized database
operation plus coordinated restart of Tantor server and artifact repository.

Keep `TANTOR_ENCRYPTION_KEY` unchanged during the database upgrade. Changing it
can make existing encrypted application values unreadable.

## PITR and disaster recovery dependency

This H-02 implementation proves logical dump/restore upgrade safety only. It
does not implement continuous WAL archiving, physical base backups, retention,
off-host storage, or point-in-time recovery.

An approved PITR/DR design, documented RPO/RTO, monitoring, retention, encrypted
backup storage, and recurring restore exercises remain separate production-
readiness dependencies. They must be completed before any release gate that
requires PITR or disaster recovery can be marked satisfied.
