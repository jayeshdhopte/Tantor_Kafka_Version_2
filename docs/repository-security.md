# Repository credential and history security

Production database credentials are supplied only through the Podman secret
files `TANTOR_DB_USER` and `TANTOR_DB_PASSWORD`. Development values belong in
the ignored local `.env`; examples must leave credential values empty.

Treat `EXPOSED_DATABASE_CREDENTIAL` as compromised. A database administrator
must rotate it everywhere it was accepted, review authentication logs, and
check for reuse in SIT/UAT, production, developer databases, CI variables and
external secret stores. Repository changes cannot perform or verify that live
rotation.

The full-history scan also identified two distinct plaintext database passwords
in deleted developer helpers (`TestDb.java` and `tantor-server/InjectCluster.java`).
Treat those values as compromised too: identify their accounts from the
protected incident record, rotate or revoke them wherever they were accepted,
and review their authentication logs. Never paste the values into an issue,
commit message, CI log, or rotation ticket.

Install Gitleaks 8.29.1 and pre-commit, then enable the checked-in hook:

```bash
pre-commit install
gitleaks dir --config .gitleaks.toml --redact .
gitleaks git --config .gitleaks.toml --redact .
```

CI performs both current-tree and complete reachable-history scans using a
digest-pinned official container. Recovery scripts, model/transcript dumps,
lint JSON dumps, local environment files, private-key material and temporary
patch/backup files are prohibited in source control and release artifacts.

## Coordinated history cleanup

History cleanup affects shared ancestors of multiple branches. A repository
administrator must schedule a freeze, require all collaborators to discard old
clones, and use a fresh mirror clone. In that mirror, provide the compromised
value through a mode-0600 file and run `scripts/rewrite-c08-history.sh`. Review
the saved ref map, run the full Gitleaks history scan, and only then perform a
coordinated `git push --force --mirror origin`. Protect the recovery bundle
because it still contains the compromised history; destroy it after the agreed
rollback window. Revoke old CI caches and release/source archives as well.
