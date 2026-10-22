# Configuration navigation, deployment paths and log retention

Cluster **Configuration** and the cluster menu's **Configuration change** now use
`/clusters/:id/config-change`. The obsolete editor components are removed. Old
`/clusters/:id/config` bookmarks redirect to the same new page; backend configuration
read/history APIs remain because the new page uses them.

## New installation paths

New cluster deployments use `<selected mount>/tantor/kafka` as their active Kafka
installation. For example `/client-mount` becomes `/client-mount/tantor/kafka`, and
`/opt/tantor` stays `/opt/tantor/kafka`. Versioned parcels are placed beneath the
same Tantor base. Existing cluster paths are not migrated or rewritten.

## Agent logs

| Agent | Default active log | Archive directory |
| --- | --- | --- |
| Internal | `/var/log/tantor-agent/agent.log` | `/var/log/tantor-agent/archive` |
| External | `/var/log/tantor/discovery-agent/tantor-discovery-agent.log` | `/var/log/tantor/discovery-agent/archive` |

The internal agent follows `paths.log_dir` in its YAML. The external agent accepts
`TANTOR_AGENT_LOG_DIR` or `--log-dir`; its installer sets that environment variable.
Installers must provide write access to the active file and archive directory for
the runtime user. Read the active file with `tail -F`; startup errors still go to
stderr/systemd.

At the first maintenance tick after local midnight (or immediately on the first
new log write), the completed day's log is compressed to
`archive/<filename>.YYYY-MM-DD.<unique number>.gz`. Maintenance also runs at startup,
so a previous day's active file is archived after an agent restart. Rotation keeps
the active file inode, preserving systemd's inherited stderr descriptor.

Only matching regular archive files older than seven days are deleted. A persistent
hourly systemd timer also prunes expired archives while the agent is stopped:
`tantor-agent-log-retention.timer` or `tantor-discovery-agent-log-retention.timer`.
Root agents register their own timer at startup; the external installer also
installs a timer for a non-root monitoring agent. Manual pruning needs no backend
credentials:

```sh
/opt/tantor-agent/bin/tantor-agent --prune-logs --log-dir=/var/log/tantor-agent
/opt/tantor-external-agent/tantor-discovery-agent --prune-logs --log-dir=/var/log/tantor/discovery-agent
```

## Application logging

New Schema Registry and Connect deployments receive a single Log4j2 rolling file
appender, daily midnight gzip archives, seven-day retention, and an independent
hourly archive-pruning timer per service. Kafka 4+ broker/controller deployments
receive the same policy in their role-specific application log directory. Legacy
Kafka distributions retain their vendor logging configuration rather than receiving
incompatible Log4j2 JVM options. Existing service units/configurations are not
silently rewritten by an agent upgrade; applying this policy to existing services
requires a planned service configuration update/restart.

These rules do not rotate or delete Kafka partition data (`log.dirs`), metadata
storage, configuration backups, unrelated files, or the host's system journal.

## Basic Schema Registry deployment

Fresh basic deployments clear optional resource/init extensions, strip inherited
`metadata.encoder.secret` and `metadata.encoder.old.secret` properties, and unset
their environment equivalents. This prevents the optional metadata encoder and
DEK registry from creating `_schema_encoders` and `_dek_registry_keys`. The normal
schema store remains `_schemas` (or the explicitly selected schema topic).

Existing topics are not deleted. Existing encrypted services are not converted to
plaintext by an agent upgrade or by retrying a port update. Such a migration needs
an explicit review of schemas, encryption use and retained data.
