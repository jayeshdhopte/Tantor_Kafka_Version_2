# Schema/Connect follow-up validation — 22 October 2026

## 44: Automatically created consumer groups

Schema Registry's `tantor-sr...` group and distributed Connect's `tantor-connect...`
group are required coordination groups. Deployment does not start an ordinary
console consumer. Do not disable these coordination groups.

Read-only investigation found `discovery-audit-consumer-group` stable with one
`console-consumer` member on `192.168.3.194`, assigned `test-topic:0`. Its process
ran from `/root/kafka_client_discovery`, in a root login-session scope with parent
PID 1. The group name does not occur in the application or agent sources. This is
a separately running console client, rather than the deployed Tantor agent.
It has not been stopped, and its committed offsets have not been deleted.

## 46: Separate minimum and maximum heaps

The current UI requests, internal payload builder, external service configuration
and shared agent service template preserve separate `heap_xms` and `heap_xmx`.
Existing tests verify unequal values such as `512M` and `2G`, plus rejection when
the minimum exceeds the maximum. Updating application code does not rewrite a
previously created systemd unit. For runtime validation after a fresh deployment,
inspect only the `HEAP_OPTS` environment entry and the Java process's `-Xms`/`-Xmx`.

## 47: Configuration review / min ISR

The live preview returned HTTP 400:
`Broker-level min.insync.replicas cannot be altered while ELR is enabled.`
No configuration was applied during this probe. Per user preference, retain ELR
and use **Topics → select topic → Settings → Edit settings → Minimum in-sync
replicas**. This sets a topic override, not the default for every existing/future
topic in the cluster.

Configuration review now scrolls to either its result or validation error, shows
an in-progress status, and rejects missing new values with an explicit message.
An ELR rejection includes guidance and an Open topics button. Dynamic and static
previews remain separate, so failure in one form does not submit the other form.

## 57: Audit operations and filters

Schema registration, global/subject compatibility and mode updates, compatibility
checks, and connector create/config/pause/resume/stop/restart/task-restart operations
now record their success or upstream failure. Deletion auditing retains soft/hard
mode and version details without duplicate deletion records. The audit service
captures the authenticated actor and timestamp. Connector configuration bodies,
schema bodies, bearer tokens and upstream error messages are not copied into
audit details.

Audit UI adds Operation and Resource type filters derived from returned events,
keeps job/request events visible, and treats the To date as an inclusive local day.
Schema registration is no longer mislabeled as agent registration.

Validate by performing an operation on a test schema or connector, opening Audit,
selecting its operation/resource/status, and checking user, time, resource and
result. The actor and other existing filters can be combined with the new ones.

## 59: External systemd names

The first fresh service of each kind on a host uses `schema-registry.service` or
`kafka-connect.service`. Only additional same-kind instances on that same host use
numeric suffixes (`-2`, `-3`, ...), because two processes cannot share one systemd
unit. Different hosts can each use the standard name. Existing UUID-suffixed units
are preserved; they are not renamed while running or silently orphaned.

Validation uses isolated backend builds and local UI builds; this change set has
not been deployed or used to restart existing services.
