# External agents and Add Service directories

Hosts is the internal host-agent inventory and onboarding entry point. External
discovery agents are managed through External Clusters. Discovery registration
does not create an internal host enrollment. Legacy discovery records are excluded
from the Hosts response, and direct internal onboarding requests for them return
409. Internal host records remain visible with their own heartbeat, version,
name and installation path; an external cluster's first agent must never replace
another host's agent details. Existing records and services are not deleted.

External Add Service no longer creates timestamp-based `instance-…` directory
defaults. The backend supplies the defaults displayed in the form:

| Service | Install | Logs | Working |
| --- | --- | --- | --- |
| Schema Registry | `/opt/tantor/schema-registry` | `/var/log/tantor/schema-registry` | `/var/lib/tantor/schema-registry` |
| Kafka Connect | `/opt/tantor/kafka-connect` | `/var/log/tantor/kafka-connect` | `/var/lib/tantor/kafka-connect` |

Schema config defaults to `etc/schema-registry` under the installation. Connect
config and plugins default to `config` and `plugins` under its installation.
If recorded service directories overlap these defaults on any eligible host,
the form suggests a common available numeric suffix (`-2`, `-3`, etc.). This
keeps one set of form paths valid across selected hosts. These are suggestions
based on persisted inventory; agent installation prechecks still validate the
actual filesystem. Custom paths remain supported. Existing installations do not
move. Ports must still be distinct for multiple instances on the same host.

## Validation after deploying the backend and UI

1. Hard refresh the browser. Open Hosts and its Agent Connectivity dialog. An
   external discovery agent must not appear as an internal enrollment candidate.
   An old internal host record may remain offline if its internal agent was stopped.
2. Check external agent status through External Clusters. Verify each node's own
   identity/status there. For an internal host on the same machine, Hosts must keep
   its own agent name, version, path and heartbeat, not those of a cluster peer.
3. Open an external cluster's three-dot menu, Add Service, and select Schema Registry
   or Kafka Connect. Check the default directories above. Close and reopen: unchanged
   inventory must produce the same paths, without a timestamp/random directory.
4. On a host with those paths already recorded for a service, check the numeric
   suffix suggestions. Configure a free REST port for an additional instance.
5. Enter custom paths and deploy a test instance. Check the job and the service's
   directory view for the submitted paths. Existing instances must keep their paths.

These changes are in the backend/UI only; no agent binary update is required.
