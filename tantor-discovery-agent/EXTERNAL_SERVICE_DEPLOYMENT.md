# External service deployment

This Discovery agent is `3.4.0-dataservices.2-configchange.8-rollingrestart.4`.
It retains discovery and monitoring and adds managed Kafka configuration,
rolling restart, and multiple Schema Registry / Connect instances when enabled.
See [EXTERNAL_MANAGEMENT.md](EXTERNAL_MANAGEMENT.md) for setup and limitations.

Enable this capability only on a selected node of a non-production external
cluster. The service runs as root because systemd unit creation and installation
require it:

```bash
sudo ./install-agent.sh --server-url http://MANAGEMENT_HOST:PORT \
  --node-name "$(hostname -f)" --environment test \
  --scan-paths /opt/kafka --run-user root --run-group root \
  --enable-tasks --enable-service-deployment
```

The agent reports its capability to Tantor. In Clusters, open the external
cluster's three-dot menu and select **Add services to this cluster**. Choose a
capable agent as the target, select the artifact and directories, then use
**Check and deploy**. No fields are added to the external-cluster connection
form.

Before deployment, the agent requires Java 17, active NTP/chronyd, free REST
ports, Kafka reachability and safe non-overlapping directories. Connect also
requires readable connector JARs and at least 5120 MiB free working storage.
The agent reads the local Kafka configuration and rejects service paths that
overlap the discovered Kafka install, data, log, metadata, or another service.

Only PLAINTEXT Kafka service deployment is currently supported. Deploying an
additional service preserves existing Kafka security and ownership. The
management server creates or validates the required compacted internal topics;
incompatible existing topics are reported and not modified.
