# External service deployment

This Discovery agent is `3.3.0-dataservices.1`. It discovers external Kafka
clusters as before and can additionally install Schema Registry and Kafka
Connect when the operator explicitly enables it. The agent does not expose
Kafka installation, Kafka configuration, or Kafka service lifecycle commands.

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

Only PLAINTEXT Kafka service deployment is currently supported. Existing Kafka
security settings, configuration, services and ownership are not changed. The
management server creates or validates the required compacted internal topics;
incompatible existing topics are reported and not modified.
