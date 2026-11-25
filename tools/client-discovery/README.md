# Kafka client IP discovery

Tantor shows consumer IPs from live Kafka consumer-group membership. The producer collector is adapted from the supplied Ansible workflow. It runs on an authorized Ansible controller and reads existing broker request logs. Both internal and external clusters use the same snapshot format.

## Configure the collector

Use a system with Ansible, Python 3.9 or newer on the controller and brokers, and a Kafka CLI on the admin host. Create an inventory for each cluster with all brokers in `kafka_brokers` and one Kafka CLI host in `kafka_admin`. Use your normal SSH and Kafka authentication.

Set these variables for the run. Store Kafka credentials separately in the optional Admin client properties file.

```yaml
tantor_cluster_id: "00000000-0000-0000-0000-000000000000" # UUID from the Tantor cluster URL
kafka_cluster_id: "replace-with-the-live-kafka-cluster-id"
kafka_bin: /opt/tantor/kafka/bin
kafka_bootstrap_servers: kafka.example.internal:9092
kafka_client_properties: "" # Optional path on the admin host
kafka_metadata_properties: /data/kafka/broker-data/meta.properties
kafka_request_log: /var/log/kafka/kafka-request.log
kafka_log_timezone: Asia/Kolkata
discovery_snapshot_dir: /var/lib/tantor/client-discovery
discovery_lookback_minutes: 15
```

Override the metadata path, request log path, and timezone per broker if they differ. Use a dedicated request log with Kafka’s `kafka.request.logger` events and timestamps at the start of each line. Kafka 4.3 JSON completed-request records and the supplied text request format are supported. The collector reads existing logs and leaves logger levels and Kafka services alone. An empty, missing, or unreadable log yields incomplete coverage. Run the collector as an account with read access, or set `discovery_become: true` where your inventory authorizes it.

Create the snapshot directory on a trusted volume that the collector can write and the Tantor server can read. Give it a shared group and setgid so the 0640 snapshots inherit the correct group. Keep it outside web-served directories. Set the backend environment variable `TANTOR_CLIENT_DISCOVERY_SNAPSHOT_DIR` to its read path. If the controller and backend are separate, mount the same volume on both. Schedule a single run per cluster at regular intervals. The backend labels snapshots stale after 30 minutes by default. Set `TANTOR_CLIENT_DISCOVERY_MAX_AGE_SECONDS` to adjust that interval.

```sh
ansible-playbook -i inventory.ini tools/client-discovery/discover-producers.yml -e @client-discovery-vars.yml
```

Check **Topic Details → Producers** for the scan time and broker coverage. With proxies or NAT, the client IP is the address observed by Kafka. The collector includes only topic-linked Produce requests. It neither classifies sockets as producers nor imports the example data from the original ZIP. `Last seen` is the request log time. The supplied consumer script remains usable independently, while Tantor uses the current Admin API membership and assignments for consumer IPs.
