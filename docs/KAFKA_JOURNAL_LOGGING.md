# Kafka application logs in systemd (61)

The generated Kafka 4 Log4j2 policy previously referenced only a rolling file
appender. Kafka ran in the foreground, but application logging never reached its
stdout/stderr streams, so journalctl and systemctl status lacked those events.

Kafka now uses a console appender alongside the existing rotating application
file, and its generated systemd unit explicitly sets StandardOutput=journal and
StandardError=journal. Root logging stays at INFO. Daily gzip rotation and
seven-day file retention are unchanged. Schema Registry and Connect file policies
are unchanged. Kafka 3 keeps its distribution's original logging configuration.

After installation/restart of the updated Kafka service, check:

```bash
systemctl status broker.service --no-pager -l
journalctl -u broker.service -n 100 --no-pager
journalctl -u broker.service -f
```

Use the actual unit name for controller or combined-role services. Status shows
only recent journal lines; journalctl shows the retained journal history. Events
must occur after the updated logging policy is loaded. Journal storage/retention
is managed independently by systemd, not by the seven-day application-file rule.

Updating the agent binary alone does not rewrite or restart existing Kafka
services. Existing Tantor-managed Kafka services need their generated Log4j2
policy and unit updated, followed by daemon-reload and a controlled rolling
restart. Pre-existing externally managed Kafka logging is not rewritten by
discovery. No live Kafka configuration or service was changed during this fix.

Validation includes a real Java/Log4j2 runtime probe using Kafka's Log4j 2.24.3
jars: INFO/WARN/ERROR appear exactly once in both stdout and the application file;
DEBUG stays suppressed. It does not start Kafka or change any systemd service.
