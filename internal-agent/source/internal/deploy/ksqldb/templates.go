package ksqldb

const SystemdTemplate = `[Unit]
Description=ksqlDB Server
Documentation=https://ksqldb.io/
Requires=network.target
After=network.target

[Service]
Type=simple
User={{.User}}
Group={{.Group}}
Environment="JAVA_HOME={{.JavaHome}}"
Environment="KSQL_HEAP_OPTS=-Xmx2G -Xms2G"
ExecStart={{.InstallDir}}/bin/ksql-server-start {{.InstallDir}}/etc/ksqldb/ksql-server.properties
ExecStop={{.InstallDir}}/bin/ksql-server-stop
Restart=on-failure
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
`

const KsqlServerPropertiesTemplate = `
# ksqlDB Server Config
bootstrap.servers={{.BootstrapServers}}
ksql.schema.registry.url={{.SchemaRegistryUrl}}

listeners=http://0.0.0.0:8088
ksql.logging.processing.stream.auto.create=true
ksql.logging.processing.topic.auto.create=true
`
