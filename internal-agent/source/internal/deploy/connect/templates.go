package connect

const SystemdTemplate = `[Unit]
Description=Apache Kafka Connect
Documentation=http://kafka.apache.org/documentation.html
Requires=network.target
After=network.target

[Service]
Type=simple
User={{.User}}
Group={{.Group}}
Environment="JAVA_HOME={{.JavaHome}}"
Environment="KAFKA_HEAP_OPTS=-Xmx1G -Xms1G"
ExecStart={{.InstallDir}}/bin/connect-distributed.sh {{.InstallDir}}/config/connect-distributed.properties
ExecStop={{.InstallDir}}/bin/kafka-server-stop.sh
Restart=on-failure
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
`

const DistributedPropertiesTemplate = `
# Kafka Connect Distributed Config
bootstrap.servers={{.BootstrapServers}}
group.id={{.GroupId}}

# Converters
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true

# Internal Topics
offset.storage.topic=connect-offsets
offset.storage.replication.factor=3
config.storage.topic=connect-configs
config.storage.replication.factor=3
status.storage.topic=connect-status
status.storage.replication.factor=3

# Plugin Path
plugin.path={{.InstallDir}}/plugins
`
