package schema

const SystemdTemplate = `[Unit]
Description=Confluent Schema Registry
Documentation=http://docs.confluent.io/
Requires=network.target
After=network.target

[Service]
Type=simple
User={{.User}}
Group={{.Group}}
Environment="JAVA_HOME={{.JavaHome}}"
Environment="SCHEMA_REGISTRY_HEAP_OPTS=-Xmx1G -Xms1G"
ExecStart={{.InstallDir}}/bin/schema-registry-start {{.InstallDir}}/etc/schema-registry/schema-registry.properties
ExecStop={{.InstallDir}}/bin/schema-registry-stop
Restart=on-failure
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
`

const SchemaRegistryPropertiesTemplate = `
# Schema Registry Config
listeners=http://0.0.0.0:8081
kafkastore.bootstrap.servers={{.BootstrapServers}}
kafkastore.topic=_schemas
debug=false
`
