package kafka

const SystemdTemplate = `[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=network.target
After=network.target

[Service]
Type=simple
User={{.User}}
Group={{.Group}}
Environment="JAVA_HOME={{.JavaHome}}"
Environment="KAFKA_HEAP_OPTS=-Xmx{{.HeapSize}} -Xms{{.HeapSize}}"
{{if .JmxAgentPath}}Environment="KAFKA_OPTS=-javaagent:{{.JmxAgentPath}}={{.JmxPort}}:{{.JmxConfigPath}}"{{end}}
{{if .AppLogDir}}Environment="LOG_DIR={{.AppLogDir}}"{{end}}
ExecStart={{.InstallDir}}/bin/kafka-server-start.sh {{.ConfigPath}}
ExecStop={{.InstallDir}}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=15
LimitNOFILE=1024000

[Install]
WantedBy=multi-user.target
`

const KafkaExporterSystemdTemplate = `[Unit]
Description=Kafka Exporter for Apache Kafka
After=network.target

[Service]
Type=simple
User={{.User}}
Group={{.Group}}
ExecStart={{.InstallDir}}/bin/kafka_exporter --web.listen-address=:{{.ExporterPort}} --kafka.server={{.Hostname}}:{{.KafkaPort}}
Restart=on-failure
RestartSec=15

[Install]
WantedBy=multi-user.target
`

const ZooKeeperSystemdTemplate = `[Unit]
Description=Apache ZooKeeper Server
Documentation=http://kafka.apache.org/documentation.html
Requires=network.target
After=network.target

[Service]
Type=simple
User={{.User}}
Group={{.Group}}
Environment="JAVA_HOME={{.JavaHome}}"
Environment="KAFKA_HEAP_OPTS=-Xmx{{.HeapSize}} -Xms{{.HeapSize}}"
ExecStart={{.InstallDir}}/bin/zookeeper-server-start.sh {{.ConfigPath}}
ExecStop={{.InstallDir}}/bin/zookeeper-server-stop.sh
Restart=on-failure
RestartSec=15
LimitNOFILE=1024000

[Install]
WantedBy=multi-user.target
`

const JmxConfigTemplate = `rules:
  - pattern: "kafka.server<type=(.+), name=(.+)><>(\\w+)"
    name: "kafka_server_$1_$2_$3"
  - pattern: "kafka.network<type=(.+), name=(.+)><>(\\w+)"
    name: "kafka_network_$1_$2_$3"
  - pattern: "kafka.controller<type=(.+), name=(.+)><>(\\w+)"
    name: "kafka_controller_$1_$2_$3"
  - pattern: "kafka.log<type=(.+), name=(.+)><>(\\w+)"
    name: "kafka_log_$1_$2_$3"
  - pattern: "java.lang<type=(.+)><>(\\w+)"
    name: "jvm_$1_$2"
`

const ServerPropertiesTemplate = `
# KRaft Node Config
process.roles={{.Role}}
node.id={{.NodeId}}
{{if eq .QuorumMode "dynamic"}}controller.quorum.bootstrap.servers={{.QuorumBootstrapServers}}
{{else}}controller.quorum.voters={{.QuorumVoters}}
{{end}}

# Listeners
listeners={{.Listeners}}
{{if .IsBroker}}inter.broker.listener.name=PLAINTEXT
{{end}}{{if .AdvertisedListeners}}advertised.listeners={{.AdvertisedListeners}}
{{end}}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL

# Kafka Data Directories
{{if .LogDirs}}log.dirs={{.LogDirs}}
{{end}}{{if .MetadataLogDir}}metadata.log.dir={{.MetadataLogDir}}
{{end}}
# Internal Topic Settings
num.partitions={{.NumPartitions}}
offsets.topic.replication.factor={{.RepFactor}}
transaction.state.log.replication.factor={{.RepFactor}}
default.replication.factor={{.RepFactor}}
min.insync.replicas={{.MinInsyncReplicas}}
transaction.state.log.min.isr={{.MinInsyncReplicas}}
`

const ZooKeeperBrokerPropertiesTemplate = `
# ZooKeeper-backed Kafka Broker Config
broker.id={{.NodeId}}

# Listeners
listeners=PLAINTEXT://{{.Hostname}}:{{.ListenerPort}}
advertised.listeners=PLAINTEXT://{{.Hostname}}:{{.ListenerPort}}

# ZooKeeper
zookeeper.connect={{.ZooKeeperConnect}}
zookeeper.connection.timeout.ms=18000

# Kafka Data Directories
{{if .LogDirs}}log.dirs={{.LogDirs}}
{{end}}{{if .MetadataLogDir}}metadata.log.dir={{.MetadataLogDir}}
{{end}}
# Internal Topic Settings
num.partitions={{.NumPartitions}}
offsets.topic.replication.factor={{.RepFactor}}
transaction.state.log.replication.factor={{.RepFactor}}
default.replication.factor={{.RepFactor}}
min.insync.replicas={{.MinInsyncReplicas}}
transaction.state.log.min.isr={{.MinInsyncReplicas}}
`

const ZooKeeperPropertiesTemplate = `
# ZooKeeper Config
tickTime=2000
initLimit=10
syncLimit=5
dataDir={{.DataDir}}
clientPort={{.ClientPort}}
maxClientCnxns=0
admin.enableServer=false
{{if .Servers}}{{.Servers}}{{end}}
`
