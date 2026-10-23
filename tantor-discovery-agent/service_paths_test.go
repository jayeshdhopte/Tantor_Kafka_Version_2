package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExistingSchemaServicePathsAreReportedWithoutDeploymentJournal(t *testing.T) {
	props := map[string]string{"listeners": "http://0.0.0.0:8081", "kafkastore.bootstrap.servers": "PLAINTEXT://192.168.3.229:9092", "password": "secret-not-reportable"}
	path, ok := dataServicePath("schema-registry.service", "{ path=/opt/tantor/schema-registry/bin/schema-registry-start ; argv[]=ignored }", "/var/lib/tantor/schema-registry",
		[]string{"java", "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain", "/opt/tantor/schema-registry/etc/schema-registry/schema-registry.properties"},
		[]string{"LOG_DIR=/var/log/tantor/schema-registry", "SECRET=secret-not-reportable"}, props, "192.168.3.229:9092")
	if !ok || path.RestPort != 8081 || path.InstallDir != "/opt/tantor/schema-registry" || path.LogDir != "/var/log/tantor/schema-registry" || path.ServiceType != "SCHEMA_REGISTRY" {
		t.Fatalf("bad paths: %+v", path)
	}
	encoded, _ := json.Marshal(path)
	if strings.Contains(string(encoded), "secret-not-reportable") || strings.Contains(string(encoded), "bootstrap") {
		t.Fatal("sensitive configuration leaked")
	}
	_, ok = dataServicePath("schema-registry.service", "", "/", []string{"io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain", "/config"}, nil, props, "other-broker:9092")
	if ok {
		t.Fatal("reported service from another Kafka cluster")
	}
}

func TestConnectPathsUseActualExecutableAndRelativeConfigWorkingDirectory(t *testing.T) {
	path, ok := dataServicePath("kafka-connect-2.service", "{ path=/client/tantor/kafka/bin/connect-distributed.sh ; }", "/srv/connect",
		[]string{"java", "org.apache.kafka.connect.cli.ConnectDistributed", "configs/worker.properties"}, []string{"KAFKA_LOG_DIR=/logs/connect"},
		map[string]string{"bootstrap.servers": "a:9092,b:9092", "rest.port": "8088", "plugin.path": "plugins,/client/plugins"}, "b:9092")
	if !ok || path.ConfigFile != "/srv/connect/configs/worker.properties" || path.PluginDir != "/srv/connect/plugins,/client/plugins" || path.InstallDir != "/client/tantor/kafka" || path.RestPort != 8088 {
		t.Fatalf("bad paths: %+v", path)
	}
}
