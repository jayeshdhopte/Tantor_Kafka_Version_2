package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCleanupDoesNotRequireDeletedArtifactAndProtectsKafka(t *testing.T) {
	root := t.TempDir()
	props := filepath.Join(root, "broker.properties")
	if err := os.WriteFile(props, []byte("log.dirs=/data/preexisting-kafka\nmetadata.log.dir=/data/metadata\n"), 0600); err != nil {
		t.Fatal(err)
	}
	task := serviceTask{Command: "CLEAN", Kind: "schema-registry", Parameters: map[string]string{"kafka_cluster_id": "original-kafka-id", "install_dir": "/opt/tantor/schema-registry"}}
	cluster := DiscoveredCluster{KafkaClusterID: "original-kafka-id", IsRunning: true, InstallPath: "/opt/kafka", PropsFile: props}
	prepared, err := prepareServiceTask(task, []DiscoveredCluster{cluster})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"/opt/kafka", "/data/preexisting-kafka", "/data/metadata"} {
		if !strings.Contains(prepared.Parameters["protected_paths"], expected) {
			t.Fatal("Kafka protection missing", prepared.Parameters)
		}
	}
	if _, err := prepareServiceTask(task, nil); err == nil {
		t.Fatal("cleanup allowed without local Kafka identity validation")
	}
	task.Command = "INSTALL"
	if _, err := prepareServiceTask(task, []DiscoveredCluster{cluster}); err == nil {
		t.Fatal("install allowed without artifact")
	}
	task.Command = "DELETE_KAFKA"
	if _, err := prepareServiceTask(task, []DiscoveredCluster{cluster}); err == nil {
		t.Fatal("external service command can delete Kafka")
	}
}
