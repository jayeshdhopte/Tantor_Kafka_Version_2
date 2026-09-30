package kafka

import (
	"context"
	"encoding/json"
	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStorageScopeIgnoresUnrelatedCluster(t *testing.T) {
	root := t.TempDir()
	meta := filepath.Join(root, "custom-metadata")
	data := filepath.Join(root, "custom-data")
	old := filepath.Join(root, "old-cluster")
	for _, dir := range []string{meta, data, old} {
		os.MkdirAll(dir, 0755)
		id := "new"
		if dir == old {
			id = "old"
		}
		os.WriteFile(filepath.Join(dir, "meta.properties"), []byte("cluster.id="+id+"\nnode.id=3\n"), 0600)
	}
	paths := kafkaRolePaths{MetadataLogDir: meta, LogDirs: data + "," + data}
	dirs := kafkaStorageDirs(paths)
	if !reflect.DeepEqual(dirs, []string{meta, data}) {
		t.Fatal(dirs)
	}
	if err := validateMetaProperties(context.Background(), nil, dirs, "new", "3", true); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(data, "meta.properties"), []byte("cluster.id=other\nnode.id=3\n"), 0600)
	if err := validateMetaProperties(context.Background(), nil, dirs, "new", "3", true); err == nil {
		t.Fatal("accepted conflicting configured storage")
	}
}
func TestMetadataRequiresIdentityAndReadableFile(t *testing.T) {
	root := t.TempDir()
	for _, content := range []string{"", "cluster.id=new\n", "cluster.id=new\nnode.id=8\n"} {
		os.WriteFile(filepath.Join(root, "meta.properties"), []byte(content), 0600)
		if err := validateMetaProperties(context.Background(), nil, []string{root}, "new", "3", true); err == nil {
			t.Fatal("accepted incomplete or wrong identity")
		}
	}
	os.Remove(filepath.Join(root, "meta.properties"))
	if err := validateMetaProperties(context.Background(), nil, []string{root}, "new", "3", true); err == nil {
		t.Fatal("accepted missing metadata")
	}
}
func TestServiceRoleControlsPathsAndPorts(t *testing.T) {
	for _, tc := range []struct {
		role               string
		broker, controller bool
	}{{"broker", true, false}, {"controller", false, true}, {"broker_controller", true, true}} {
		task := &api.Task{Parameters: map[string]string{"service_role": tc.role}}
		_, b, c := normalizeKRaftRole(kafkaRoleForTask(task))
		if b != tc.broker || c != tc.controller {
			t.Fatal(tc)
		}
		paths := resolveKafkaRolePaths(task, "/opt/custom", "/srv/custom-data")
		if tc.role == "controller" && paths.MetadataLogDir != "/srv/custom-data/controller-data/metadata" {
			t.Fatal(paths)
		}
	}
}
func TestLiveStorageScope(t *testing.T) {
	file := os.Getenv("TANTOR_TEST_LIVE_TASK")
	if file == "" {
		t.Skip("opt-in read-only deployment storage check")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var task api.Task
	if err = json.Unmarshal(data, &task); err != nil {
		t.Fatal(err)
	}
	paths := resolveKafkaRolePaths(&task, "/opt/kafka", task.Parameters["kafka_data_dir"])
	d := &Deployer{exec: executor.New(executor.Options{PrivilegeMode: "direct"})}
	dirs := kafkaStorageDirs(paths)
	if err = validateMetaProperties(context.Background(), d, dirs, task.Parameters["cluster_uuid"], task.Parameters["node_id"], true); err != nil {
		t.Fatal(err)
	}
	t.Logf("Node %s: valid storage %v", task.Parameters["node_id"], dirs)
}
