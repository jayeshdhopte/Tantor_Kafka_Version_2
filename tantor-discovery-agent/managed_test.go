package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func managedFixture(t *testing.T) (DiscoveredCluster, RuntimeConfig, managedTask) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("managed mutation requires root")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.properties")
	if err := os.WriteFile(path, []byte("node.id=7\ncluster.id=cluster-1\nlog.retention.hours=48\nnum.io.threads=8\n"), 0640); err != nil {
		t.Fatal(err)
	}
	previous := managedBackupRegister
	managedBackupRegister = func(context.Context, string) error { return nil }
	t.Cleanup(func() { managedBackupRegister = previous })
	return DiscoveredCluster{NodeID: 7, KafkaClusterID: "cluster-1", PropsFile: path, SystemdService: "custom-broker.service"}, RuntimeConfig{AllowConfigWrite: true}, managedTask{Command: "PATCH_KAFKA_CONFIG", Parameters: map[string]string{"node_id": "7", "kafka_cluster_id": "cluster-1", "config_path": path, "service_name": "custom-broker.service", "config_changes_json": `{"log.retention.hours":null,"num.io.threads":"12"}`, "expected_values_json": `{"log.retention.hours":"48","num.io.threads":"8"}`}}
}
func TestManagedPatchBacksUpBeforeAtomicDeletion(t *testing.T) {
	r, cfg, task := managedFixture(t)
	result := executeManagedTask(context.Background(), cfg, []DiscoveredCluster{r}, task)
	if result.Status != "SUCCESS" {
		t.Fatal(result.Message)
	}
	data, _ := os.ReadFile(r.PropsFile)
	if strings.Contains(string(data), "log.retention.hours") || !strings.Contains(string(data), "num.io.threads=12") {
		t.Fatal(string(data))
	}
	files, _ := filepath.Glob(filepath.Join(filepath.Dir(r.PropsFile), "config-backup", "broker.properties", "*.bak.*"))
	if len(files) != 1 {
		t.Fatalf("backups: %v", files)
	}
	backup, _ := os.ReadFile(files[0])
	if !strings.Contains(string(backup), "log.retention.hours=48") {
		t.Fatal("backup was taken after deletion")
	}
	info, _ := os.Stat(r.PropsFile)
	if info.Mode().Perm() != 0640 {
		t.Fatal("file permissions changed")
	}
}
func TestManagedRejectsStaleValuesAndUnknownIdentity(t *testing.T) {
	r, cfg, task := managedFixture(t)
	task.Parameters["expected_values_json"] = `{"log.retention.hours":"100","num.io.threads":"8"}`
	if result := executeManagedTask(context.Background(), cfg, []DiscoveredCluster{r}, task); result.Status != "FAILED" {
		t.Fatal("stale preview accepted")
	}
	before, _ := os.ReadFile(r.PropsFile)
	task.Parameters["kafka_cluster_id"] = "other-cluster"
	if result := executeManagedTask(context.Background(), cfg, []DiscoveredCluster{r}, task); result.Status != "FAILED" {
		t.Fatal("wrong cluster accepted")
	}
	after, _ := os.ReadFile(r.PropsFile)
	if string(before) != string(after) {
		t.Fatal("rejected task changed file")
	}
}
func TestManagedBackupFailurePreventsMutation(t *testing.T) {
	r, cfg, task := managedFixture(t)
	before, _ := os.ReadFile(r.PropsFile)
	managedBackupRegister = func(context.Context, string) error { return errors.New("retention registration failed") }
	result := executeManagedTask(context.Background(), cfg, []DiscoveredCluster{r}, task)
	if result.Status != "FAILED" {
		t.Fatal("backup failure ignored")
	}
	after, _ := os.ReadFile(r.PropsFile)
	if string(before) != string(after) {
		t.Fatal("file changed without registered backup")
	}
}
func TestManagedDynamicSnapshotAndCapabilityGuards(t *testing.T) {
	r, cfg, task := managedFixture(t)
	task.Command = "BACKUP_KAFKA_CONFIG"
	task.Parameters["config_key"] = "num.io.threads"
	task.Parameters["dynamic_snapshot_json"] = `{"mode":"DYNAMIC","key":"num.io.threads","previous":{"value":"8","source":"DYNAMIC_BROKER_CONFIG"}}`
	result := executeManagedTask(context.Background(), cfg, []DiscoveredCluster{r}, task)
	if result.Status != "SUCCESS" {
		t.Fatal(result.Message)
	}
	files, _ := filepath.Glob(filepath.Join(filepath.Dir(r.PropsFile), "config-backup", "broker.properties", "*.dynamic.json"))
	if len(files) != 1 {
		t.Fatal("missing dynamic checkpoint")
	}
	data, _ := os.ReadFile(files[0])
	var snapshot map[string]interface{}
	if json.Unmarshal(data, &snapshot) != nil || snapshot["key"] != "num.io.threads" {
		t.Fatal("invalid checkpoint")
	}
	cfg.AllowConfigWrite = false
	if result := executeManagedTask(context.Background(), cfg, []DiscoveredCluster{r}, task); result.Status != "FAILED" {
		t.Fatal("disabled capability ignored")
	}
	task.Command = "RESTART_SERVICE"
	task.Parameters["service_name"] = "unrelated.service"
	if result := executeManagedTask(context.Background(), cfg, []DiscoveredCluster{r}, task); result.Status != "FAILED" {
		t.Fatal("arbitrary service accepted")
	}
}
