package kafka

import (
	"bytes"
	"context"
	"errors"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"io.translab/tantor-agent/internal/configbackup"
	"io.translab/tantor-agent/pkg/api"
)

type checkpointExecutor struct {
	registry string
	failCopy bool
}

func (e *checkpointExecutor) Run(ctx context.Context, command string, args ...string) (string, string, error) {
	cmd := osexec.CommandContext(ctx, command, args...)
	var out, detail bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &detail
	err := cmd.Run()
	return out.String(), detail.String(), err
}
func (e *checkpointExecutor) RunSudo(ctx context.Context, command string, args ...string) (string, string, error) {
	if e.failCopy && command == "cp" {
		return "", "disk full", errors.New("backup failed")
	}
	args = append([]string(nil), args...)
	for i, a := range args {
		if a == configbackup.Registry {
			args[i] = e.registry
		}
	}
	return e.Run(ctx, command, args...)
}

func checkpointFixture(t *testing.T) (*Deployer, *api.Task, string, *checkpointExecutor) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("production backup requires root")
	}
	root, err := os.MkdirTemp("/var/tmp", "tantor-checkpoint-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	configPath := filepath.Join(root, "kafka", "config", "broker.properties")
	os.MkdirAll(filepath.Dir(configPath), 0755)
	os.WriteFile(configPath, []byte("# retain\nlog.retention.hours=168\n"), 0640)
	// Preserved file modification times must not cause immediate backup expiry.
	os.Chtimes(configPath, time.Now().Add(-90*24*time.Hour), time.Now().Add(-90*24*time.Hour))
	task := &api.Task{Parameters: map[string]string{"kafka_install_base_dir": root, "config_path": configPath, "role": "broker", "config_key": "log.retention.hours", "config_version": "test", "config_changes_json": `{"log.retention.hours":"72"}`, "expected_values_json": `{"log.retention.hours":"168"}`, "dynamic_snapshot_json": `{"mode":"DYNAMIC","key":"log.retention.hours","previous":{"value":"168","source":"DYNAMIC_BROKER_CONFIG"},"requestedValue":"72"}`}}
	e := &checkpointExecutor{registry: filepath.Join(root, "index")}
	return NewDeployer(nil, nil, e), task, configPath, e
}

func TestDynamicAndStaticCheckpointsContainPreviousStateAndExpire(t *testing.T) {
	d, task, file, e := checkpointFixture(t)
	dynamicLog, err := d.BackupDynamicConfig(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	content, _ := os.ReadFile(file)
	if !strings.Contains(string(content), "=168") {
		t.Fatal("dynamic backup changed disk config")
	}
	if !strings.Contains(dynamicLog, ".dynamic.json") {
		t.Fatal(dynamicLog)
	}
	if _, err := d.PatchConfig(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(filepath.Dir(file), ".tantor-backups", "broker.properties")
	files, _ := os.ReadDir(dir)
	if len(files) != 3 {
		t.Fatalf("expected two disk checkpoints and one live snapshot: %v", files)
	}
	for _, f := range files {
		data, _ := os.ReadFile(filepath.Join(dir, f.Name()))
		if strings.HasSuffix(f.Name(), ".bak") && !strings.Contains(string(data), "=168") {
			t.Fatal("previous file was not preserved")
		}
		if strings.HasSuffix(f.Name(), ".dynamic.json") && !strings.Contains(string(data), "DYNAMIC_BROKER_CONFIG") {
			t.Fatal("dynamic override source missing")
		}
	}
	if err := configbackup.Sweep(e.registry, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	files, _ = os.ReadDir(dir)
	if len(files) != 3 {
		t.Fatal("old original mtime prematurely expired backups")
	}
	if err := configbackup.Sweep(e.registry, time.Now().UTC().Add(8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	files, _ = os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("seven-day retention did not expire backups")
	}
	content, _ = os.ReadFile(file)
	if !strings.Contains(string(content), "=72") {
		t.Fatal("retention removed/changed active config")
	}
}

func TestBackupFailureLeavesStaticConfigUntouched(t *testing.T) {
	d, task, file, e := checkpointFixture(t)
	e.failCopy = true
	if _, err := d.PatchConfig(context.Background(), task); err == nil {
		t.Fatal("applied change without backup")
	}
	data, _ := os.ReadFile(file)
	if !strings.Contains(string(data), "=168") {
		t.Fatal("modified config despite backup failure")
	}
}

func TestRetentionTracksBothVersionsAfterKafkaSymlinkUpgrade(t *testing.T) {
	d, task, file, e := checkpointFixture(t)
	active := filepath.Dir(filepath.Dir(file))
	old := active + "_old"
	if err := os.Rename(active, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(old, active); err != nil {
		t.Fatal(err)
	}
	if _, err := d.BackupDynamicConfig(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	newRoot := active + "_new"
	if err := os.MkdirAll(filepath.Join(newRoot, "config"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(newRoot, "config", "broker.properties"), []byte("log.retention.hours=168\n"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(active); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(newRoot, active); err != nil {
		t.Fatal(err)
	}
	if _, err := d.BackupDynamicConfig(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(e.registry)
	if err != nil || len(entries) != 2 {
		t.Fatalf("expected both canonical backup directories: %v %v", entries, err)
	}
	if err := configbackup.Sweep(e.registry, time.Now().UTC().Add(8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{old, newRoot} {
		files, err := os.ReadDir(filepath.Join(root, "config", ".tantor-backups", "broker.properties"))
		if err != nil || len(files) != 0 {
			t.Fatalf("old checkpoints remain: %v %v", files, err)
		}
		if _, err := os.Stat(filepath.Join(root, "config", "broker.properties")); err != nil {
			t.Fatal(err)
		}
	}
}
