package kafka

import (
	"context"
	"io.translab/tantor-agent/internal/configbackup"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"io.translab/tantor-agent/pkg/api"
)

type recordingExecutor struct {
	commands []string
}

func (e *recordingExecutor) RunSudo(_ context.Context, command string, args ...string) (string, string, error) {
	e.commands = append(e.commands, command+" "+strings.Join(args, " "))
	return "", "", nil
}

func (e *recordingExecutor) Run(_ context.Context, command string, args ...string) (string, string, error) {
	e.commands = append(e.commands, command+" "+strings.Join(args, " "))
	return "", "", nil
}

func TestBackupConfigUsesConfigFilenameAndTimestamp(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("production backup requires a root agent")
	}
	exec := &recordingExecutor{}
	deployer := &Deployer{exec: exec}
	configPath := filepath.Join(t.TempDir(), "config", "broker.properties")
	os.MkdirAll(filepath.Join(filepath.Dir(configPath), "config-backup", "broker.properties"), 0700)
	task := &api.Task{Parameters: map[string]string{"config_version": "7"}}

	backupPath, err := deployer.backupConfig(context.Background(), task, configPath)
	if err != nil {
		t.Fatalf("backupConfig returned error: %v", err)
	}
	normalizedBackupPath := filepath.ToSlash(backupPath)
	if !strings.Contains(normalizedBackupPath, "/config-backup/broker.properties/broker.properties.bak.") {
		t.Fatalf("unexpected backup path %q", backupPath)
	}
	stamp := strings.TrimPrefix(filepath.Base(backupPath), "broker.properties.bak.")
	if _, err := time.Parse(configbackup.Timestamp, stamp); err != nil {
		t.Fatalf("invalid backup timestamp: %v", err)
	}
	if strings.Contains(filepath.Base(backupPath), "v7-") {
		t.Fatal("job version leaked into backup filename")
	}
	if !strings.Contains(strings.Join(exec.commands, "\n"), "config-backup-index") {
		t.Fatalf("seven-day retention was not registered: %v", exec.commands)
	}
	if !strings.Contains(strings.Join(exec.commands, "\n"), "cp -p "+configPath+" ") {
		t.Fatalf("config backup copy was not recorded: %v", exec.commands)
	}
}
