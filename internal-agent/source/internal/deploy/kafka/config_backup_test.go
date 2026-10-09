package kafka

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestBackupConfigUsesVersionedImmutablePath(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("production backup requires a root agent")
	}
	exec := &recordingExecutor{}
	deployer := &Deployer{exec: exec}
	task := &api.Task{Parameters: map[string]string{"config_version": "7"}}

	backupPath, err := deployer.backupConfig(context.Background(), task, "/opt/kafka/config/kraft/broker.properties")
	if err != nil {
		t.Fatalf("backupConfig returned error: %v", err)
	}
	normalizedBackupPath := filepath.ToSlash(backupPath)
	if !strings.Contains(normalizedBackupPath, "/config-backup/broker.properties/v7-") || !strings.HasSuffix(normalizedBackupPath, ".bak") {
		t.Fatalf("unexpected backup path %q", backupPath)
	}
	if !strings.Contains(strings.Join(exec.commands, "\n"), "config-backup-index") {
		t.Fatalf("seven-day retention was not registered: %v", exec.commands)
	}
	if !strings.Contains(strings.Join(exec.commands, "\n"), "cp -p /opt/kafka/config/kraft/broker.properties ") {
		t.Fatalf("config backup copy was not recorded: %v", exec.commands)
	}
}
