package kafka

import (
	"context"
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
	exec := &recordingExecutor{}
	deployer := &Deployer{exec: exec}
	task := &api.Task{Parameters: map[string]string{"config_version": "7"}}

	backupPath, err := deployer.backupConfig(context.Background(), task, "/opt/kafka/config/kraft/broker.properties")
	if err != nil {
		t.Fatalf("backupConfig returned error: %v", err)
	}
	normalizedBackupPath := filepath.ToSlash(backupPath)
	if !strings.Contains(normalizedBackupPath, "/.tantor-backups/broker.properties/v7-") || !strings.HasSuffix(normalizedBackupPath, ".bak") {
		t.Fatalf("unexpected backup path %q", backupPath)
	}
	if len(exec.commands) != 3 {
		t.Fatalf("expected test, mkdir and copy commands; got %v", exec.commands)
	}
	if !strings.HasPrefix(exec.commands[2], "cp -p /opt/kafka/config/kraft/broker.properties ") {
		t.Fatalf("config backup copy was not recorded: %v", exec.commands)
	}
}
