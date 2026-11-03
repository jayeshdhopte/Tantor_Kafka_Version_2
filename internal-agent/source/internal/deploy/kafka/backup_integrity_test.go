package kafka

import (
	"context"
	"io.translab/tantor-agent/pkg/api"
	"os"
	"path/filepath"
	"testing"
)

type corruptBackupExecutor struct{ recordingExecutor }

func (e *corruptBackupExecutor) RunSudo(ctx context.Context, command string, args ...string) (string, string, error) {
	if command == "cp" {
		return "", "", os.WriteFile(args[len(args)-1], []byte("corrupted copy"), 0600)
	}
	return e.recordingExecutor.RunSudo(ctx, command, args...)
}
func TestCorruptCopyStopsVerifiedBackupBeforeAnyConfigurationMutation(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root agent contract")
	}
	path := filepath.Join(t.TempDir(), "broker.properties")
	original := []byte("node.id=1\nnum.io.threads=8\n")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(filepath.Dir(path), "config-backup", filepath.Base(path)), 0700); err != nil {
		t.Fatal(err)
	}
	deployer := &Deployer{exec: &corruptBackupExecutor{}}
	_, receipt, err := deployer.backupConfigVerified(context.Background(), &api.Task{Parameters: map[string]string{}}, path)
	if err == nil || receipt != "" {
		t.Fatal("corrupt backup accepted")
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(original) {
		t.Fatal("active configuration changed")
	}
}
